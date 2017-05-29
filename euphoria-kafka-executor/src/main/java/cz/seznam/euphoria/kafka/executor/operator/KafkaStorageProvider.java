/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cz.seznam.euphoria.kafka.executor.operator;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.kafka.executor.io.TopicSpec;
import cz.seznam.euphoria.kafka.executor.proto.Serialization;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * {@code StorageProvider} using Kafka as its commit log.
 * It caches the state locally using given cache implementation.
 */
class KafkaStorageProvider<K, V> implements Closeable {

  private final LocalCache<Serialization.StateKey, byte[]> cache;
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final TopicSpec stateSpec;
  private final KafkaProducer<byte[], byte[]> producer;
  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;
  private final CountDownLatch termLatch = new CountDownLatch(1);

  private volatile boolean terminate = false;


  /**
   * Create the state provider.
   * @param cache local cache implementation to use to store the cache topic
   * @param consumer the kafka consumer - it has to be already assigned partitions to consume
   */
  KafkaStorageProvider(
      LocalCache<Serialization.StateKey, byte[]> cache,
      KafkaConsumer<byte[], byte[]> consumer,
      TopicSpec stateSpec,
      KafkaProducer<byte[], byte[]> producer,
      Serde<K> keySerde,
      Serde<V> valueSerde) {

    this.cache = cache;
    this.consumer = consumer;
    this.stateSpec = stateSpec;
    this.producer = producer;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
  }

  /**
   * Execute the background caching process.
   */
  public void run(Executor executor) {
    executor.execute(this::runInternal);
  }

  private void runInternal() {
    while (!Thread.currentThread().isInterrupted() && !terminate) {
      // FIXME: configuration
      consumer.poll(100).forEach(record -> {
        try {
          Serialization.StateKey key = toKey(record.key());
          cache.update(key, record.value());
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      });
    }
    termLatch.countDown();
  }

  V get(String name, K key) {
    Serialization.StateKey cacheKey = Serialization.StateKey.newBuilder()
        .setType(Serialization.StateKey.Type.SINGLE_VALUE)
        .setName(name)
        .setKey(ByteString.copyFrom(keySerde.toBytes(key)))
        .build();
    return valueSerde.fromBytes(cache.getLatest(cacheKey));
  }

  Iterable<V> list(String name, K key) {
    Serialization.StateKey cacheKey = Serialization.StateKey.newBuilder()
        .setType(Serialization.StateKey.Type.LIST_VALUE)
        .setName(name)
        .setKey(ByteString.copyFrom(keySerde.toBytes(key)))
        .build();
    return Iterables.transform(cache.getAll(cacheKey), valueSerde::fromBytes);
  }

  byte[] getTimer(String name, K key) {
    Serialization.StateKey cacheKey = Serialization.StateKey.newBuilder()
        .setTimerName(name)
        .setKey(ByteString.copyFrom(keySerde.toBytes(key)))
        .setType(Serialization.StateKey.Type.TIMER)
        .build();
    return cache.getLatest(cacheKey);
  }

  void write(String name, K key, Serialization.StateKey.Type type, byte[] value) {
    Serialization.StateKey cacheKey = toKey(type, key, name);
    cache.add(cacheKey, value);
    // FIXME: partitioning, confirmation, error handling
    producer.send(new ProducerRecord<>(stateSpec.getName(), value));
  }

  void reset(String name, K key, Serialization.StateKey.Type type) {
    Serialization.StateKey cacheKey = toKey(type, key, name);
    cache.reset(cacheKey);
  }

  private Serialization.StateKey toKey(
      Serialization.StateKey.Type type, K key, String name) {

    Serialization.StateKey.Builder cacheKey = Serialization.StateKey.newBuilder()
        .setType(type)
        .setKey(ByteString.copyFrom(keySerde.toBytes(key)));
    switch (type) {

      case LIST_VALUE:
      case SINGLE_VALUE:
        cacheKey.setName(name);
        break;

      case TIMER:
        cacheKey.setTimerName(name);
        break;

    }
    return cacheKey.build();
  }

  private Serialization.StateKey toKey(byte[] key)
      throws InvalidProtocolBufferException {

    return Serialization.StateKey.parseFrom(key);
  }


  public StorageProvider getKeyedStorageProvider(K key) {
    
    return new StorageProvider() {
      @Override
      public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
        return new ValueStorage<T>() {

          @Override
          public void set(T value) {
            write(
                descriptor.getName(), key,
                Serialization.StateKey.Type.SINGLE_VALUE,
                // FIXME: serialization, this might be wrong
                valueSerde.toBytes((V) value));
          }

          @Override
          public T get() {
            // FIXME: serialization of value
            return (T) KafkaStorageProvider.this.get(descriptor.getName(), key);
          }

          @Override
          public void clear() {
            reset(descriptor.getName(), key, Serialization.StateKey.Type.SINGLE_VALUE);
          }

        };
      }

      @Override
      public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
        return new ListStorage<T>() {

          @Override
          public void add(T element) {
            write(
                descriptor.getName(), key,
                Serialization.StateKey.Type.LIST_VALUE,
                valueSerde.toBytes((V) element));
          }

          @Override
          public Iterable<T> get() {
            // FIXME
            return (Iterable) list(descriptor.getName(), key);
          }

          @Override
          public void clear() {
            reset(descriptor.getName(), key, Serialization.StateKey.Type.LIST_VALUE);
          }

        };
      }

    };
  }

  @Override
  public void close() {
    terminate = true;
    try {
      termLatch.await();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }


}
