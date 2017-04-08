/*
 * Copyright 2017 Seznam.cz, a.s..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cz.seznam.euphoria.kafka.executor;

import cz.seznam.euphoria.inmem.stream.StreamObserver;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cz.seznam.euphoria.inmem.stream.ObservableStream;

/**
 * An observer of stream data in Kafka topic.
 */
class KafkaTopicStreamObservable<T> implements ObservableStream<Datum<T>> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicStreamObservable.class);

  private final Executor executor;
  private final String topic;
  private final Function<byte[], T> deserializer;

  KafkaTopicStreamObservable(
      Executor executor,
      String topic,
      Function<byte[], T> deserializer) {

    this.executor = executor;
    this.topic = topic;
    this.deserializer = deserializer;
  }

  @Override
  public void observe(String name, StreamObserver<Datum<T>> observer) {
    KafkaConsumer<String, byte[]> consumer = createConsumer(topic);
    executor.execute(() -> {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          ConsumerRecords<String, byte[]> records = consumer.poll(100);
          for (ConsumerRecord<String, byte[]> r : records) {
            KafkaRecord kafkaRec = KafkaRecord.fromBytes(r.value());
            T val = deserializer.apply(kafkaRec.getValue());
            Datum d = new Datum(kafkaRec.getWindow(), r.timestamp(), val);
            observer.onNext(d);
          }
        }
        observer.onCompleted();
      } catch (Throwable exc) {
        LOG.error("Error caught observing topic {} by {}", new Object[] {
          topic, name, exc
        });
        observer.onError(exc);
      }
    });
  }

  private KafkaConsumer<String, byte[]> createConsumer(String topic) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

}
