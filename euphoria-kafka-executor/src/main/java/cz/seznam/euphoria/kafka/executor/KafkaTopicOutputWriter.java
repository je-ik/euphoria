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

import cz.seznam.euphoria.inmem.stream.OutputWriter;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

/**
 * Writer that writes data into Kafka topic.
 */
class KafkaTopicOutputWriter<T> implements OutputWriter<Datum<T>> {

  private final String topic;
  private final KafkaProducer<String, byte[]> producer;
  private final Function<T, Integer> partitioner;
  private final Function<T, byte[]> serializer;

  KafkaTopicOutputWriter(
      String topic,
      Function<T, Integer> partitioner,
      Function<T, byte[]> serializer) {

    this.topic = topic;
    this.partitioner = partitioner;
    this.serializer = serializer;
    producer = createProducer();
  }

  @Override
  public void write(Datum<T> elem, Callback callback) {
    ProducerRecord<String, byte[]> rec = new ProducerRecord<>(
        topic,
        partitioner.apply(elem.getElement()),
        elem.getTimestamp(),
        null,
        new KafkaRecord(
            elem.getWindow(), serializer.apply(elem.getElement()))
            .toBytes());
    producer.send(rec, (meta, exc) -> {
      callback.confirm(exc == null, exc);
    });
  }

  protected KafkaProducer<String, byte[]> createProducer() {

    Properties props = new Properties();
    return new KafkaProducer<>(
        props,
        Serdes.String().serializer(),
        Serdes.ByteArray().serializer());
  }

}
