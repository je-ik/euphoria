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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * A {@code KafkaExecutor} that actually doesn't use kafka, but uses
 * {@code BlockingQueue} for all data pipelines. This makes this
 * executor actually only sort of equivalent to {@code InMemExecutor}.
 */
class TestKafkaExecutor extends KafkaExecutor {

  Map<String, ObservableStream<KafkaStreamElement>> topicQueues = new HashMap<>();

  TestKafkaExecutor(ExecutorService executor) {
    super(executor, new String[] { });
  }

  @Override
  ObservableStream<KafkaStreamElement> kafkaObservableStream(
      String topic,
      Function<byte[], Object> deserializer) {

    return Objects.requireNonNull(
        topicQueues.get(topic),
        "Cannot find stream  " + topic);
  }

  @Override
  OutputWriter outputWriter(String topic) {
    BlockingQueue<KafkaStreamElement> queue = new ArrayBlockingQueue<>(100);
    topicQueues.put(
        topic,
        BlockingQueueObservableStream.wrap(topic, queue, 0));
    return (elem, source, target, callback) -> {
      try {
        queue.put(elem);
        callback.apply(true, null);
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        callback.apply(false, ex);
      } catch (Exception ex) {
        callback.apply(false, ex);
      }
    };
  }

}
