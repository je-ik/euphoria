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

import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.kafka.executor.io.TopicSpec;
import java.util.Objects;
import java.util.concurrent.Executor;
import scala.concurrent.ExecutionContext;

/**
 * Implementation of RSBK on input {@code KafkaConsumer}.
 * The output is written to the provided {@code OutputWriter}.
 * The input topic has to be already partitioned according to the key.
 * Whole value is shuffled inside the topic for now.
 */
public class OpReduceStateByKey {

  private final Node<ReduceStateByKey> node;
  private final ExecutionContext context;
  private final KafkaConsumerFactory consumerFactory;
  private final KafkaProducerFactory producerFactory;
  private final TopicSpec inputSpec;
  private final TopicSpec stateSpec;

  private KafkaStorageProvider stateProvider;

  public OpReduceStateByKey(
      Node<ReduceStateByKey> node,
      ExecutionContext context,
      KafkaConsumerFactory consumerFactory,
      KafkaProducerFactory producerFactory,
      TopicSpec inputSpec,
      TopicSpec stateSpec) {

    this.node = Objects.requireNonNull(node);
    this.context = Objects.requireNonNull(context);
    this.consumerFactory = Objects.requireNonNull(consumerFactory);
    this.producerFactory = Objects.requireNonNull(producerFactory);
    this.inputSpec = Objects.requireNonNull(inputSpec);
    this.stateSpec = Objects.requireNonNull(stateSpec);
  }

  /** Run the reducer via given executor. */
  public void run(Executor executor) {

  }

}
