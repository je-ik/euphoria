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

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.operator.Operator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import cz.seznam.euphoria.inmem.stream.ObservableStream;

/**
 * A context keeping mapping from {@code Datasets} to {@code OutputWriter}s
 * and {@code StreamObserverable}s.
 */
class ExecutionContext {

  final Set<Operator<?, ?>> running = new HashSet<>();
  final Map<Dataset<?>, ObservableStream<Datum<?>>> datasetToObservable = new HashMap<>();

  public void runIfNotRunning(
      Operator<?, ?> op, Supplier<ObservableStream<Datum<?>>> cmd) {
    
    if (running.add(op)) {
      datasetToObservable.put(op.output(), cmd.get());
    }
  }

  /**
   * Retrieve observable stream that is on input of the given operator.
   */
  List<ObservableStream<Datum<?>>> getInputs(Operator<?, ?> op) {
    return op.listInputs().stream()
        .map(input -> {
          ObservableStream<Datum<?>> ret;
          ret = datasetToObservable.get(input);
          if (ret == null) {
            throw new IllegalStateException(
                "Cannot find observable for input "
                + input);
          }
          return ret;
        })
        .collect(Collectors.toList());
  }

}
