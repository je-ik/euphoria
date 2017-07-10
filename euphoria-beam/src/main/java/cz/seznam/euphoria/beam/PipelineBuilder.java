/**
 * Copyright 2017 Seznam.cz, a.s.
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

package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.beam.io.BeamBoundedSource;
import cz.seznam.euphoria.beam.io.BeamWriteSink;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Sets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.tuple.Pair;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * This class converts Euphoria's {@code Flow} into Beam's Pipeline.
 */
class PipelineBuilder {

  @SuppressWarnings("unchecked")
  private static class Context {
    final Map<Dataset, PCollectionList> mapping = new HashMap<>();
    void addAll(Map<Dataset<?>, PCollectionList<?>> mappings) {
      mapping.putAll(mappings);
    }
    Optional<PCollectionList<?>> get(Dataset<?> dataset) {
      return Optional.ofNullable((PCollectionList<?>)mapping.get(dataset));
    }
    void put(Dataset<?> dataset, PCollectionList<?> collection) {
      if (mapping.put(dataset, collection) != null) {
        throw new IllegalArgumentException(
            "PCollection for dataset " + dataset
            + " has already been registered!");
      }
    }
  }

  /**
   * Convert given flow to pipeline.
   * @param flow the {@code Flow} to convert
   */
  @SuppressWarnings("unchecked")
  static Pipeline toPipeline(Flow flow, PipelineOptions options) {
    // FIXME: names, and other options here
    Pipeline pipeline = Pipeline.create(options);
    Context context = new Context();
    Collection<Dataset<?>> sources = flow.sources();
    context.addAll(sources.stream()
        .map(ds -> {
          DataSource<?> source = ds.getSource();
          PCollectionList<?> collections = PCollectionList.empty(pipeline);
          for (int partitionId = 0; partitionId < source.getPartitions().size(); partitionId++) {
            collections = collections.and((PCollection) pipeline.apply(
                Read.from(BeamBoundedSource.wrap(source, partitionId))));
          }
          return Pair.of(ds, collections);
        })
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight)));
    DAG<Operator<?, ?>> dag = FlowUnfolder.unfold(flow, basicOps());
    dag.traverse().forEach(n -> add(context, n));
    dag.getLeafs().forEach(l -> {
      Dataset<?> output = l.get().output();
      PCollectionList outputCollection = context.get(output).orElseThrow(
          () -> new IllegalStateException("Dataset " + output + " has not been materialized"));
      DataSink<?> sink = output.getOutputSink();
      outputCollection.apply(BeamWriteSink.wrap(pipeline, sink));
    });
    return pipeline;
  }

  @SuppressWarnings("unchecked")
  static void add(Context context, Node<Operator<?, ?>> node) {
    Operator<?, ?> op = node.get();
    if (op instanceof FlatMap) {
      flatMap(context, (Node) node);
    }
  }

  @SuppressWarnings("unchecked")
  private static void flatMap(Context context, Node<Operator<?, ?>> node) {
    FlatMap op = (FlatMap) node.get();
    Node<Operator<?, ?>> parent = Iterables.getOnlyElement(node.getParents());
    Dataset<?> inputDataset = parent.get().output();
    PCollectionList<?> input = context.get(inputDataset).orElseThrow(
        () -> new IllegalStateException("Dataset " + inputDataset + " is not materialized yet"));
    UnaryFunctor functor = op.getFunctor();
    FlatMapper mapper = new FlatMapper(functor);
    PCollectionList<?> output = PCollectionList.empty(input.getPipeline());
    for (PCollection<?> partition : input.getAll()) {
      output = output.and((PCollection) partition.apply(
          FlatMapElements.<Object, Object>via(mapper)));
    }
    context.put(op.output(), output);
  }

  @SuppressWarnings("unchecked")
  private static Set<Class<? extends Operator<?, ?>>> basicOps() {
    return (Set) Sets.newHashSet(
        FlatMap.class, Union.class, Repartition.class, ReduceStateByKey.class);
  }
}
