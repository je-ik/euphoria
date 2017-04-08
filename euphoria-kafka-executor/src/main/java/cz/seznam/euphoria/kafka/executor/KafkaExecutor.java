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

import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioner;
import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.ExtractEventTime;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import cz.seznam.euphoria.inmem.ExecUnit;
import cz.seznam.euphoria.inmem.InMemStorageProvider;
import cz.seznam.euphoria.inmem.NoopTriggerScheduler;
import cz.seznam.euphoria.inmem.ProcessingTimeTriggerScheduler;
import cz.seznam.euphoria.inmem.runnable.ReduceStateByKeyReducer;
import cz.seznam.euphoria.inmem.TriggerScheduler;
import cz.seznam.euphoria.inmem.WatermarkEmitStrategy;
import cz.seznam.euphoria.inmem.WatermarkTriggerScheduler;
import cz.seznam.euphoria.inmem.stream.BlockingQueueOutputWriter;
import cz.seznam.euphoria.inmem.stream.BlockingQueueStreamObservable;
import cz.seznam.euphoria.inmem.stream.OutputWriter;
import cz.seznam.euphoria.inmem.stream.StreamObserver;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cz.seznam.euphoria.inmem.stream.ObservableStream;

/**
 * An {@code Executor} that uses Apache Kafka as a message passing system.
 */
public class KafkaExecutor implements cz.seznam.euphoria.core.executor.Executor {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaExecutor.class);

  private void createStream(DataSource<?> source, ExecutionContext context) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  public static class Builder0 {
    public Builder1 executor(Executor executor) {
      return new Builder1(executor);
    }
    public KafkaExecutor build() {
      return new KafkaExecutor(
          Executors.newCachedThreadPool(),
          ProcessingTimeTriggerScheduler::new,
          WatermarkEmitStrategy.Default::new,
          new InMemStorageProvider());
    }
  }

  public static class Builder1 {
    final Executor executor;
    Builder1(Executor executor) {
      this.executor = executor;
    }
    public KafkaExecutor build() {
      return new KafkaExecutor(
          executor,
          ProcessingTimeTriggerScheduler::new,
          WatermarkEmitStrategy.Default::new,
          new InMemStorageProvider());
    }
    public Builder2 withSchedulerSupplier(Supplier<TriggerScheduler> schedulerSupplier) {
      return new Builder2(executor, schedulerSupplier);
    }
  }

  public static class Builder2 {
    final Executor executor;
    final Supplier<TriggerScheduler> schedulerSupplier;
    Builder2(Executor executor, Supplier<TriggerScheduler> schedulerSupplier) {
      this.executor = executor;
      this.schedulerSupplier = schedulerSupplier;
    }
    public KafkaExecutor build() {
      return new KafkaExecutor(
          executor,
          schedulerSupplier,
          WatermarkEmitStrategy.Default::new,
          new InMemStorageProvider());
    }
    public Builder3 withEmitStrategy(Supplier<WatermarkEmitStrategy> watermarkEmit) {
      return new Builder3(executor, schedulerSupplier, watermarkEmit);
    }
  }

  public static class Builder3 {
    final Executor executor;
    final Supplier<TriggerScheduler> schedulerSupplier;
    final Supplier<WatermarkEmitStrategy> watermarkEmit;
    Builder3(
        Executor executor,
        Supplier<TriggerScheduler> schedulerSupplier,
        Supplier<WatermarkEmitStrategy> watermarkEmit) {
      this.executor = executor;
      this.schedulerSupplier = schedulerSupplier;
      this.watermarkEmit = watermarkEmit;
    }
    public KafkaExecutor build() {
      return new KafkaExecutor(
          executor,
          schedulerSupplier,
          watermarkEmit,
          new InMemStorageProvider());
    }
    public Builder4 withStorageProvider(StorageProvider provider) {
      return new Builder4(
          executor,
          schedulerSupplier,
          watermarkEmit,
          provider);
    }
  }

  public static class Builder4 {
    final Executor executor;
    final Supplier<TriggerScheduler> schedulerSupplier;
    final Supplier<WatermarkEmitStrategy> watermarkEmit;
    final StorageProvider storageProvider;
    Builder4(
        Executor executor,
        Supplier<TriggerScheduler> schedulerSupplier,
        Supplier<WatermarkEmitStrategy> watermarkEmit,
        StorageProvider storageProvider) {
      this.executor = executor;
      this.schedulerSupplier = schedulerSupplier;
      this.watermarkEmit = watermarkEmit;
      this.storageProvider = storageProvider;
    }
    public KafkaExecutor build() {
      return new KafkaExecutor(
          executor,
          schedulerSupplier,
          watermarkEmit,
          storageProvider);
    }
  }

  public static Builder0 builder() {
    return new Builder0();
  }

  private final Executor executor;
  private final Supplier<WatermarkEmitStrategy> watermarkSupplier;
  private final Supplier<TriggerScheduler> triggerSchedulerSupplier;
  private final StorageProvider storageProvider;

  KafkaExecutor(
      Executor executor,
      Supplier<TriggerScheduler> triggerSchedulerSupplier,
      Supplier<WatermarkEmitStrategy> watermarkSupplier,
      StorageProvider storageProvider) {
    
    this.executor = executor;
    this.watermarkSupplier = watermarkSupplier;
    this.triggerSchedulerSupplier = triggerSchedulerSupplier;
    this.storageProvider = storageProvider;
  }

  @Override
  public CompletableFuture<Result> submit(Flow flow) {
    return CompletableFuture.supplyAsync(() -> run(flow), executor);
  }

  @Override
  public void shutdown() {

  }

  /**
   * Run the flow synchronously.
   * @param flow the flow to run
   */
  private Result run(Flow flow) {
    // transform the given flow to DAG of basic operators
    DAG<Operator<?, ?>> dag = FlowUnfolder.unfold(flow,
        cz.seznam.euphoria.core.executor.Executor.getBasicOps());

    final List<Future> runningTasks = new ArrayList<>();
    Collection<Node<Operator<?, ?>>> leafs = dag.getLeafs();

    List<ExecUnit> units = ExecUnit.split(dag);

    if (units.isEmpty()) {
      throw new IllegalArgumentException("Cannot execute empty flow");
    }

    for (ExecUnit unit : units) {
      ExecutionContext context = new ExecutionContext();
      execUnit(unit, context);

      runningTasks.addAll(consumeOutputs(unit.getLeafs(), context));
    }

    // extract all processed sinks
    List<DataSink<?>> sinks = leafs.stream()
            .map(n -> n.get().output().getOutputSink())
            .filter(s -> s != null)
            .collect(Collectors.toList());

    // wait for all threads to finish
    for (Future<?> f : runningTasks) {
      try {
        f.get();
      } catch (InterruptedException e) {
        break;
      } catch (ExecutionException e) {
        // when any one of the tasks fails rollback all sinks and fail
        for (DataSink<?> s : sinks) {
          try {
            s.rollback();
          } catch (Exception ex) {
            LOG.error("Exception during DataSink rollback", ex);
          }
        }
        throw new RuntimeException(e);
      }
    }

    // commit all sinks
    try {
      for (DataSink<?> s : sinks) {
        s.commit();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    
    return new Result();
  }


  private void execUnit(ExecUnit unit, ExecutionContext context) {
    unit.getDAG().traverse().forEach(n -> execNode(n, context));
  }

  /**
   * Execute single operator and return the suppliers for partitions
   * of output.
   */
  @SuppressWarnings("unchecked")
  private void execNode(
      Node<Operator<?, ?>> node, ExecutionContext context) {
    Operator<?, ?> op = node.get();    
    if (op instanceof FlowUnfolder.InputOperator) {
      createStream(op.output().getSource(), context);
    } else if (op instanceof FlatMap) {
      execMap((Node) node, context);
    } else if (op instanceof Repartition) {
      execRepartition((Node) node, context);
    } else if (op instanceof ReduceStateByKey) {
      execReduceStateByKey((Node) node, context);
    } else if (op instanceof Union) {
      execUnion((Node) node, context);
    } else {
      throw new IllegalStateException("Invalid operator: " + op);
    }
  }

  @SuppressWarnings("unchecked")
  private void execMap(
      Node<FlatMap> flatMap,
      ExecutionContext context) {

    final FlatMap op = flatMap.get();
    final ObservableStream<Datum<?>> observable = Iterables.getOnlyElement(
        context.getInputs(flatMap.getSingleParentOrNull().get()));
    final UnaryFunctor mapper = flatMap.get().getFunctor();
    // forward elements via blocking queue
    final BlockingQueue<Datum<?>> outputQueue = new ArrayBlockingQueue<>(100);
    final OutputWriter<Datum<?>> output = BlockingQueueOutputWriter.wrap(outputQueue);

    context.runIfNotRunning(op, () -> {
      final ObservableStream<Datum<?>> outputObservable;
      outputObservable = BlockingQueueStreamObservable.wrap(executor, outputQueue);
      WindowedContext udfContext = new WindowedContext(output);
      observable.observe(new StreamObserver<Datum<?>>() {
        @Override
        public void onNext(Datum<?> elem) {
          udfContext.set(elem);
          mapper.apply(elem.getElement(), udfContext);
        }

        @Override
        public void onError(Throwable err) {
        }

        @Override
        public void onCompleted() {
        }
      });
      return outputObservable;
    });
  }

  @SuppressWarnings("unchecked")
  private void execRepartition(
      Node<Repartition> repartition,
      ExecutionContext context) {

    Repartition op = repartition.get();
    Partitioning partitioning = repartition.get().getPartitioning();
    final Partitioner partitioner = partitioning.getPartitioner();
    int numPartitions = partitioning.getNumPartitions();
    ObservableStream<Datum<?>> input = Iterables.getOnlyElement(context.getInputs(op));
    if (numPartitions <= 0) {
      throw new IllegalArgumentException(
          "Cannot repartition input to "
          + numPartitions + " partitions");    
    }
    final String topic = "repartition_" + op.getFlow().getName() + "_" + op.getName();

    final Function<Object, Integer> partFunc = o -> {
      return (partitioner.getPartition(o) & Integer.MAX_VALUE) % numPartitions;
    };

    Pair<ObservableStream, Runnable> repartitioned;
    repartitioned = repartitionStream(op.getName(), input, topic, partFunc);

    context.runIfNotRunning(
        op,
        () -> {
          repartitioned.getSecond().run();
          return repartitioned.getFirst();
        });
  }

  @SuppressWarnings("unchecked")
  private Pair<ObservableStream, Runnable> repartitionStream(
      final String name,
      final ObservableStream<Datum<?>> input,
      final String topic,
      final Function<Object, Integer> partFunc) {

    KafkaTopicOutputWriter writer;
    KafkaTopicStreamObservable observable = new KafkaTopicStreamObservable(
        executor, topic, bytes -> deserializeKryo(bytes));

    writer = new KafkaTopicOutputWriter(topic, partFunc, this::serializeKryo);
    return Pair.of(observable, () -> input.observe(new StreamObserver<Datum<?>>() {
      @Override
      public void onNext(Datum<?> elem) {
        try {
          writer.write(elem);
        } catch (InterruptedException ex) {
          LOG.warn("Interrupted while writing element to output partition", ex);
          Thread.currentThread().interrupt();
        }
      }


      @Override
      public void onError(Throwable err) {
        LOG.error(
            "Error observing input partitions of operator {}",
            name, err);
      }

      @Override
      public void onCompleted() {

      }
    }));
  }

  @SuppressWarnings("unchecked")
  private void execReduceStateByKey(
      Node<ReduceStateByKey> reduceStateByKeyNode,
      ExecutionContext context) {

    final ReduceStateByKey reduceStateByKey = reduceStateByKeyNode.get();
    final UnaryFunction keyExtractor = reduceStateByKey.getKeyExtractor();
    final UnaryFunction valueExtractor = reduceStateByKey.getValueExtractor();
    final Partitioning partitioning = reduceStateByKey.getPartitioning();
    final Partitioner partitioner = partitioning.getPartitioner();
    final int numPartitions = partitioning.getNumPartitions();
    final Windowing windowing = reduceStateByKey.getWindowing();
    final ExtractEventTime eventTimeAssigner = reduceStateByKey.getEventTimeAssigner();
    final TriggerScheduler triggerScheduler = triggerSchedulerSupplier.get();
    final long watermarkDuration
        = triggerScheduler instanceof WatermarkTriggerScheduler
            ? ((WatermarkTriggerScheduler) triggerScheduler).getWatermarkDuration()
            : 0L;
    final ObservableStream<Datum<?>> input = Iterables.getOnlyElement(
        context.getInputs(reduceStateByKey));
    final String topic = "rsbk_" + reduceStateByKey.getFlow().getName()
        + "_" + reduceStateByKey.getName();
    final Function<Object, Integer> partFunc = o -> {
      return (partitioner.getPartition(o) & Integer.MAX_VALUE) % numPartitions;
    };

    // repartition
    Pair<ObservableStream, Runnable> repartitioned;
    repartitioned = repartitionStream(
        reduceStateByKey.getName() + "-repartition",
        input, topic, partFunc);
    BlockingQueue<Datum<?>> queue = new ArrayBlockingQueue<>(100);

    context.runIfNotRunning(reduceStateByKey, () -> {
      BlockingQueueStreamObservable<Datum<?>> observable;
      observable = BlockingQueueStreamObservable.wrap(executor, queue);
      repartitioned.getSecond().run();
      // consume repartitioned suppliers
      new ReduceStateByKeyReducer(
          reduceStateByKey,
          reduceStateByKey.getName() + "-reduce",
          repartitioned.getFirst(),
          BlockingQueueOutputWriter.wrap(queue),
          keyExtractor, valueExtractor,
          // ~ on batch input we use a noop trigger scheduler
          // ~ if using attached windowing, we have to use watermark triggering
          reduceStateByKey.input().isBounded()
            ? new NoopTriggerScheduler()
            : (windowing != null
                  ? triggerSchedulerSupplier.get()
                  : new WatermarkTriggerScheduler(watermarkDuration)),
          watermarkSupplier.get(),
          storageProvider).run();
      return observable;
    });
    
  }


  @SuppressWarnings("unchecked")
  private void execUnion(Node<Union> union, ExecutionContext context) {

    final Union op = union.get();
    final BlockingQueue<Datum<?>> output = new ArrayBlockingQueue<>(100);
    final List<ObservableStream<Datum<?>>> inputs = context.getInputs(op);

    context.runIfNotRunning(op, () -> {
      BlockingQueueStreamObservable<Datum<?>> observable;
      observable = BlockingQueueStreamObservable.wrap(executor, output);
      inputs.forEach(o -> o.observe(new StreamObserver<Datum<?>>() {

        @Override
        public void onNext(Datum<?> elem) {
          try {
            output.put(elem);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            LOG.warn(
                "Interrupted while writing to output of operator {}",
                op.getName());
          }
        }

        @Override
        public void onError(Throwable err) {
          LOG.error("Failed to observe input of {}", op.getName(), err);
        }

        @Override
        public void onCompleted() {

        }

      }));
      return observable;
    });

  }


  private Collection<? extends Future> consumeOutputs(
      Collection<Node<Operator<?, ?>>> leafs, ExecutionContext context) {
    throw new UnsupportedOperationException("Not supported yet.");
  }


  /**
   * Serialize given object using kryo instance.
   */
  private byte[] serializeKryo(Object o) {
    // FIXME
    return null;
  }

  /**
   * Deserialize object using Kryo.
   */
  @SuppressWarnings("unchecked")
  private Object deserializeKryo(byte[] bytes) {
    // FIXME
    return null;
  }


}
