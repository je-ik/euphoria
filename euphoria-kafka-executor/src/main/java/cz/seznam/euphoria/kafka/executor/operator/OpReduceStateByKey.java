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

import cz.seznam.euphoria.core.client.dataset.partitioning.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StateMerger;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.inmem.TriggerScheduler;
import cz.seznam.euphoria.inmem.VectorClock;
import cz.seznam.euphoria.inmem.operator.KeyedWindow;
import cz.seznam.euphoria.inmem.operator.StreamElement;
import cz.seznam.euphoria.kafka.executor.ObservableStream;
import cz.seznam.euphoria.kafka.executor.OutputWriter;
import cz.seznam.euphoria.kafka.executor.StreamObserver;
import cz.seznam.euphoria.kafka.executor.io.SnapshotableStorageProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Implementation of RSBK on input {@code ObservableStream}.
 * The output is written to the provided {@code OutputWriter}.
 * The input topic has to be already partitioned according to the key.
 * Whole value is shuffled inside the topic for now.
 */
public class OpReduceStateByKey implements Context, TriggerContext {

  private final Node<ReduceStateByKey> node;
  private final ObservableStream<StreamElement> input;
  private final OutputWriter output;
  private final SnapshotableStorageProvider storageProvider;
  private final TriggerScheduler scheduler;

  private final ReduceStateByKey op;
  private final StateFactory stateFactory;
  private final UnaryFunction keyExtractor;
  private final UnaryFunction valueExtractor;
  private final Partitioning partitioning;
  private final StateMerger stateMerger;
  private final Windowing<?, Window> windowing;

  private final KafkaStorageProvider processingState;

  // the following is held in memory, is reset and reconstructed
  // upon each state reload
  private final Map<KeyedWindow<Window, Object>, State> openedStates = new HashMap<>();

  public OpReduceStateByKey(
      Node<ReduceStateByKey> node,
      ObservableStream<StreamElement> input,
      SnapshotableStorageProvider storageProvider,
      TriggerScheduler scheduler,
      KafkaStorageProvider processingState,
      OutputWriter output) {

    this.node = Objects.requireNonNull(node);
    this.input = Objects.requireNonNull(input);
    this.storageProvider = Objects.requireNonNull(storageProvider);
    this.scheduler = Objects.requireNonNull(scheduler);
    this.processingState = Objects.requireNonNull(processingState);
    this.output = Objects.requireNonNull(output);

    this.op = node.get();
    this.windowing = op.getWindowing();
    this.stateMerger = op.getStateMerger();
    this.partitioning = op.getPartitioning();
    this.valueExtractor = op.getValueExtractor();
    this.keyExtractor = op.getKeyExtractor();
    this.stateFactory = op.getStateFactory();
  }

  /** Run the reducer via given executor. */
  @SuppressWarnings("unchecked")
  public void run(Executor executor) {

    input.observe(new StreamObserver<StreamElement>() {

      VectorClock clock;

      @Override
      public void onAssign(int numPartitions) {
        clock = new VectorClock(numPartitions);

        // FIXME: reload the whole state and triggers
      }

      @Override
      public void onNext(int partitionId, StreamElement elem) {
        if (elem.isElement()) {
          clock.update(elem.getTimestamp(), partitionId);
          long now = clock.getCurrent();
          scheduler.updateStamp(now);
          processElement(elem, now);
        } else if (elem.isWindowTrigger()) {
          registerWindowEnd(elem.getWindow(), elem.getTimestamp());
        }
      }

      @Override
      public void onError(Throwable err) {
        // FIXME
      }

      @Override
      public void onCompleted() {
        processEndOfStream();
      }

    });

  }

  @SuppressWarnings("unchecked")
  private void processElement(StreamElement elem, long now) {
    Object key = keyExtractor.apply(elem.getElement());
    Object value = valueExtractor.apply(elem.getElement());
    Iterable<Window> windows = windowing.assignWindowsToElement(elem);

    for (Window window : windows) {
      KeyedWindow<Window, Object> wk = new KeyedWindow(window, key);
      State state = openedStates.get(wk);
      if (state == null) {
        openedStates.put(wk, state = stateFactory.createState(storageProvider, this));
      }
      windowing.getTrigger().onElement(now, window, this);
      state.add(value);
    }

  }

  private void registerWindowEnd(Window window, long timestamp) {

  }

  private void processEndOfStream() {

  }

  @Override
  public void collect(Object elem) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Object getWindow() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean registerTimer(long stamp, Window window) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void deleteTimer(long stamp, Window window) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long getCurrentTimestamp() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
    throw new UnsupportedOperationException("Not supported yet.");
  }



}
