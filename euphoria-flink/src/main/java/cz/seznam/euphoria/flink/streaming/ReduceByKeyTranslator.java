/**
 * Copyright 2016 Seznam.cz, a.s.
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
package cz.seznam.euphoria.flink.streaming;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.Utils;
import cz.seznam.euphoria.flink.functions.IteratorIterable;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.flink.streaming.windowing.FlinkWindow;
import cz.seznam.euphoria.flink.streaming.windowing.FlinkWindowTrigger;
import cz.seznam.euphoria.flink.streaming.windowing.MultiWindowedElement;
import cz.seznam.euphoria.flink.streaming.windowing.MultiWindowedElementWindowFunction;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
class ReduceByKeyTranslator implements StreamingOperatorTranslator<ReduceByKey> {

  @Override
  public DataStream<?> translate(FlinkOperator<ReduceByKey> operator,
                                 StreamingExecutorContext context) {
    DataStream<?> input =
            Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceByKey origOperator = operator.getOriginalOperator();
    final UnaryFunction<Iterable, Object> reducer = origOperator.getReducer();
    final UnaryFunction keyExtractor = origOperator.getKeyExtractor();
    final UnaryFunction valueExtractor = origOperator.getValueExtractor();
    final Windowing windowing = origOperator.getWindowing();
    final UnaryFunction eventTimeAssigner = origOperator.getEventTimeAssigner();

    // apply windowing first
    SingleOutputStreamOperator<WindowedElement<?, Pair>> reduced;
    if (windowing == null) {
      throw new UnsupportedOperationException("OOPS - no attached windowing");
    } else {
      input = context.windower.applyEventTime((DataStream) input, eventTimeAssigner);
      if (origOperator.isCombinable()) {
        KeySelector kvKeySelector =
                Utils.wrapQueryable((WindowedElement we) -> keyExtractor.apply(we.getElement()));
        KeyedStream keyed = input.keyBy(kvKeySelector);
        if (windowing instanceof MergingWindowing) {
          throw new UnsupportedOperationException("OOPS - no merging windowing");
        }

        WindowedStream windowed = keyed.window(new WX_WindowAssigner(windowing));
        // reduce incrementally
        reduced = windowed.apply(null, new WX_IncrementalReducer(valueExtractor, reducer), new WX_WindowFunction());
      } else {
        throw new UnsupportedOperationException("OOPS - no non-combining reduce");
      }
    }

    DataStream<WindowedElement<?, Pair>> out =
            reduced.name(operator.getName())
                   .setParallelism(operator.getParallelism());

    // FIXME partitioner should be applied during "reduce" to avoid
    // unnecessary shuffle, but there is no (known) way how to set custom
    // partitioner to "keyBy" transformation

    // apply custom partitioner if different from default
    if (!origOperator.getPartitioning().hasDefaultPartitioner()) {
      out = out.partitionCustom(
              new PartitionerWrapper<>(origOperator.getPartitioning().getPartitioner()),
              p -> p.getElement().getKey());
    }

    return out;
  }

  private static class WX_IncrementalReducer<I, O>
          implements FoldFunction<WindowedElement<?, I>, O>,
          ResultTypeQueryable<O> {

    final UnaryFunction<Iterable<O>, O> reducer;
    final UnaryFunction<I, O> valueFn;

    public WX_IncrementalReducer(UnaryFunction<I, O> valueFn, UnaryFunction<Iterable<O>, O> reducer) {
      this.valueFn = valueFn;
      this.reducer = reducer;
    }

    @Override
    public O fold(O accumulator, WindowedElement<?, I> input) throws Exception {
      O value = valueFn.apply(input.getElement());
      return accumulator == null ? value : reducer.apply(Arrays.asList(accumulator, value));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<O> getProducedType() {
      return TypeInformation.of((Class) Object.class);
    }
  }

  public class WX_WindowFunction<WID extends cz.seznam.euphoria.core.client.dataset.windowing.Window, KEY, VALUE>
          implements WindowFunction<
          VALUE,
          WindowedElement<WID, Pair<KEY, VALUE>>,
          KEY,
          FlinkWindow<WID>> {

    @Override
    public void apply(KEY key,
                      FlinkWindow<WID> window,
                      Iterable<VALUE> input,
                      Collector<WindowedElement<WID, Pair<KEY, VALUE>>> out)
            throws Exception {
      for (VALUE i : input) {
        out.collect(new WindowedElement<>(
                window.getWindowID(),
                window.getEmissionWatermark(),
                Pair.of(key, i)));
      }
    }
  }

  //  ~ basically a copy of FlinkWindowAssigner dependent on WindowedElement instead of MultiWindowedElementWindowFunction
  private static class WX_WindowAssigner<T, WID extends cz.seznam.euphoria.core.client.dataset.windowing.Window>
          extends WindowAssigner<WindowedElement<WID, T>, FlinkWindow<WID>> {

    private final Windowing<T, WID> windowing;

    public WX_WindowAssigner(Windowing<T, WID> windowing) {
      this.windowing = windowing;
    }

    Windowing<T, WID> getWindowing() {
      return this.windowing;
    }

    @Override
    public Collection<FlinkWindow<WID>> assignWindows(
            WindowedElement<WID, T> element,
            long timestamp, WindowAssignerContext context) {

      return this.windowing.assignWindowsToElement(element)
              // map collection of Euphoria WIDs to FlinkWindows
              .stream()
              .map(FlinkWindow::new)
              .collect(Collectors.toList());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Trigger<WindowedElement<WID, T>, FlinkWindow<WID>> getDefaultTrigger(
            StreamExecutionEnvironment env) {
      return new FlinkWindowTrigger(windowing.getTrigger());
    }

    @Override
    public org.apache.flink.api.common.typeutils.TypeSerializer<FlinkWindow<WID>>
    getWindowSerializer(ExecutionConfig executionConfig) {
      return new KryoSerializer(FlinkWindow.class, executionConfig);
    }

    @Override
    public boolean isEventTime() {
      return true;
    }
  }
  /**
   * Performs incremental reduction (in case of combining reduce).
   */
  private static class WindowedElementIncrementalReducer
          implements ReduceFunction<WindowedElement<?, Pair>>,
          ResultTypeQueryable<WindowedElement<?, Pair>> {

    final UnaryFunction<Iterable, Object> reducer;

    public WindowedElementIncrementalReducer(UnaryFunction<Iterable, Object> reducer) {
      this.reducer = reducer;
    }

    @Override
    public WindowedElement<?, Pair> reduce(
            WindowedElement<?, Pair> p1,
            WindowedElement<?, Pair> p2) {

      Object v1 = p1.getElement().getSecond();
      Object v2 = p2.getElement().getSecond();
      return new WindowedElement<>(
        p1.getWindow(),
        p1.getTimestamp(),
        Pair.of(p1.getElement().getKey(), reducer.apply(Arrays.asList(v1, v2))));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<WindowedElement<?, Pair>> getProducedType() {
      return TypeInformation.of((Class) WindowedElement.class);
    }
  } // ~ end of WindowedElementIncrementalReducer

  /**
   * Performs non-incremental reduction (in case of non-combining reduce).
   */
  private static class WindowedElementWindowedReducer
          implements WindowFunction<
          WindowedElement<?, Pair>,
          WindowedElement<?, Pair>,
          Object,
          Window> {

    private final UnaryFunction<Iterable, Object> reducer;
    private final WindowFunction<WindowedElement<?, Pair>,
            WindowedElement<?, Pair>, Object, Window> emissionFunction;

    @SuppressWarnings("unchecked")
    public WindowedElementWindowedReducer(UnaryFunction<Iterable, Object> reducer,
                                                   WindowFunction emissionFunction) {
      this.reducer = reducer;
      this.emissionFunction = emissionFunction;
    }

    @Override
    public void apply(Object key,
                      Window window,
                      Iterable<WindowedElement<?, Pair>> input,
                      Collector<WindowedElement<?, Pair>> collector)
            throws Exception {

      Iterator<WindowedElement<?, Pair>> it = input.iterator();

      // read the first element to obtain window metadata
      WindowedElement<?, Pair> element = it.next();
      cz.seznam.euphoria.core.client.dataset.windowing.Window wid = element.getWindow();
      long emissionWatermark = element.getTimestamp();

      // concat the already read element with rest of the opened iterator
      Iterator<WindowedElement<?, Pair>> concatIt =
              Iterators.concat(Iterators.singletonIterator(element), it);

      // unwrap all elements to be used in user defined reducer
      Iterator<Object> unwrapped =
              Iterators.transform(concatIt, e -> e.getElement().getValue());

      Object reduced = reducer.apply(new IteratorIterable<>(unwrapped));

      WindowedElement<?, Pair> out =
          new WindowedElement<>(wid, emissionWatermark, Pair.of(key, reduced));

      // decorate resulting item with emission watermark from fired window
      emissionFunction.apply(key, window, Collections.singletonList(out), collector);
    }
  } // ~ end of WindowedElementWindowedReducer


  /**
   * Performs incremental reduction (in case of combining reduce) on
   * {@link MultiWindowedElement}s. Assumes the result is emitted using
   * {@link MultiWindowedElementWindowFunction}.
   */
  private static class MultiWindowedElementIncrementalReducer<
          WID extends cz.seznam.euphoria.core.client.dataset.windowing.Window, KEY, VALUE>
      implements ReduceFunction<MultiWindowedElement<WID, Pair<KEY, VALUE>>>,
      ResultTypeQueryable<MultiWindowedElement<WID, Pair<KEY, VALUE>>> {

    final UnaryFunction<Iterable<VALUE>, VALUE> reducer;

    public MultiWindowedElementIncrementalReducer(UnaryFunction<Iterable<VALUE>, VALUE> reducer) {
      this.reducer = reducer;
    }

    @Override
    public MultiWindowedElement<WID, Pair<KEY, VALUE>> reduce(
        MultiWindowedElement<WID, Pair<KEY, VALUE>> p1,
        MultiWindowedElement<WID, Pair<KEY, VALUE>> p2) {

      VALUE v1 = p1.getElement().getSecond();
      VALUE v2 = p2.getElement().getSecond();
      Set<WID> s = Collections.emptySet();
      return new MultiWindowedElement<>(s,
          Pair.of(p1.getElement().getFirst(), reducer.apply(Arrays.asList(v1, v2))));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<MultiWindowedElement<WID, Pair<KEY, VALUE>>>
    getProducedType() {
      return TypeInformation.of((Class) MultiWindowedElement.class);
    }
  } // ~ end of MultiWindowedElementIncrementalReducer

  /**
   * Performs non-incremental reduction (in case of non-combining reduce).
   */
  private static class MultiWindowedElementWindowedReducer<
          WID extends cz.seznam.euphoria.core.client.dataset.windowing.Window,
          KEY, VALUEIN, VALUEOUT>
      implements WindowFunction<
      MultiWindowedElement<?, Pair<KEY, VALUEIN>>,
      WindowedElement<WID, Pair<KEY, VALUEOUT>>,
      KEY,
      FlinkWindow<WID>> {

    private final UnaryFunction<Iterable<VALUEIN>, VALUEOUT> reducer;
    private final MultiWindowedElementWindowFunction<WID, KEY, VALUEOUT> emissionFunction;

    public MultiWindowedElementWindowedReducer(
        UnaryFunction<Iterable<VALUEIN>, VALUEOUT> reducer,
        MultiWindowedElementWindowFunction<WID, KEY, VALUEOUT> emissionFunction) {
      this.reducer = reducer;
      this.emissionFunction = emissionFunction;
    }

    @Override
    public void apply(KEY key,
                      FlinkWindow<WID> window,
                      Iterable<MultiWindowedElement<?, Pair<KEY, VALUEIN>>> input,
                      Collector<WindowedElement<WID, Pair<KEY, VALUEOUT>>> collector)
        throws Exception {

      VALUEOUT reducedValue = reducer.apply(new IteratorIterable<>(
          Iterators.transform(input.iterator(), e -> e.getElement().getValue())));
      MultiWindowedElement<WID, Pair<KEY, VALUEOUT>> reduced =
          new MultiWindowedElement<>(Collections.emptySet(), Pair.of(key, reducedValue));
      this.emissionFunction.apply(key, window,
          Collections.singletonList(reduced), collector);
    }
  } // ~ end of MultiWindowedElementWindowedReducer

}
