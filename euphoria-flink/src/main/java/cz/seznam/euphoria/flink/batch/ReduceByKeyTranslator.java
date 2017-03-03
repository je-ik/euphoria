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
package cz.seznam.euphoria.flink.batch;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.TimedWindow;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.Operator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

public class ReduceByKeyTranslator implements BatchOperatorTranslator<ReduceByKey> {

  static boolean wantTranslate(ReduceByKey operator) {
    boolean b = operator.isCombinable()
        && (operator.getWindowing() == null
            || (!(operator.getWindowing() instanceof MergingWindowing)
                && !operator.getWindowing().getTrigger().isStateful()));
    return b;
  }

  @Override
  @SuppressWarnings("unchecked")
  public DataSet translate(FlinkOperator<ReduceByKey> operator,
                           BatchExecutorContext context) {

    // FIXME parallelism should be set to the same level as parent until we reach "shuffling"

    DataSet input = Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceByKey origOperator = operator.getOriginalOperator();
    final UnaryFunction<Iterable, Object> reducer = origOperator.getReducer();
    final Windowing windowing =
        origOperator.getWindowing() == null
        ? AttachedWindowing.INSTANCE
        : origOperator.getWindowing();

    Preconditions.checkState(origOperator.isCombinable(),
        "Non-combinable ReduceByKey not supported!");
    Preconditions.checkState(
        !(windowing instanceof MergingWindowing),
        "MergingWindowing not supported!");
    Preconditions.checkState(!windowing.getTrigger().isStateful(),
        "Stateful triggers not supported!");

    // ~ prepare key and value functions
    final UnaryFunction udfKey = origOperator.getKeyExtractor();
    final UnaryFunction udfValue = origOperator.getValueExtractor();

    Objects.requireNonNull(udfKey.getReturnType());
    Objects.requireNonNull(windowing.getWindowType());

    // ~ extract key/value from input elements and assign windows
    DataSet<Tuple4<Long, Window, Comparable, Object>> tuples;
    {
      // FIXME require keyExtractor to deliver `Comparable`s

      UnaryFunction<Object, Long> timeAssigner = origOperator.getEventTimeAssigner();
      FlatMapOperator<Object, Tuple4<Long, Window, Comparable, Object>> wAssigned =
          input.flatMap((i, c) -> {
            WindowedElement wel = (WindowedElement) i;
            if (timeAssigner != null) {
              long stamp = timeAssigner.apply(wel.getElement());
              wel.setTimestamp(stamp);
            }
            Set<Window> assigned = windowing.assignWindowsToElement(wel);
            for (Window wid : assigned) {
              Object el = wel.getElement();
              long stamp = (wid instanceof TimedWindow)
                  ? ((TimedWindow) wid).maxTimestamp()
                  : wel.getTimestamp();
              c.collect(new Tuple4<>(stamp, wid, udfKey.apply(el), udfValue.apply(el)));
            }
          });
      tuples = wAssigned
          .name(operator.getName() + "::map-input")
          .setParallelism(operator.getParallelism())
          .returns(new TupleTypeInfo<>(
              TypeExtractor.createTypeInfo(Long.class),
              TypeExtractor.createTypeInfo(windowing.getWindowType()),
              TypeExtractor.createTypeInfo(udfKey.getReturnType()),
              TypeExtractor.createTypeInfo(Object.class)));
    }

    // ~ reduce the data now
    Operator<Tuple4<Long, Window, Comparable, Object>, ?> reduced;
    reduced = tuples
        .groupBy(new RBKKeySelector(windowing.getWindowType(), udfKey.getReturnType()))
        .reduce(new RBKReducer(reducer, windowing.getWindowType(), udfKey.getReturnType()))
        // ~ use hash-based reduction to avoid deserialization of the values
        .setCombineHint(ReduceOperatorBase.CombineHint.HASH);
    reduced = reduced
        .setParallelism(operator.getParallelism())
        .name(operator.getName() + "::reduce");

    // FIXME partitioner should be applied during "reduce" to avoid
    // unnecessary shuffle, but there is no (known) way how to set custom
    // partitioner to "groupBy" transformation

    // apply custom partitioner if different from default
    if (!origOperator.getPartitioning().hasDefaultPartitioner()) {
      reduced = reduced
          .partitionCustom(
              new PartitionerWrapper<>(origOperator.getPartitioning().getPartitioner()), 2)
          .setParallelism(operator.getParallelism());
    }

    return reduced.map((MapFunction<Tuple4<Long, Window, Comparable, Object>,
        WindowedElement<Window, Pair<Comparable, Object>>>) in -> new WindowedElement<>(in.f1, in.f0, Pair.of(in.f2, in.f3)))
        .returns(outputType(windowing.getWindowType(), udfKey.getReturnType()))
        .setParallelism(operator.getParallelism())
        .name("::map-to-windowed-element");
  }

  // ------------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  static <W extends Window, K> TypeInformation<WindowedElement<W, Pair<K, Object>>>
  outputType(Class<W> windowType, Class<K> keyType) {
    try {
      return WindowedElements.of(windowType, new PojoTypeInfo(Pair.class,
          Arrays.asList(
              new PojoField(Pair.class.getField("first"), TypeExtractor.createTypeInfo(keyType)),
              new PojoField(Pair.class.getField("second"), TypeExtractor.createTypeInfo(Object.class)))));
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Produces Tuple2[Window, Element Key]
   */
  @SuppressWarnings("unchecked")
  static class RBKKeySelector<W, K>
      implements KeySelector<Tuple4<Long, W, K, Object>, Tuple2<W, K>>,
      ResultTypeQueryable<Tuple2<W, K>> {

    private final TupleTypeInfo<Tuple2<W, K>> tt;

    public RBKKeySelector(Class<W> windowType, Class<K> keyType) {
      TypeInformation<W> wt = TypeExtractor.getForClass(windowType);
      TypeInformation<K> kt = TypeExtractor.getForClass(keyType);
      this.tt = new TupleTypeInfo<>(wt, kt);
    }

    @Override
    public Tuple2<W, K> getKey(Tuple4<Long, W, K, Object> in) {
      return new Tuple2(in.f1, in.f2);
    }

    @Override
    public TypeInformation<Tuple2<W, K>> getProducedType() {
      return tt;
    }
  }

  @FunctionAnnotation.ForwardedFieldsFirst("f1->f1; f2->f2")
  static class RBKReducer<W extends Window, K>
        implements ReduceFunction<Tuple4<Long, W, K, Object>>,
              ResultTypeQueryable<Tuple4<Long, W, K, Object>> {

    final UnaryFunction<Iterable, Object> reducer;
    final TupleTypeInfo<Tuple4<Long, W, K, Object>> tt;

    RBKReducer(UnaryFunction<Iterable, Object> reducer, Class<W> windowType, Class<K> keyType) {
      this.reducer = reducer;
      this.tt = new TupleTypeInfo<>(
          TypeExtractor.getForClass(Long.class),
          TypeExtractor.getForClass(windowType),
          TypeExtractor.getForClass(keyType),
          TypeExtractor.getForClass(Object.class));
    }

    @Override
    public Tuple4<Long, W, K, Object>
    reduce(Tuple4<Long, W, K, Object> p1, Tuple4<Long, W, K, Object> p2) {
      return new Tuple4<>(
          Math.max(p1.f0, p2.f0), // max. stamp
          p1.f1, // window
          p1.f2, // key
          reducer.apply(Arrays.asList(p1.f3, p2.f3)));
    }

    @Override
    public TypeInformation<Tuple4<Long, W, K, Object>> getProducedType() {
      return tt;
    }
  }
}
