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

package cz.seznam.euphoria.beam.io;

import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.Writer;
import java.io.IOException;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;

/**
 * Write to output sink using beam.
 */
@DoFn.BoundedPerElement
public class BeamWriteSink<T> extends PTransform<PCollectionList<T>, PDone> {
  
  public static <T> BeamWriteSink<T> wrap(Pipeline pipeline, DataSink<T> sink) {
    return new BeamWriteSink<>(pipeline, sink);
  }

  private static final class WriteFn<T> extends DoFn<T, Void> {

    final DataSink<T> sink;
    final int partitionId;
    Writer<T> writer = null;

    WriteFn(int partitionId, DataSink<T> sink) {
      this.partitionId = partitionId;
      this.sink = sink;
    }

    @Setup
    public void setup() {
      writer = sink.openWriter(partitionId);
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws IOException {
      T element = c.element();
      writer.write(element);
    }

    @Teardown
    public void finish() throws IOException{
      writer.commit();
      writer.close();
    }

  }

  private final Pipeline pipeline;
  private final DataSink<T> sink;

  BeamWriteSink(Pipeline pipeline, DataSink<T> sink) {
    this.pipeline = Objects.requireNonNull(pipeline);
    this.sink = Objects.requireNonNull(sink);
  }

  @Override
  public PDone expand(PCollectionList<T> input) {
    int partitionId = 0;
    for (PCollection<T> partition : input.getAll()) {
      partition.apply(ParDo.of(new WriteFn<>(partitionId++, sink)));
    }
    return PDone.in(pipeline);
  }

}
