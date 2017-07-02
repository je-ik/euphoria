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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Write to output sink using beam.
 */
public class BeamWriteSink<T> extends PTransform<PCollection<T>, PDone> {
  
  public static <T> BeamWriteSink<T> wrap(Pipeline pipeline, DataSink<T> sink) {
    return new BeamWriteSink<>(pipeline, sink);
  }

  private static final class WriteFn<T> extends DoFn<T, Void> {

    final DataSink<T> sink;
    Map<Integer, Writer<T>> openWriters = new HashMap<>();

    WriteFn(DataSink<T> sink) {
      this.sink = sink;
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws IOException {
      int partitionId = (int) c.pane().getIndex();
      Writer<T> writer = openWriters.get(partitionId);
      if (writer == null) {
        openWriters.put(partitionId, writer = sink.openWriter(partitionId));
      }
      T element = c.element();
      writer.write(element);
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) throws Exception {
      openWriters.values().forEach(w -> {
        try {
          w.commit();
          w.close();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      });
    }
  }

  private final Pipeline pipeline;
  private final DataSink<T> sink;

  BeamWriteSink(Pipeline pipeline, DataSink<T> sink) {
    this.pipeline = Objects.requireNonNull(pipeline);
    this.sink = Objects.requireNonNull(sink);
  }

  @Override
  public PDone expand(PCollection<T> input) {
    input.apply(ParDo.of(new WriteFn<>(sink)));
    return PDone.in(pipeline);
  }

}
