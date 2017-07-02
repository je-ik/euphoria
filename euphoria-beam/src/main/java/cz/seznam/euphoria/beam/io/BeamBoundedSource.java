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

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * A {@code BoundedSource} created from {@code DataSource}.
 */
public class BeamBoundedSource<T> extends BoundedSource<T> {
  
  public static <T> BeamBoundedSource wrap(DataSource<T> wrap) {
    return new BeamBoundedSource<>(wrap);
  }

  final DataSource<T> wrap;
  @Nullable
  Partition<T> partition;

  private BeamBoundedSource(DataSource<T> wrap) {
    this.wrap = Objects.requireNonNull(wrap);
  }

  private BeamBoundedSource(DataSource<T> wrap, Partition<T> partition) {
    this.wrap = wrap;
    this.partition = partition;
  }

  @Override
  public List<? extends BoundedSource<T>> split(long ignore, PipelineOptions po)
      throws Exception {

    // the split is defined by the source itself
    return wrap.getPartitions().stream()
        .map(p -> new BeamBoundedSource<>(wrap, p))
        .collect(Collectors.toList());
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions po) throws Exception {
    // not supported
    return -1L;
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions po) throws IOException {
    Objects.requireNonNull(
        partition,
        "Partition cannot be null, the source for concrete partition has to be "
            + "create by calling the `split` method");
    cz.seznam.euphoria.core.client.io.Reader<T> reader = partition.openReader();
    return new BoundedReader<T>() {

      @Override
      public BoundedSource<T> getCurrentSource() {
        return BeamBoundedSource.this;
      }

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        return reader.hasNext();
      }

      @Override
      public T getCurrent() throws NoSuchElementException {
        return reader.next();
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }

    };
  }

  @Override
  public void validate() {
    // FIXME
  }

  @Override
  public Coder<T> getDefaultOutputCoder() {
    return new KryoCoder<>();
  }

}
