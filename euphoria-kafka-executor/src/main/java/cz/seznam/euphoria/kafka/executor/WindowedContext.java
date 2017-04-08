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

import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.inmem.stream.OutputWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@code Context} passed to client UDFs.
 */
public class WindowedContext implements Context {

  private static final Logger LOG = LoggerFactory.getLogger(WindowedContext.class);

  final OutputWriter<Datum<Object>> writer;
  Datum<?> origin;
  
  @SuppressWarnings("unchecked")
  WindowedContext(OutputWriter<Datum<?>> writer) {
    this.writer = (OutputWriter) writer;
  }

  void set(Datum<?> origin) {
    this.origin = origin;
  }

  @Override
  public void collect(Object elem) {
    try {
      writer.write(new Datum<>(origin.getWindow(), origin.getTimestamp(), elem));
    } catch (InterruptedException ex) {
      LOG.warn("Interrupted while collecting result of operator", ex);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public Object getWindow() {
    return origin.getWindow();
  }

}
