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
package cz.seznam.euphoria.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.TimedWindow;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.inmem.stream.StreamElement;
import cz.seznam.euphoria.inmem.stream.StreamElementFactory;

import java.util.Objects;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowedElementCollector<T> implements Context<T> {

  private static final Logger LOG = LoggerFactory.getLogger(WindowedElementCollector.class);

  private final Collector<StreamElement<T>> wrap;
  private final Supplier<Long> stampSupplier;
  private final StreamElementFactory<T, ? extends StreamElement<T>> elementFactory;

  protected Window window;

  public WindowedElementCollector(
      Collector<StreamElement<T>> wrap,
      Supplier<Long> stampSupplier,
      StreamElementFactory<T, ? extends StreamElement<T>> elementFactory) {

    this.wrap = Objects.requireNonNull(wrap);
    this.stampSupplier = stampSupplier;
    this.elementFactory = elementFactory;
  }

  @Override
  public void collect(T elem) {
    // ~ timestamp assigned to element can be either end of window
    // or current watermark supplied by triggering
    // ~ this is a workaround for NoopTriggerScheduler
    // used for batch processing that fires all windows
    // at the end of bounded input
    long stamp = (window instanceof TimedWindow)
            ? ((TimedWindow) window).maxTimestamp()
            : stampSupplier.get();

    try {
      StreamElement<T> data = elementFactory.data(window, stamp, elem);
      wrap.collect(data);
    } catch (InterruptedException ex) {
      LOG.warn("Interrupted while collecting element {}", elem);
      Thread.currentThread().interrupt();
    }
  }

  public void setWindow(Window window) {
    this.window = window;
  }

  @Override
  public Object getWindow() {
    return window;
  }

}
