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

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;

/**
 * Class passed internally in executor pipelines.
 * This class have to be efficiently serializable, which is key performance issue.
 * Uses protobuf serialization.
 */
class Datum<T> implements WindowedElement<Window, T> {

  private final Window window;
  private final long timestamp;
  private final T elem;

  Datum(Window window, long timestamp, T elem) {
    this.window = window;
    this.timestamp = timestamp;
    this.elem = elem;
  }

  @Override
  public Window getWindow() {
    return window;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public T getElement() {
    return elem;
  }

}
