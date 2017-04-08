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
package cz.seznam.euphoria.inmem.stream;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

/**
 * A factory for implementations of {@code StreamElement}.
 * This factory must be provided by runtimes.
 */
public interface StreamElementFactory<D, T extends StreamElement<D>> {

  /** Create end-of-stream marker. */
  T endOfStream();

  /** Create watermark marker. */
  T watermark(long stamp);

  /** Create end-of-window marker. */
  T windowTrigger(Window window, long stamp);

  /** Create data payload element. */
  T data(Window window, long stamp, D data);

}
