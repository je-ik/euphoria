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
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;

/**
 * An interface each element in stream pipelines must implement.
 */
public interface StreamElement<T> extends WindowedElement<Window, T> {

  /** Is this regular element message? */
  boolean isElement();

  /** Is this end-of-stream message? */
  boolean isEndOfStream();

  /** Is this watermark message? */
  boolean isWatermark();

  /** Is this window trigger event? */
  boolean isWindowTrigger();

}
