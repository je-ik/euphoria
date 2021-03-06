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
package cz.seznam.euphoria.core.client.io;

/**
 * A collector of elements. Used in functors.
 *
 * @param <T> the type of elements collected through this context
 */
public interface Context<T> {

  /**
   * Collects the given element to the output of this context.
   *
   * @param elem the element to collect
   */
  void collect(T elem);

  /**
   * Retrieves the window - if any - underlying the current
   * execution of this context.
   *
   * @return {@code null} if this context is not executed within a
   *          windowing strategy, otherwise the current window of
   *          this context
   */
  Object getWindow();

}