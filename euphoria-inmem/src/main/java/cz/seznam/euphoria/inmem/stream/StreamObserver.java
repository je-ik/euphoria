/**
 * Copyright 2016 Seznam.cz, a.s..
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

package cz.seznam.euphoria.inmem.stream;

/**
 * Observer of stream flowing between operators.
 */
public interface StreamObserver<T> {

  /**
   * Observe next element.
   * @param elem the new element read from the stream
   */
  void onNext(T elem);

  /**
   * Error occurred while observing the stream. This implies that no more {@code onNext}.
   * will be called on this stream.
   * @param err error that was caught during processing
   */
  void onError(Throwable err);

  /**
   * The stream has ended and no more {@code onNext} will be called.
   */
  void onCompleted();

}
