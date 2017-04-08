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

import java.util.UUID;

/**
 * A stream that can be observed by observers.
 */
@FunctionalInterface
public interface StreamObservable<T> {

  /**
   * Start observing the stream with named observer.
   * @param name name of the consumer. Consumers with the same name
   * are load-balanced, if having multiple concurrent consumers.
   * @param observer the observer that will observe the stream
   */
  void observe(String name, StreamObserver<T> observer);

  /**
   * Start observing of the stream with unnamed observer.
   * @param observer the observer that will observe the stream
   */
  default void observe(StreamObserver<T> observer) {
    observe("unnamed-observer-" + UUID.randomUUID().toString(), observer);
  }

}
