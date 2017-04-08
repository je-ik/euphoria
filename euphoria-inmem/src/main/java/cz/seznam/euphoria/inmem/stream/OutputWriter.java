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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A writer to output stream for consumption of another operator.
 */
public interface OutputWriter<T> {
  
  /**
   * Commit callback called when the output finishes.
   */
  public static interface Callback {
    
    /**
     * Confirm the write.
     * @param succ whether the call succeeded or failed
     * @param err error that occurred if the {@code succ} is {@code false}.
     *            {@code null} otherwise
     */
    void confirm(boolean succ, Throwable err);
    
  }

  /**
   * Write data to the output stream.
   * @param elem the element to output
   * @param callback commit callback
   */
  void write(T elem, Callback callback);

  /**
   * Write the output synchronously, throwing any exceptions as
   * runtime exceptions.
   * @param elem the element to write to the output
   * @throws InterruptedException when interrupted during the synchronous waiting
   */
  default void write(T elem) throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> err = new AtomicReference<>();
    write(elem, (succ, exc) -> {
      latch.countDown();
      if (!succ) {
        err.set(exc);
      }
    });
    latch.await();
    if (err.get() != null) {
      if (err.get() instanceof InterruptedException) {
        throw (InterruptedException) err.get();
      }
      throw new RuntimeException(err.get());
    }

  }

}
