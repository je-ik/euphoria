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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

/**
 * A stream observer with implementation wrap {@code BlockingQueue}.
 */
public class BlockingQueueStreamObservable<T extends StreamElement<?>>
    implements ObservableStream<T> {

  /**
   * Create the observable from blocking queue.
   * @param <T> the type of the elements in the queue
   * @param executor the executor to use to execute asynchronous tasks
   * @param queue the blocking queue to wrap the observable around
   * @return the wrapped {@code ObservableStream}
   */
  public static <T extends StreamElement<?>> BlockingQueueStreamObservable<T> wrap(
      Executor executor,
      BlockingQueue<T> queue) {

    return new BlockingQueueStreamObservable<>(executor, queue);
  }

  private final Executor executor;
  private final BlockingQueue<T> queue;
  private final List<StreamObserver<T>> observers = new ArrayList<>();

  private BlockingQueueStreamObservable(
      Executor executor, BlockingQueue<T> queue) {

    this.executor = executor;
    this.queue = queue;
  }

  @Override
  public void observe(String name, StreamObserver<T> observer) {

    synchronized (observers) { 
      if (observers.isEmpty()) {
        executor.execute(() -> {
          try {
            while (!Thread.currentThread().isInterrupted()) {
              try {
                T elem = queue.take();
                if (elem.isEndOfStream()) {
                  break;
                }
                synchronized (observers) {
                  observers.forEach(o -> o.onNext(elem));
                }
              } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
              }
            }
            synchronized (observers) {
              observers.forEach(o -> o.onCompleted());
            }

          } catch (Throwable err) {
            synchronized (observers) {
              observers.forEach(o -> o.onError(err));
            }
          }
        });
      }

      observers.add(observer);
    }
  }


}

