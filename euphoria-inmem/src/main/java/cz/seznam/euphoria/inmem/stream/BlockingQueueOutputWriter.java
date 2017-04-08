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

import java.util.concurrent.BlockingQueue;

/**
 * {@code OutputWriter} into {@code BlockingQueue}.
 */
public class BlockingQueueOutputWriter<T> implements OutputWriter<T> {
  
  public static <T> BlockingQueueOutputWriter<T> wrap(BlockingQueue<T> queue) {
    return new BlockingQueueOutputWriter<>(queue);
  }

  private final BlockingQueue<T> queue;

  private BlockingQueueOutputWriter(BlockingQueue<T> queue) {
    this.queue = queue;
  }

  @Override
  public void write(T elem, Callback callback) {
    try {
      queue.put(elem);
      callback.confirm(true, null);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      callback.confirm(false, ex);
    } catch (Throwable ex) {
      callback.confirm(false, ex);
    }
  }

}
