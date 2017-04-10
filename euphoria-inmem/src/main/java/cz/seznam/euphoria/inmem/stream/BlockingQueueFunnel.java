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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * A combiner of multiple {@code BlockingQueue}s into single one.
 */
public class BlockingQueueFunnel<T> {

  public static <T> BlockingQueueFunnel<T> of(
      List<BlockingQueue<T>> inputs,
      BlockingQueue<T> output) {

    return new BlockingQueueFunnel<>(inputs, output);
  }

  private final List<BlockingQueue<T>> inputs;
  private final BlockingQueue<T> output;
  private final List<Thread> forwards = new ArrayList<>();

  BlockingQueueFunnel(List<BlockingQueue<T>> inputs, BlockingQueue<T> output) {
    this.inputs = inputs;
    this.output = output;
    for (int i = 0; i < inputs.size(); i++) {
      final int p = i;
      Thread thread = new Thread(() -> {
        BlockingQueue<T> input = inputs.get(p);
        while (!Thread.currentThread().isInterrupted()) {
          try {
            output.put(input.take());
          } catch (InterruptedException ex) {
            break;
          }
        }
      });
      thread.setDaemon(true);
      forwards.add(thread);
    }
  }

  public void run() {
    forwards.forEach(Thread::start);
  }

}
