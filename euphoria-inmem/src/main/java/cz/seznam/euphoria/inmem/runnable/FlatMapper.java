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

package cz.seznam.euphoria.inmem.runnable;

import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.inmem.WindowedElementCollector;
import cz.seznam.euphoria.inmem.stream.ObservableStream;
import cz.seznam.euphoria.inmem.stream.OutputWriter;
import cz.seznam.euphoria.inmem.stream.StreamElement;
import cz.seznam.euphoria.inmem.stream.StreamElementFactory;
import cz.seznam.euphoria.inmem.stream.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A runnable that performs {@code FlatMap} operation on stream.
 */
public class FlatMapper implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(FlatMapper.class);

  private final String name;
  private final ObservableStream<StreamElement<?>> input;
  private final OutputWriter<StreamElement<Object>> output;
  private final UnaryFunctor mapper;
  private final StreamElementFactory<Object, ? extends StreamElement<Object>> elementFactory;
  
  public FlatMapper(
      String name,
      ObservableStream<StreamElement<?>> input,
      OutputWriter<StreamElement<Object>> output,
      UnaryFunctor mapper,
      StreamElementFactory<Object, ? extends StreamElement<Object>> elementFactory) {

    this.name = name;
    this.input = input;
    this.output = output;
    this.mapper = mapper;
    this.elementFactory = elementFactory;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void run() {
    
    input.observe("flatmapper-" + name, new StreamObserver<StreamElement<?>>() {
      @Override
      public void onNext(StreamElement<?> elem) {
        WindowedElementCollector<Object> outC = new WindowedElementCollector<>(
            output::write,
            elem::getTimestamp,
            elementFactory);

        if (elem.isElement()) {
          // transform
          outC.setWindow(elem.getWindow());
          mapper.apply(elem.getElement(), outC);
        } else {
          try {
            output.write((StreamElement) elem);
          } catch (InterruptedException ex) {
            LOG.warn("Interrupted while writing to output", ex);
            Thread.currentThread().interrupt();
          }
        }
      }

      @Override
      public void onError(Throwable err) {
        LOG.error(
            "Failed to observe the input stream of operator {}",
            name, err);
      }

      @Override
      public void onCompleted() {
        try {
          output.write(elementFactory.endOfStream());
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }

    });
  }



}
