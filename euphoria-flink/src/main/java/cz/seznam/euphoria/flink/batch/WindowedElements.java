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
package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.util.Arrays;
import java.util.Objects;

public class WindowedElements {

  public static <W extends Window, T> TypeInformation<WindowedElement<W, T>>
  of(Class<W> window, Class<T> value) {
    return of(window, TypeExtractor.createTypeInfo(value));
  }

  @SuppressWarnings("unchecked")
  public static <W extends Window, T> TypeInformation<WindowedElement<W, T>>
  of(Class<W> window, TypeInformation<T> valueInfo) {
    Objects.requireNonNull(window);
    Objects.requireNonNull(valueInfo);
    try {
      return new PojoTypeInfo(WindowedElement.class,
          Arrays.asList(
              new PojoField(WindowedElement.class.getField("window"), TypeExtractor.createTypeInfo(window)),
              new PojoField(WindowedElement.class.getField("timestamp"), TypeExtractor.createTypeInfo(Long.class)),
              new PojoField(WindowedElement.class.getField("element"), valueInfo)));
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  private WindowedElements() {}
}
