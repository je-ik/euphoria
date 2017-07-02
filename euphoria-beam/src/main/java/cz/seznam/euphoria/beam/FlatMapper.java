/**
 * Copyright 2017 Seznam.cz, a.s.
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

package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.beam.io.ListCollector;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * Function passed into flat map operation.
 */
@SuppressWarnings("unchecked")
public class FlatMapper extends SimpleFunction<Object, Iterable<Object>> {

  private final UnaryFunctor functor;
  private transient ListCollector collector;

  FlatMapper(UnaryFunctor functor) {
    this.functor = functor;
  }

  @Override
  public Iterable<Object> apply(Object input) {
    if (collector == null) {
      collector = new ListCollector();
    }
    collector.clear();
    functor.apply(input, collector);
    return collector.get();
  }

}
