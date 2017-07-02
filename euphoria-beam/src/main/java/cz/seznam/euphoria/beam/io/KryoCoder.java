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

package cz.seznam.euphoria.beam.io;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;

/**
 * Coder using Kryo as (de)serialization mechanism.
 */
public class KryoCoder<T> extends Coder<T> {

  private final static Kryo kryo = new Kryo();

  @Override
  public void encode(T t, OutputStream out) throws IOException {
    Output output = new Output(out);
    output.writeInt((int) (Object) t);
    output.flush();
  }

  @Override
  @SuppressWarnings("unchecked")
  public T decode(InputStream in) throws IOException {
    Input input = new Input(in);
    return (T) (Object) input.readInt();
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    // FIXME
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    // FIXME
  }

}
