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

package cz.seznam.euphoria.kafka.executor;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

/**
 * A record stored in Kafka topic.
 */
class KafkaRecord {
  
  /** The associated window. */
  final Window window;

  /** The value itself. */
  final byte[] value;

  KafkaRecord(Window window, byte[] value) {
    this.window = window;
    this.value = value;
  }

  public Window getWindow() {
    return window;
  }

  public byte[] getValue() {
    return value;
  }

  static KafkaRecord fromBytes(byte[] bytes) {
    // FIXME
    return null;
  }

  byte[] toBytes() {
    // FIXME
    return null;
  }

}
