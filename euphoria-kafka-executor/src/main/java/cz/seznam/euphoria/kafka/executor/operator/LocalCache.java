/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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

package cz.seznam.euphoria.kafka.executor.operator;

/**
 * Local cache implementation of stream coming from Kafka commir log.
 */
public interface LocalCache<K, V> extends AutoCloseable {


  /**
   * Store new value to the cache. Only the latest value will be stored.
   * The implementation has to be thread-safe.
   * @param key key of the record
   * @param value the deserialized value
   */
  void update(K key, V value);


  /**
   * Add new value to the to the key. All values stored for this key
   * using `add` call has to be retained forever and returned to call to
   * `getAll`.
   */
  void add(K key, V value);


  /**
   * Retrieve the last value inserted by call to `update` for this key.
   * The implementation has to be thread-safe.
   */
  V getLatest(K key);


  /**
   * Retrieve all values stored by call to `add`.
   */
  Iterable<V> getAll(K key);


  /**
   * Clear all values associated with given key.
   */
  void reset(K key);


  /**
   * Checkpoint the cache to safe and persistent location.
   * @param metadata any bytes to be stored inside the checkpoint (besides the
   * checkpointed data itself).
   */
  void checkpoint(byte[] metadata);


  /**
   * Restore last available checkpoint.
   * @return the metadata stored during call to checkpoint
   */
  byte[] restore();



  @Override
  public void close();

  
}
