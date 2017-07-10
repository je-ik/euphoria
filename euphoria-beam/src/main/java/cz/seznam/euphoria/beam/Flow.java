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

import cz.seznam.euphoria.core.util.Settings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * Extension of {@code cz.seznam.euphoria.core.client.flow.Flow} that wraps {@code Pipeline}.
 */
@SuppressFBWarnings(
    value = "NM_SAME_SIMPLE_NAME_AS_SUPERCLASS",
    justification = "This is just a PoC. Will be merged with the super class later.")
public class Flow extends cz.seznam.euphoria.core.client.flow.Flow {

  /**
   * Creates a new (anonymous) Flow.
   *
   * @return a new flow with an undefined name,
   *          i.e. either not named at all or with a system generated name
   */
  public static Flow create() {
    return create(null);
  }

  /**
   * Creates a new Flow.
   *
   * @param flowName a symbolic name of the flow; can be {@code null}
   *
   * @return a newly created flow
   */
  public static Flow create(@Nullable String flowName) {
    return new Flow(flowName, new Settings());
  }

  /**
   * Creates a new Flow.
   *
   * @param flowName a symbolic name of the flow; can be {@code null}
   * @param settings euphoria settings to be associated with the new flow
   *
   * @return a newly created flow
   */
  public static Flow create(String flowName, Settings settings) {
    return new Flow(flowName, settings);
  }

  protected Flow(@Nullable String name, Settings settings) {
    super(name, settings);
  }

  /**
   * Convert this flow to Beam's {@code Pipeline}.
   */
  public Pipeline toPipeline() {
    return toPipeline(PipelineOptionsFactory.create());
  }

  /**
   * Convert this flow to Beam's {@code Pipeline}.
   */
  public Pipeline toPipeline(PipelineOptions options) {
    return PipelineBuilder.toPipeline(this, options);
  }
}
