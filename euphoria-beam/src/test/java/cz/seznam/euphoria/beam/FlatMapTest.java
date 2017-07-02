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

import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;


/**
 * Test {@code FlatMap} operator's integration with beam.
 */
public class FlatMapTest {
  
  Flow flow;

  @Before
  public void setUp() {
    flow = Flow.create();
  }

  @Test
  public void testSimpleMap() {
    ListDataSource<Integer> input = ListDataSource.bounded(
        Arrays.asList(1, 2, 3),
        Arrays.asList(2, 3, 4));
    ListDataSink<Integer> output = ListDataSink.get(2);

    MapElements.of(flow.createInput(input))
        .using(i -> i + 1)
        .output()
        .persist(output);

    Pipeline pipeline = flow.toPipeline();
    PipelineResult result = pipeline.run();
    PipelineResult.State state = result.waitUntilFinish();
    assertEquals(PipelineResult.State.DONE, state);
    assertEquals(Arrays.asList(2, 3, 4), output.getOutput(0));
    assertEquals(Arrays.asList(3, 4, 5), output.getOutput(1));
  }

}
