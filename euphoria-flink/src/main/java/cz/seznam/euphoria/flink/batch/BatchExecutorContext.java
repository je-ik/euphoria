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

import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.flink.ExecutorContext;
import cz.seznam.euphoria.flink.FlinkOperator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class BatchExecutorContext
    extends ExecutorContext<ExecutionEnvironment, DataSet<?>>
{
  public BatchExecutorContext(ExecutionEnvironment env, DAG<FlinkOperator<?>> dag) {
    super(env, dag);
  }
}
