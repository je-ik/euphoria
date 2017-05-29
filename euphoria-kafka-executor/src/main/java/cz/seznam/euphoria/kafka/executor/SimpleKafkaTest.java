/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.kafka.executor;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.core.client.io.StdoutSink;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Sums;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.kafka.executor.io.Serde;
import cz.seznam.euphoria.kafka.executor.io.TopicSpec;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Test real {@code KafkaExecutor} application.
 */
public class SimpleKafkaTest {

  public static class SocketSource implements DataSource<String> {

    final String host;
    final int port;
    SocketSource(String host, int port) {
      this.host = host;
      this.port = port;
    }

    @Override
    public List<Partition<String>> getPartitions() {
      return Arrays.asList(
          new Partition<String>() {
            @Override
            public Set<String> getLocations() {
              return Collections.singleton("localhost");
            }

            @Override
            public Reader<String> openReader() throws IOException {
              Socket socket = new Socket(host, port);
              InputStream input = socket.getInputStream();
              BufferedReader reader = new BufferedReader(new InputStreamReader(input));
              return new Reader<String>() {

                String line;

                @Override
                public void close() throws IOException {
                  reader.close();
                }

                @Override
                public boolean hasNext() {
                  try {
                    return (line = reader.readLine()) != null;
                  } catch (IOException ex) {
                    return false;
                  }
                }

                @Override
                public String next() {
                  return line;
                }

              };
            }
          }
      );
    }

    @Override
    public boolean isBounded() {
      return false;
    }

  }

  public static void main(String[] args) throws Exception {
    Executor executor = new KafkaExecutor(
        new ScheduledThreadPoolExecutor(100),
        new String[] { "localhost:9092" },
        SimpleKafkaTest::topicSpec);

    Flow flow = Flow.create("test-flow");
    SocketSource source = new SocketSource("localhost", 8000);
    Dataset<String> input = flow.createInput(source);
    Dataset<Pair<String, Long>> mapped = FlatMap
        .named("read")
        .of(input)
        .using((String in, Context<Pair<String, Long>> ctx) -> {
          String[] split = in.split(" ");
          for (String s : split) {
            if (!s.isEmpty()) {
              ctx.collect(Pair.of(s, 1L));
            }
          }
        })
        .output();
    
    Dataset<Pair<String, Long>> counted = ReduceByKey
        .named("count")
        .of(mapped)
        .keyBy(Pair::getFirst)
        .valueBy(Pair::getSecond)
        .combineBy(Sums.ofLongs())
        .windowBy(Time.of(Duration.ofSeconds(10)))
        .setNumPartitions(2)
        .output();

    counted.persist(new StdoutSink<>());
    
    executor.submit(flow).get();
    executor.shutdown();
  }

  static TopicSpec topicSpec(Flow flow, Dataset<?> dataset) {
    return TopicSpec.of(
            "euphoria",
            Serde.from(
                window -> new byte[0],
                bytes -> GlobalWindowing.Window.get()),
            Serde.from(
                (Pair<String, Long> p) -> (p.getFirst() + "#" + String.valueOf(p.getSecond())).getBytes(),
                bytes -> {
                  String[] parts = new String(bytes).split("#");
                  return Pair.of(parts[0], Long.valueOf(parts[1]));
                }));
  }

}
