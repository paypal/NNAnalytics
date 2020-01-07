/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hdfs.server.namenode.analytics;

import gnu.trove.map.hash.THashMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hdfs.server.namenode.GSetGenerator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.util.CollectionsView;
import org.apache.hadoop.util.GSet;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkGSetFiltering {

  private GSet<INode, INodeWithAdditionalFields> gset;

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(".*" + BenchmarkGSetFiltering.class.getSimpleName() + ".*")
            .warmupIterations(5)
            .measurementIterations(5)
            .addProfiler(MemoryProfiler.class)
            .addProfiler(GCProfiler.class)
            .forks(1)
            .build();
    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void doSetup() {
    GSetGenerator gSetGenerator = new GSetGenerator();
    gset = gSetGenerator.getGSet((short) 3, 15, 500);
  }

  @Benchmark
  public void benchmarkJavaFiltering() {
    Map<INode, INodeWithAdditionalFields> files =
        StreamSupport.stream(gset.spliterator(), true)
            .filter(INode::isFile)
            .collect(Collectors.toMap(node -> node, node -> node, throwingMerger(), HashMap::new));
    Map<INode, INodeWithAdditionalFields> dirs =
        StreamSupport.stream(gset.spliterator(), true)
            .filter(INode::isDirectory)
            .collect(Collectors.toMap(node -> node, node -> node, throwingMerger(), HashMap::new));
    Collection<INode> all = CollectionsView.combine(files.keySet(), dirs.keySet());
    MemoryProfiler.keepReference(files);
    MemoryProfiler.keepReference(dirs);
    MemoryProfiler.keepReference(all);
  }

  @Benchmark
  public void benchmarkConcurrentJavaFiltering() {
    Map<INode, INodeWithAdditionalFields> files =
        StreamSupport.stream(gset.spliterator(), true)
            .filter(INode::isFile)
            .collect(
                Collectors.toMap(
                    node -> node, node -> node, throwingMerger(), ConcurrentHashMap::new));
    Map<INode, INodeWithAdditionalFields> dirs =
        StreamSupport.stream(gset.spliterator(), true)
            .filter(INode::isDirectory)
            .collect(
                Collectors.toMap(
                    node -> node, node -> node, throwingMerger(), ConcurrentHashMap::new));
    Collection<INode> all = CollectionsView.combine(files.keySet(), dirs.keySet());
    MemoryProfiler.keepReference(files);
    MemoryProfiler.keepReference(dirs);
    MemoryProfiler.keepReference(all);
  }

  @Benchmark
  public void benchmarkTroveFiltering() {
    Map<INode, INodeWithAdditionalFields> files =
        StreamSupport.stream(gset.spliterator(), true)
            .filter(INode::isFile)
            .collect(Collectors.toMap(node -> node, node -> node, throwingMerger(), THashMap::new));
    Map<INode, INodeWithAdditionalFields> dirs =
        StreamSupport.stream(gset.spliterator(), true)
            .filter(INode::isDirectory)
            .collect(Collectors.toMap(node -> node, node -> node, throwingMerger(), THashMap::new));
    Collection<INode> all = CollectionsView.combine(files.keySet(), dirs.keySet());
    MemoryProfiler.keepReference(files);
    MemoryProfiler.keepReference(dirs);
    MemoryProfiler.keepReference(all);
  }

  @Benchmark
  public void benchmarkGoldmanSachsFiltering() {
    Map<INode, INodeWithAdditionalFields> files =
        StreamSupport.stream(gset.spliterator(), true)
            .filter(INode::isFile)
            .collect(
                Collectors.toMap(
                    node -> node,
                    node -> node,
                    throwingMerger(),
                    org.eclipse.collections.impl.map.mutable.ConcurrentHashMap::new));
    Map<INode, INodeWithAdditionalFields> dirs =
        StreamSupport.stream(gset.spliterator(), true)
            .filter(INode::isDirectory)
            .collect(
                Collectors.toMap(
                    node -> node,
                    node -> node,
                    throwingMerger(),
                    org.eclipse.collections.impl.map.mutable.ConcurrentHashMap::new));
    Collection<INode> all = CollectionsView.combine(files.keySet(), dirs.keySet());
    MemoryProfiler.keepReference(files);
    MemoryProfiler.keepReference(dirs);
    MemoryProfiler.keepReference(all);
  }

  private static <T> BinaryOperator<T> throwingMerger() {
    return (u, v) -> {
      throw new IllegalStateException(String.format("Duplicate key %s", u));
    };
  }
}
