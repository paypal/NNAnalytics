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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdfs.server.namenode.AirConcurrentMapINodeCollection;
import org.apache.hadoop.hdfs.server.namenode.ConcurrentHashMapINodeCollection;
import org.apache.hadoop.hdfs.server.namenode.EclipseINodeCollection;
import org.apache.hadoop.hdfs.server.namenode.GSetGenerator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.NonBlockingHashMapINodeCollection;
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
@OutputTimeUnit(TimeUnit.MILLISECONDS)
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
    gset = gSetGenerator.getGSet((short) 3, 20, 500);
  }

  @Benchmark
  public void benchmarkConcurrentJavaFiltering() {
    ConcurrentHashMapINodeCollection concurrent = new ConcurrentHashMapINodeCollection();
    Map<INode, INodeWithAdditionalFields> files = concurrent.filterFiles(gset);
    Map<INode, INodeWithAdditionalFields> dirs = concurrent.filterDirs(gset);
  }

  @Benchmark
  public void benchmarkNonBlockingHashMapFiltering() {
    NonBlockingHashMapINodeCollection concurrent = new NonBlockingHashMapINodeCollection();
    Map<INode, INodeWithAdditionalFields> files = concurrent.filterFiles(gset);
    Map<INode, INodeWithAdditionalFields> dirs = concurrent.filterDirs(gset);
  }

  @Benchmark
  public void benchmarkEclipseConcurrentFiltering() {
    EclipseINodeCollection concurrent = new EclipseINodeCollection();
    Map<INode, INodeWithAdditionalFields> files = concurrent.filterFiles(gset);
    Map<INode, INodeWithAdditionalFields> dirs = concurrent.filterDirs(gset);
  }

  @Benchmark
  public void benchmarkAirConcurrentMapFiltering() {
    AirConcurrentMapINodeCollection concurrent = new AirConcurrentMapINodeCollection();
    Map<INode, INodeWithAdditionalFields> files = concurrent.filterFiles(gset);
    Map<INode, INodeWithAdditionalFields> dirs = concurrent.filterDirs(gset);
  }
}
