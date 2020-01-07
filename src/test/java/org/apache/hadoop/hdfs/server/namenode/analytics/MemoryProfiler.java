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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

/**
 * An {@link InternalProfiler} to measure the memory allocated per benchmark iteration.
 *
 * @author Eric Bruneton
 */
public class MemoryProfiler implements InternalProfiler {

  private static Object[] references;
  private static int referenceCount;
  private static long usedMemoryBeforeIteration;

  public static void keepReference(final Object reference) {
    references[referenceCount++] = reference;
  }

  @Override
  public String getDescription() {
    return "Adds used memory to the result.";
  }

  @Override
  public void beforeIteration(
      final BenchmarkParams benchmarkParams, final IterationParams iterationParams) {
    references = new Object[100000];
    referenceCount = 0;
    usedMemoryBeforeIteration = getUsedMemory();
  }

  @Override
  public Collection<? extends Result> afterIteration(
      final BenchmarkParams benchmarkParams,
      final IterationParams iterationParams,
      final IterationResult result) {
    long usedMemoryAfterIteration = getUsedMemory();
    references = null;

    long usedMemoryInIteration = usedMemoryAfterIteration - usedMemoryBeforeIteration;
    double usedMemoryPerOp =
        ((double) usedMemoryInIteration) / result.getMetadata().getMeasuredOps();
    List<Result> results = new ArrayList<Result>();
    results.add(new ScalarResult("+memory.used", usedMemoryPerOp, "bytes", AggregationPolicy.AVG));
    return results;
  }

  /**
   * Triggers a gc, waits for completion and returns the used memory. Inspired from cache2k
   * ForcedGcMemoryProfiler, itself inspired from the JMH approach.
   */
  private static long getUsedMemory() {
    final int MAX_WAIT_MSEC = 20 * 1000;
    List<GarbageCollectorMXBean> gcBeans = new ArrayList<GarbageCollectorMXBean>();
    for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
      long count = gcBean.getCollectionCount();
      if (count != -1) {
        gcBeans.add(gcBean);
      }
    }
    if (gcBeans.isEmpty()) {
      System.err.println("WARNING: MXBeans can not report GC info. Cannot get memory used.");
      return -1;
    }

    long startGcCount = countGc(gcBeans);
    long startTimeMillis = System.currentTimeMillis();
    System.gc();
    while (System.currentTimeMillis() - startTimeMillis < MAX_WAIT_MSEC) {
      try {
        Thread.sleep(234);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (countGc(gcBeans) > startGcCount) {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
      }
    }
    System.err.println("WARNING: System.gc() was invoked but couldn't detect a GC occurring.");
    return -1;
  }

  private static long countGc(final List<GarbageCollectorMXBean> gcBeans) {
    long gcCount = 0;
    for (GarbageCollectorMXBean bean : gcBeans) {
      gcCount += bean.getCollectionCount();
    }
    return gcCount;
  }
}
