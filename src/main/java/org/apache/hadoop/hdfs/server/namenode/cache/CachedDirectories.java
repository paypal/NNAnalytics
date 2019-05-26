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

package org.apache.hadoop.hdfs.server.namenode.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.AbstractQueryEngine;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.QueryEngine;
import org.apache.hadoop.util.VirtualINode;
import org.apache.hadoop.util.VirtualINodeTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedDirectories {

  public static final Logger LOG = LoggerFactory.getLogger(SuggestionsEngine.class.getName());

  /* Based on testing, it was seen that a parentDir histogram is equal to about 5 filters.
   * So we can say if there are 5+ children under the ancestor, then we should do a histogram. */
  private static final int HISTOGRAM_TRIGGER_SIZE = 5;
  private Set<String> cachedDirs;

  public void start(CacheManager cacheManager) {
    this.cachedDirs = Collections.synchronizedSet(cacheManager.getCachedSet("cachedDirs"));
  }

  public boolean add(String directory) {
    return cachedDirs.add(directory);
  }

  public boolean remove(String directory) {
    return cachedDirs.remove(directory);
  }

  public Set<String> getCachedDirSet() {
    return cachedDirs;
  }

  public void analyze(
      QueryEngine queryEngine,
      Collection<INode> files,
      Map<String, Long> countMap,
      Map<String, Long> diskspaceMap) {
    VirtualINodeTree tree = new VirtualINodeTree();
    cachedDirs.forEach(tree::addElement);
    Set<VirtualINode> ancestors = tree.getCommonAncestors();

    for (VirtualINode ancestor : ancestors) {
      String commonRoot = ancestor.path();
      if (ancestor.isMarked()) {
        continue;
      }
      if (shouldPerformHistogram(ancestor)) {
        List<VirtualINode> childrenToRetain = ancestor.getChildren();
        Set<String> keysToRetain =
            childrenToRetain.stream().map(VirtualINode::path).collect(Collectors.toSet());
        Stream<INode> inodeStream =
            queryEngine.combinedFilterToStream(
                files, new String[] {"path"}, new String[] {"startsWith:" + commonRoot});
        Map<String, LongSummaryStatistics> countAndDisk =
            queryEngine.genericSummarizingHistogram(
                inodeStream,
                AbstractQueryEngine.getDirectoryAtDepthFunction(new Path(commonRoot).depth() + 1),
                queryEngine.getSumFunctionForINode("diskspaceConsumed"));
        countAndDisk.remove("NO_MAPPING");
        long countSum =
            countAndDisk.values().parallelStream().mapToLong(LongSummaryStatistics::getCount).sum();
        long diskspaceSum =
            countAndDisk.values().parallelStream().mapToLong(LongSummaryStatistics::getSum).sum();
        countAndDisk.entrySet().removeIf(entry -> !keysToRetain.contains(entry.getKey()));
        for (Entry<String, LongSummaryStatistics> entry : countAndDisk.entrySet()) {
          countMap.put(entry.getKey(), entry.getValue().getCount());
          diskspaceMap.put(entry.getKey(), entry.getValue().getSum());
        }
        childrenToRetain.forEach(VirtualINode::mark);
        if (cachedDirs.contains(commonRoot)) {
          countMap.put(commonRoot, countSum);
          diskspaceMap.put(commonRoot, diskspaceSum);
          ancestor.mark();
        }
        continue;
      }
      Collection<INode> commonINodes =
          queryEngine.combinedFilter(
              files, new String[] {"path"}, new String[] {"startsWith:" + commonRoot});
      for (String cachedDir : cachedDirs) {
        if (!cachedDir.startsWith(commonRoot)) {
          continue;
        }
        Collection<INode> inodes;
        if (cachedDir.equals(commonRoot)) {
          inodes = commonINodes;
          long count = inodes.size();
          long diskspaceConsumed = queryEngine.sum(inodes, "diskspaceConsumed");
          countMap.put(cachedDir, count);
          diskspaceMap.put(cachedDir, diskspaceConsumed);
        } else {
          inodes =
              queryEngine.combinedFilter(
                  commonINodes, new String[] {"path"}, new String[] {"startsWith:" + cachedDir});
          long count = inodes.size();
          long diskspaceConsumed = queryEngine.sum(inodes, "diskspaceConsumed");
          countMap.put(cachedDir, count);
          diskspaceMap.put(cachedDir, diskspaceConsumed);
        }
      }
      ancestor.mark();
    }
  }

  /* If every child of the ancestor is itself a cachedDir, (ancestor may also be a cachedDir),
   * and there are more than HISTOGRAM_TRIGGER_SIZE of them, then we can optimize further. */
  private boolean shouldPerformHistogram(VirtualINode ancestor) {
    List<VirtualINode> children = ancestor.getChildren();
    if (ancestor.getChildren().size() >= HISTOGRAM_TRIGGER_SIZE) {
      for (VirtualINode child : children) {
        if (!cachedDirs.contains(child.path()) || child.isMarked()) {
          return false;
        }
      }
      return true;
    } else {
      return false;
    }
  }
}
