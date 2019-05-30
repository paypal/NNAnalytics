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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLoader;
import org.apache.hadoop.hdfs.server.namenode.QueryEngine;
import org.apache.hadoop.util.VirtualINodeTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedDirectories {

  public static final Logger LOG = LoggerFactory.getLogger(SuggestionsEngine.class.getName());

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

  /**
   * Call to perform analysis across set of cached dirs.
   *
   * @param nnLoader the namenodeLoader of the NNA instance
   * @param countMap map of dir -> file count to add to
   * @param diskspaceMap map of dir -> diskspace to add to
   */
  public void analyze(
      NameNodeLoader nnLoader, Map<String, Long> countMap, Map<String, Long> diskspaceMap) {
    long start = System.currentTimeMillis();

    /* Make an in-mem copy of the cachedDirs so we can parallelize the stream. */
    HashSet<String> inMemCachedDirsCopy = new HashSet<>(cachedDirs);
    Map<String, ContentSummary> contentSummaries =
        inMemCachedDirsCopy
            .parallelStream()
            .collect(Collectors.toMap(Function.identity(), nnLoader::getContentSummary));
    for (Entry<String, ContentSummary> entry : contentSummaries.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        continue;
      }
      countMap.put(entry.getKey(), entry.getValue().getFileCount());
      diskspaceMap.put(entry.getKey(), entry.getValue().getSpaceConsumed());
    }

    long end = System.currentTimeMillis();
    LOG.info(
        "Performed cached directory analysis using getContentSummary calls in: "
            + (end - start)
            + "ms.");
  }

  /**
   * Call to perform analysis across set of cached dirs.
   *
   * @param queryEngine queryEngine to use
   * @param inodeSet the inode set
   * @param countMap map of dir -> file count to add to
   * @param diskspaceMap map of dir -> diskspace to add to
   */
  public void analyze(
      QueryEngine queryEngine,
      Collection<INode> inodeSet,
      Map<String, Long> countMap,
      Map<String, Long> diskspaceMap) {
    long start = System.currentTimeMillis();

    VirtualINodeTree tree = new VirtualINodeTree();
    cachedDirs.forEach(tree::addElement);
    List<String> commonRoots = tree.getCommonAncestorsAsStrings();

    for (String commonRoot : commonRoots) {
      Collection<INode> commonINodes =
          queryEngine.combinedFilter(
              inodeSet, new String[] {"path"}, new String[] {"startsWith:" + commonRoot});

      for (String cachedDir : cachedDirs) {
        if (!cachedDir.startsWith(commonRoot)) {
          continue;
        }
        Collection<INode> inodes;
        if (cachedDir.equals(commonRoot)) {
          inodes = commonINodes;
        } else {
          inodes =
              queryEngine.combinedFilter(
                  commonINodes, new String[] {"path"}, new String[] {"startsWith:" + cachedDir});
        }
        long count = inodes.size();
        long diskspaceConsumed = queryEngine.sum(inodes, "diskspaceConsumed");
        countMap.put(cachedDir, count);
        diskspaceMap.put(cachedDir, diskspaceConsumed);
      }
    }

    long end = System.currentTimeMillis();
    LOG.info(
        "Performed cached directory analysis using VirtualINodeTree in: " + (end - start) + "ms.");
  }
}
