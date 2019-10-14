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
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLoader;
import org.apache.hadoop.hdfs.server.namenode.QueryEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedFileTypes {

  public static final Logger LOG = LoggerFactory.getLogger(SuggestionsEngine.class.getName());

  private Map<String, Map<String, Long>> cachedUserFileTypeCount;
  private Map<String, Map<String, Long>> cachedUserFileTypeDiskspace;

  /**
   * Initialize.
   *
   * @param cacheManager the cache manager
   */
  public void start(CacheManager cacheManager) {
    this.cachedUserFileTypeCount =
        Collections.synchronizedMap(cacheManager.getCachedMapToMap("cachedUserFileTypeCount"));
    this.cachedUserFileTypeDiskspace =
        Collections.synchronizedMap(cacheManager.getCachedMapToMap("cachedUserFileTypeDiskspace"));
  }

  /**
   * Main analysis call for compute all quotas and quota usage information.
   *
   * @param loader the nn loader
   * @param files set of inode files
   */
  public void analyze(NameNodeLoader loader, Collection<INode> files) {
    /* If there is no namesystem (static testing), then exit. */
    if (loader.getCurrentTxId() == -1L) {
      return;
    }

    final QueryEngine queryEngine = loader.getQueryEngine();
    final Map<String, Map<String, Long>> fileTypeDiskspaceUsers =
        queryEngine.genericTwoLevelHistogram(
            files.parallelStream(),
            INode::getUserName,
            queryEngine.getGroupingFunctionToStringForINode("fileType", null, null),
            queryEngine.getSumFunctionForINode("diskspaceConsumed", null));
    final Map<String, Map<String, Long>> fileTypeCountUsers =
        queryEngine.genericTwoLevelHistogram(
            files.parallelStream(),
            INode::getUserName,
            queryEngine.getGroupingFunctionToStringForINode("fileType", null, null),
            queryEngine.getSumFunctionForINode("count", null));
    cachedUserFileTypeCount.clear();
    cachedUserFileTypeDiskspace.clear();
    for (Entry<String, Map<String, Long>> entry : fileTypeCountUsers.entrySet()) {
      cachedUserFileTypeCount.put(entry.getKey(), entry.getValue());
    }
    for (Entry<String, Map<String, Long>> entry : fileTypeDiskspaceUsers.entrySet()) {
      cachedUserFileTypeDiskspace.put(entry.getKey(), entry.getValue());
    }
  }

  public Map<String, Long> getFileTypeCount(String user) {
    return cachedUserFileTypeCount.getOrDefault(user, Collections.emptyMap());
  }

  public Map<String, Long> getFileTypeDiskspace(String user) {
    return cachedUserFileTypeDiskspace.getOrDefault(user, Collections.emptyMap());
  }

  public Map<String, Map<String, Long>> getAllFileTypeCount() {
    return cachedUserFileTypeCount;
  }

  public Map<String, Map<String, Long>> getAllFileTypeDiskspace() {
    return cachedUserFileTypeDiskspace;
  }
}
