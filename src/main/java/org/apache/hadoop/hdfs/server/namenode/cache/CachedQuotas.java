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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLoader;
import org.apache.hadoop.hdfs.server.namenode.QueryEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedQuotas {

  public static final Logger LOG = LoggerFactory.getLogger(SuggestionsEngine.class.getName());

  private Map<String, Map<String, Long>> cachedUserNsQuotaAssigned;
  private Map<String, Map<String, Long>> cachedUserDsQuotaAssigned;
  private Map<String, Map<String, Long>> cachedUserNsQuotaUsed;
  private Map<String, Map<String, Long>> cachedUserDsQuotaUsed;
  private Map<String, Map<String, Long>> cachedUserDsQuotaRatios;
  private Map<String, Map<String, Long>> cachedUserNsQuotaRatios;

  /**
   * Initialize.
   *
   * @param cacheManager the cache manager
   */
  public void start(CacheManager cacheManager) {
    this.cachedUserNsQuotaRatios =
        Collections.synchronizedMap(cacheManager.getCachedMapToMap("cachedUserNsQuotaRatios"));
    this.cachedUserDsQuotaRatios =
        Collections.synchronizedMap(cacheManager.getCachedMapToMap("cachedUserDsQuotaRatios"));
    this.cachedUserNsQuotaAssigned =
        Collections.synchronizedMap(cacheManager.getCachedMapToMap("cachedUserNsQuotaAssigned"));
    this.cachedUserDsQuotaAssigned =
        Collections.synchronizedMap(cacheManager.getCachedMapToMap("cachedUserDsQuotaAssigned"));
    this.cachedUserNsQuotaUsed =
        Collections.synchronizedMap(cacheManager.getCachedMapToMap("cachedUserNsQuotaUsed"));
    this.cachedUserDsQuotaUsed =
        Collections.synchronizedMap(cacheManager.getCachedMapToMap("cachedUserDsQuotaUsed"));
  }

  /**
   * Main analysis call for compute all quotas and quota usage information.
   *
   * @param loader the nn loader
   * @param dirs set of inode directories
   * @param dsQuotaCountsUsers map of user -> # of ds quotas they have
   * @param nsQuotaCountsUsers map of user -> # of ns quotas they have
   * @param dsQuotaThreshCountsUsers map of user -> # of high ds usage dirs they have
   * @param nsQuotaThreshCountsUsers map of user -> # of high ns usage dirs they have
   */
  public void analyze(
      NameNodeLoader loader,
      Collection<INode> dirs,
      Map<String, Long> dsQuotaCountsUsers,
      Map<String, Long> nsQuotaCountsUsers,
      Map<String, Long> dsQuotaThreshCountsUsers,
      Map<String, Long> nsQuotaThreshCountsUsers) {
    /* If there is no namesystem (static testing), then exit. */
    if (loader.getCurrentTxId() == -1L) {
      return;
    }

    final QueryEngine queryEngine = loader.getQueryEngine();
    final Map<String, Map<String, INode>> ownerDirContentSummary =
        queryEngine
            .combinedFilterToStream(dirs, new String[] {"hasQuota"}, new String[] {"eq:true"})
            .collect(
                Collectors.groupingBy(
                    INode::getUserName,
                    Collectors.toMap(INode::getFullPathName, Function.identity())));
    cachedUserNsQuotaAssigned.clear();
    cachedUserNsQuotaUsed.clear();
    cachedUserNsQuotaRatios.clear();
    cachedUserDsQuotaAssigned.clear();
    cachedUserDsQuotaUsed.clear();
    cachedUserDsQuotaRatios.clear();
    for (Entry<String, Map<String, INode>> entry : ownerDirContentSummary.entrySet()) {
      long nsQuotaRatio85 = 0;
      long dsQuotaRatio85 = 0;
      long nsQuotaCount = 0;
      long dsQuotaCount = 0;
      Map<String, Long> nsQuotaAssigned = new HashMap<>();
      Map<String, Long> nsQuotaUsed = new HashMap<>();
      Map<String, Long> nsQuotaRatio = new HashMap<>();
      Map<String, Long> dsQuotaAssigned = new HashMap<>();
      Map<String, Long> dsQuotaUsed = new HashMap<>();
      Map<String, Long> dsQuotaRatio = new HashMap<>();
      String user = entry.getKey();
      for (Entry<String, INode> innerEntry : entry.getValue().entrySet()) {
        INode inode = innerEntry.getValue();
        Long nsQuotaRatioLong =
            queryEngine.getSumFunctionForINode("nsQuotaRatioUsed", null).apply(inode);
        if (nsQuotaRatioLong >= 0) {
          nsQuotaCount++;
          if (nsQuotaRatioLong >= 85) {
            nsQuotaRatio85++;
          }
        } else {
          nsQuotaRatioLong = -1L;
        }
        Long nsQuotaAssignedLong = queryEngine.getSumFunctionForINode("nsQuota", null).apply(inode);
        if (nsQuotaAssignedLong <= -1L) {
          nsQuotaAssignedLong = -1L;
        }
        Long nsQuotaUsedLong = queryEngine.getSumFunctionForINode("nsQuotaUsed", null).apply(inode);
        if (nsQuotaUsedLong <= -1L) {
          nsQuotaUsedLong = -1L;
        }
        Long dsQuotaRatioLong =
            queryEngine.getSumFunctionForINode("dsQuotaRatioUsed", null).apply(inode);
        if (dsQuotaRatioLong >= 0) {
          dsQuotaCount++;
          if (dsQuotaRatioLong >= 85) {
            dsQuotaRatio85++;
          }
        } else {
          dsQuotaRatioLong = -1L;
        }
        Long dsQuotaAssignedLong = queryEngine.getSumFunctionForINode("dsQuota", null).apply(inode);
        if (dsQuotaAssignedLong <= -1L) {
          dsQuotaAssignedLong = -1L;
        }
        Long dsQuotaUsedLong = queryEngine.getSumFunctionForINode("dsQuotaUsed", null).apply(inode);
        if (dsQuotaUsedLong <= -1L) {
          dsQuotaUsedLong = -1L;
        }
        String path = innerEntry.getKey();
        nsQuotaAssigned.put(path, nsQuotaAssignedLong);
        dsQuotaAssigned.put(path, dsQuotaAssignedLong);
        nsQuotaUsed.put(path, nsQuotaUsedLong);
        dsQuotaUsed.put(path, dsQuotaUsedLong);
        nsQuotaRatio.put(path, nsQuotaRatioLong);
        dsQuotaRatio.put(path, dsQuotaRatioLong);
      }
      cachedUserNsQuotaAssigned.put(user, nsQuotaAssigned);
      cachedUserNsQuotaUsed.put(user, nsQuotaUsed);
      cachedUserNsQuotaRatios.put(user, nsQuotaRatio);
      cachedUserDsQuotaAssigned.put(user, dsQuotaAssigned);
      cachedUserDsQuotaUsed.put(user, dsQuotaUsed);
      cachedUserDsQuotaRatios.put(user, dsQuotaRatio);
      nsQuotaThreshCountsUsers.put(user, nsQuotaRatio85);
      dsQuotaThreshCountsUsers.put(user, dsQuotaRatio85);
      nsQuotaCountsUsers.put(user, nsQuotaCount);
      dsQuotaCountsUsers.put(user, dsQuotaCount);
    }
  }

  public Map<String, Long> getDiskQuotaRatio(String user) {
    return cachedUserDsQuotaRatios.getOrDefault(user, Collections.emptyMap());
  }

  public Map<String, Long> getNameQuotaRatio(String user) {
    return cachedUserNsQuotaRatios.getOrDefault(user, Collections.emptyMap());
  }

  public Map<String, Long> getDiskQuotaAssigned(String user) {
    return cachedUserDsQuotaAssigned.getOrDefault(user, Collections.emptyMap());
  }

  public Map<String, Long> getNameQuotaAssigned(String user) {
    return cachedUserNsQuotaAssigned.getOrDefault(user, Collections.emptyMap());
  }

  public Map<String, Long> getDiskQuotaUsed(String user) {
    return cachedUserDsQuotaUsed.getOrDefault(user, Collections.emptyMap());
  }

  public Map<String, Long> getNameQuotaUsed(String user) {
    return cachedUserNsQuotaUsed.getOrDefault(user, Collections.emptyMap());
  }

  public Map<String, Map<String, Long>> getAllDsQuotaRatio() {
    return cachedUserDsQuotaRatios;
  }

  public Map<String, Map<String, Long>> getAllNsQuotaRatio() {
    return cachedUserNsQuotaRatios;
  }

  public Map<String, Map<String, Long>> getAllDsQuotaAssigned() {
    return cachedUserDsQuotaAssigned;
  }

  public Map<String, Map<String, Long>> getAllNsQuotaAssigned() {
    return cachedUserNsQuotaAssigned;
  }

  public Map<String, Map<String, Long>> getAllDsQuotaUsed() {
    return cachedUserDsQuotaUsed;
  }

  public Map<String, Map<String, Long>> getAllNsQuotaUsed() {
    return cachedUserNsQuotaUsed;
  }
}
