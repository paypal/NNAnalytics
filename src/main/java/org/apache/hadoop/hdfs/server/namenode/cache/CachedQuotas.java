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
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.ContentSummary;
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

    QueryEngine queryEngine = loader.getQueryEngine();
    Stream<INode> quotaDirsStream =
        queryEngine.combinedFilterToStream(
            dirs, new String[] {"hasQuota"}, new String[] {"eq:true"});
    Map<String, List<String>> ownerAndDirs =
        quotaDirsStream.collect(
            Collectors.groupingBy(
                INode::getUserName,
                Collectors.mapping(INode::getFullPathName, Collectors.toList())));
    Map<String, ContentSummary> dirToContentSummary =
        ownerAndDirs
            .values()
            .parallelStream()
            .flatMap(List::stream)
            .collect(Collectors.toMap(Function.identity(), loader::getContentSummary));
    Map<String, Long> dirDsQuotaAssigned =
        dirToContentSummary
            .entrySet()
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Entry::getKey,
                    e -> {
                      ContentSummary summary = e.getValue();
                      if (summary.getSpaceQuota() <= -1L) {
                        return -1L;
                      } else {
                        return summary.getSpaceQuota();
                      }
                    }));
    Map<String, Long> dirDsQuotaUsed =
        dirToContentSummary
            .entrySet()
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Entry::getKey,
                    e -> {
                      ContentSummary summary = e.getValue();
                      return summary.getSpaceConsumed();
                    }));
    Map<String, Long> dirDsQuotaRatio =
        dirToContentSummary
            .entrySet()
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Entry::getKey,
                    e -> {
                      ContentSummary summary = e.getValue();
                      if (summary.getSpaceQuota() <= -1L) {
                        return -1L;
                      } else {
                        return (long)
                            (100 * ((double) summary.getSpaceConsumed()) / summary.getSpaceQuota());
                      }
                    }));
    Map<String, Long> dirNsQuotaAssigned =
        dirToContentSummary
            .entrySet()
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Entry::getKey,
                    e -> {
                      ContentSummary summary = e.getValue();
                      if (summary.getQuota() <= -1L) {
                        return -1L;
                      } else {
                        return summary.getQuota();
                      }
                    }));
    Map<String, Long> dirNsQuotaUsed =
        dirToContentSummary
            .entrySet()
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Entry::getKey,
                    e -> {
                      ContentSummary summary = e.getValue();
                      return (summary.getFileCount() + summary.getDirectoryCount());
                    }));
    Map<String, Long> dirNsQuotaRatio =
        dirToContentSummary
            .entrySet()
            .parallelStream()
            .collect(
                Collectors.toMap(
                    Entry::getKey,
                    e -> {
                      ContentSummary summary = e.getValue();
                      if (summary.getQuota() <= -1L) {
                        return -1L;
                      } else {
                        return (long)
                            (100
                                * (((double) (summary.getFileCount() + summary.getDirectoryCount()))
                                    / summary.getQuota()));
                      }
                    }));
    ownerAndDirs
        .entrySet()
        .parallelStream()
        .forEach(
            entry -> {
              String user = entry.getKey();
              Map<String, Long> nsQuotaRatio =
                  entry
                      .getValue()
                      .parallelStream()
                      .collect(Collectors.toMap(Function.identity(), dirNsQuotaRatio::get));
              Map<String, Long> nsQuotaAssigned =
                  entry
                      .getValue()
                      .parallelStream()
                      .collect(Collectors.toMap(Function.identity(), dirNsQuotaAssigned::get));
              Map<String, Long> nsQuotaUsed =
                  entry
                      .getValue()
                      .parallelStream()
                      .collect(Collectors.toMap(Function.identity(), dirNsQuotaUsed::get));
              Map<String, Long> dsQuotaRatio =
                  entry
                      .getValue()
                      .parallelStream()
                      .collect(Collectors.toMap(Function.identity(), dirDsQuotaRatio::get));
              Map<String, Long> dsQuotaAssigned =
                  entry
                      .getValue()
                      .parallelStream()
                      .collect(Collectors.toMap(Function.identity(), dirDsQuotaAssigned::get));
              Map<String, Long> dsQuotaUsed =
                  entry
                      .getValue()
                      .parallelStream()
                      .collect(Collectors.toMap(Function.identity(), dirDsQuotaUsed::get));
              final long nsThreshExceeded =
                  nsQuotaRatio.values().parallelStream().filter(v -> v > 85L).count();
              final long dsThreshExceeded =
                  dsQuotaRatio.values().parallelStream().filter(v -> v > 85L).count();
              cachedUserNsQuotaAssigned.put(user, nsQuotaAssigned);
              cachedUserNsQuotaUsed.put(user, nsQuotaUsed);
              cachedUserNsQuotaRatios.put(user, nsQuotaRatio);
              cachedUserDsQuotaAssigned.put(user, dsQuotaAssigned);
              cachedUserDsQuotaUsed.put(user, dsQuotaUsed);
              cachedUserDsQuotaRatios.put(user, dsQuotaRatio);
              nsQuotaThreshCountsUsers.put(user, nsThreshExceeded);
              dsQuotaThreshCountsUsers.put(user, dsThreshExceeded);
              nsQuotaCountsUsers.put(user, (long) nsQuotaRatio.size());
              dsQuotaCountsUsers.put(user, (long) dsQuotaRatio.size());
            });
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
