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
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.QueryEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedQuotas {

  public static final Logger LOG = LoggerFactory.getLogger(SuggestionsEngine.class.getName());

  private Map<String, Map<String, Long>> cachedUserNsQuotas;
  private Map<String, Map<String, Long>> cachedUserDsQuotas;

  /**
   * Initialize.
   *
   * @param cacheManager the cache manager
   */
  public void start(CacheManager cacheManager) {
    this.cachedUserNsQuotas =
        Collections.synchronizedMap(cacheManager.getCachedMapToMap("cachedUserNsQuotas"));
    this.cachedUserDsQuotas =
        Collections.synchronizedMap(cacheManager.getCachedMapToMap("cachedUserDsQuotas"));
  }

  /**
   * Main analysis call for compute all quotas and quota usage information.
   *
   * @param queryEngine the query engine
   * @param dirs set of inode directories
   * @param users the set of users to iterate for
   * @param dsQuotaCountsUsers map of user -> # of ds quotas they have
   * @param nsQuotaCountsUsers map of user -> # of ns quotas they have
   * @param dsQuotaThreshCountsUsers map of user -> # of high ds usage dirs they have
   * @param nsQuotaThreshCountsUsers map of user -> # of high ns usage dirs they have
   */
  public void analyze(
      QueryEngine queryEngine,
      Collection<INode> dirs,
      Set<String> users,
      Map<String, Long> dsQuotaCountsUsers,
      Map<String, Long> nsQuotaCountsUsers,
      Map<String, Long> dsQuotaThreshCountsUsers,
      Map<String, Long> nsQuotaThreshCountsUsers) {
    for (String user : users) {
      Stream<INode> quotaDirs1 =
          queryEngine.combinedFilterToStream(
              dirs, new String[] {"user", "hasQuota"}, new String[] {"eq:" + user, "eq:true"});
      Map<String, Long> nsQuotaRatio =
          queryEngine.dirQuotaHistogram(quotaDirs1, "nsQuotaRatioUsed");
      Stream<INode> quotaDirs2 =
          queryEngine.combinedFilterToStream(
              dirs, new String[] {"user", "hasQuota"}, new String[] {"eq:" + user, "eq:true"});
      Map<String, Long> dsQuotaRatio =
          queryEngine.dirQuotaHistogram(quotaDirs2, "dsQuotaRatioUsed");
      final long nsThreshExceeded =
          nsQuotaRatio.values().parallelStream().filter(v -> v > 85L).count();
      final long dsThreshExceeded =
          dsQuotaRatio.values().parallelStream().filter(v -> v > 85L).count();
      cachedUserNsQuotas.put(user, nsQuotaRatio);
      cachedUserDsQuotas.put(user, dsQuotaRatio);
      nsQuotaThreshCountsUsers.put(user, nsThreshExceeded);
      dsQuotaThreshCountsUsers.put(user, dsThreshExceeded);
      nsQuotaCountsUsers.put(user, (long) nsQuotaRatio.size());
      dsQuotaCountsUsers.put(user, (long) dsQuotaRatio.size());
    }
  }

  public Map<String, Long> getDiskQuotaUsed(String user) {
    return cachedUserDsQuotas.getOrDefault(user, Collections.emptyMap());
  }

  public Map<String, Long> getNameQuotaUsed(String user) {
    return cachedUserNsQuotas.getOrDefault(user, Collections.emptyMap());
  }

  public Map<String, Map<String, Long>> getAllDsQuotaUsed() {
    return cachedUserDsQuotas;
  }

  public Map<String, Map<String, Long>> getAllNsQuotaUsed() {
    return cachedUserNsQuotas;
  }
}
