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

import com.google.common.collect.Sets;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.namenode.Constants;
import org.apache.hadoop.hdfs.server.namenode.Constants.AnalysisState;
import org.apache.hadoop.hdfs.server.namenode.Constants.Histogram;
import org.apache.hadoop.hdfs.server.namenode.Constants.HistogramOutput;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLoader;
import org.apache.hadoop.hdfs.server.namenode.QueryEngine;
import org.apache.hadoop.hdfs.server.namenode.analytics.ApplicationConfiguration;
import org.apache.hadoop.hdfs.server.namenode.analytics.Helper;
import org.apache.hadoop.hdfs.server.namenode.analytics.HsqlDriver;
import org.apache.hadoop.hdfs.server.namenode.analytics.QueryChecker;
import org.apache.hadoop.hdfs.server.namenode.queries.Histograms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is class handles all end-user cached reports that the suggestions UI page uses. All
 * information stored here is present to "CACHE" priviledged users.
 *
 * <p>The goal of this class is to provide in-depth analysis for users and selected directories that
 * is stored in MapDB mmap'd file caches.
 *
 * <p>The main logic for this class is in the reloadSuggestions() method call which does a large
 * analysis and finally updates cache stores.
 */
public class SuggestionsEngine {

  public static final Logger LOG = LoggerFactory.getLogger(SuggestionsEngine.class.getName());

  private final CacheManager cacheManager;
  private final CachedDirectories cachedDirectories;

  private Map<String, Long> cachedValues;
  private Map<String, Map<String, Long>> cachedMaps;
  private Map<String, Long> cachedLogins;
  private Set<String> cachedUsers;
  private Map<String, String> cachedQueries;
  private Map<String, Map<String, Long>> cachedUserNsQuotas;
  private Map<String, Map<String, Long>> cachedUserDsQuotas;
  private Map<String, Long> cachedValueQueries;
  private Map<String, Map<String, Long>> cachedMapQueries;

  private AtomicBoolean loaded;
  private int suggestionsReloadSleepMs;
  private AnalysisState currentState;

  /** Main constructor. */
  public SuggestionsEngine() {
    this.cacheManager = new CacheManager();
    this.cachedDirectories = new CachedDirectories();
    this.loaded = new AtomicBoolean(false);
    this.currentState = AnalysisState.sleep;
  }

  public boolean isLoaded() {
    return loaded.get();
  }

  public AnalysisState getCurrentState() {
    return currentState;
  }

  public void setCurrentState(AnalysisState state) {
    this.currentState = state;
  }

  private Map<String, Long> getCachedMap(String innerMapName) {
    return cachedMaps.getOrDefault(innerMapName, Collections.emptyMap());
  }

  /**
   * This method should only be called after NameNodeLoader has finished loading the FSImage.
   *
   * <p>Calling this method will issue many queries in the background and update the various MapDB
   * cached objects.
   *
   * @param nameNodeLoader The main NameNodeLoader and in-memory metadata set.
   */
  public void reloadSuggestions(NameNodeLoader nameNodeLoader) {
    final long s1 = System.currentTimeMillis();
    Collection<INode> files = nameNodeLoader.getINodeSet("files");
    Collection<INode> dirs = nameNodeLoader.getINodeSet("dirs");

    final long numFiles = files.size();
    final long numDirs = dirs.size();
    long capacity = 0L;
    long timer;

    timer = System.currentTimeMillis();
    currentState = AnalysisState.capacity;
    try {
      FileSystem fs = nameNodeLoader.getFileSystem();
      capacity = fs.getStatus().getCapacity();
    } catch (IOException e) {
      LOG.error("Failed to fetch capacity from active cluster.", e);
    }
    long capacityFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.capacity took: {} ms.", capacityFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.fileAges;
    QueryEngine queryEngine = nameNodeLoader.getQueryEngine();
    final Map<String, Long> modTimeCount =
        queryEngine.modTimeHistogram(files.parallelStream(), "count", null, "monthly");
    final Map<String, Long> modTimeDiskspace =
        queryEngine.modTimeHistogram(files.parallelStream(), "diskspaceConsumed", null, "monthly");
    long fileAgesFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.fileAges took: {} ms.", fileAgesFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.users;
    final Set<String> fileUsers =
        files.parallelStream().map(INode::getUserName).collect(Collectors.toSet());
    final Set<String> dirUsers =
        dirs.parallelStream().map(INode::getUserName).collect(Collectors.toSet());
    final Set<String> users = Sets.union(fileUsers, dirUsers);
    long uniqueUsersFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.users took: {} ms.", uniqueUsersFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.diskspace;
    final long diskspace = queryEngine.sum(files, "diskspaceConsumed");
    final Map<String, Long> diskspaceUsers =
        queryEngine.byUserHistogram(files.parallelStream(), "diskspaceConsumed", null);
    long diskspaceUsersFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.diskspace took: {} ms.", diskspaceUsersFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.files24h;
    final Collection<INode> files24h =
        queryEngine.combinedFilter(files, new String[] {"modTime"}, new String[] {"hoursAgo:24"});
    final long numFiles24h = files24h.size();
    final long diskspace24h = queryEngine.sum(files24h, "diskspaceConsumed");
    Map<String, LongSummaryStatistics> countAndDisk24hUsers =
        queryEngine.genericSummarizingHistogram(
            files24h.parallelStream(),
            INode::getUserName,
            queryEngine.getSumFunctionForINode("diskspaceConsumed"));
    final Map<String, Long> numFiles24hUsers =
        countAndDisk24hUsers
            .entrySet()
            .parallelStream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getCount()));
    final Map<String, Long> diskspace24hUsers =
        countAndDisk24hUsers
            .entrySet()
            .parallelStream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSum()));
    long files24hUsersFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.files24hr took: {} ms.", files24hUsersFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.files1y2y;
    final Stream<INode> oldFiles1yr =
        queryEngine.combinedFilterToStream(
            files, new String[] {"accessTime"}, new String[] {"olderThanYears:1"});
    Map<String, LongSummaryStatistics> oldFiles1yrCountAndDisk =
        queryEngine.genericSummarizingHistogram(
            oldFiles1yr,
            INode::getUserName,
            queryEngine.getSumFunctionForINode("diskspaceConsumed"));
    final Map<String, Long> oldFiles1yrCountUsers =
        oldFiles1yrCountAndDisk
            .entrySet()
            .parallelStream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getCount()));
    final Map<String, Long> oldFiles1yrDsUsers =
        oldFiles1yrCountAndDisk
            .entrySet()
            .parallelStream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSum()));
    final Stream<INode> oldFiles2yr =
        queryEngine.combinedFilterToStream(
            files, new String[] {"accessTime"}, new String[] {"olderThanYears:2"});
    final Map<String, Long> oldFiles2yrCountUsers =
        queryEngine.byUserHistogram(oldFiles2yr, "count", null);
    final Map<String, Long> oldFiles2yrDsUsers =
        queryEngine.byUserHistogram(oldFiles2yr, "diskspaceConsumed", null);
    long oldFilesUsersFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.files1yr2yr took: {} ms.", oldFilesUsersFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.perUserCount;
    final Map<String, Long> filesUsers =
        queryEngine.byUserHistogram(files.parallelStream(), "count", null);
    final Map<String, Long> dirsUsers =
        queryEngine.byUserHistogram(dirs.parallelStream(), "count", null);
    long perUserCountFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.perUserCount took: {} ms.", perUserCountFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.directories;
    Map<String, LongSummaryStatistics> dirCountAndDisk =
        queryEngine.genericSummarizingHistogram(
            files.parallelStream(),
            Helper.getDirectoryAtDepthFunction(3),
            queryEngine.getSumFunctionForINode("diskspaceConsumed"));
    dirCountAndDisk.remove("NO_MAPPING");
    Map<String, Long> dirCount =
        dirCountAndDisk
            .entrySet()
            .parallelStream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getCount()));
    Map<String, Long> dirDs =
        dirCountAndDisk
            .entrySet()
            .parallelStream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSum()));
    dirDs = Histograms.sliceToTop(dirDs, 1000);
    dirCount = Histograms.sliceToTop(dirCount, 1000);
    currentState = AnalysisState.cachedDirectories;
    cachedDirectories.analyze(nameNodeLoader, dirCount, dirDs);
    long directoriesFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.directories took: {} ms.", directoriesFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.systemFilter;
    final Collection<INode> emptyFiles =
        queryEngine.combinedFilter(files, new String[] {"fileSize"}, new String[] {"eq:0"});
    final Collection<INode> emptyDirs =
        queryEngine.combinedFilter(dirs, new String[] {"dirNumChildren"}, new String[] {"eq:0"});
    final Collection<INode> tinyFiles =
        queryEngine.combinedFilter(
            files, new String[] {"fileSize", "fileSize"}, new String[] {"lte:1024", "gt:0"});
    final Collection<INode> smallFiles =
        queryEngine.combinedFilter(
            files, new String[] {"fileSize", "fileSize"}, new String[] {"lte:1048576", "gt:1024"});
    final Collection<INode> mediumFiles =
        queryEngine.combinedFilter(
            files,
            new String[] {"fileSize", "fileSize"},
            new String[] {"lte:134217728", "gt:1048576"});
    long systemFilterFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.systemFilter took: {} ms.", systemFilterFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.system24h;
    final Collection<INode> emptyFiles24h =
        queryEngine.combinedFilter(
            emptyFiles, new String[] {"modTime"}, new String[] {"hoursAgo:24"});
    final Collection<INode> emptyDirs24h =
        queryEngine.combinedFilter(
            emptyDirs, new String[] {"modTime"}, new String[] {"hoursAgo:24"});
    final Collection<INode> tinyFiles24h =
        queryEngine.combinedFilter(
            tinyFiles, new String[] {"modTime"}, new String[] {"hoursAgo:24"});
    final Collection<INode> smallFiles24h =
        queryEngine.combinedFilter(
            smallFiles, new String[] {"modTime"}, new String[] {"hoursAgo:24"});
    long system24hFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.system24hr took: {} ms.", system24hFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.system1y;
    final Collection<INode> emptyFiles1yr =
        queryEngine.combinedFilter(
            emptyFiles, new String[] {"accessTime"}, new String[] {"olderThanYears:1"});
    final Collection<INode> emptyDirs1yr =
        queryEngine.combinedFilter(
            emptyDirs, new String[] {"modTime"}, new String[] {"olderThanYears:1"});
    final Collection<INode> tinyFiles1yr =
        queryEngine.combinedFilter(
            tinyFiles, new String[] {"accessTime"}, new String[] {"olderThanYears:1"});
    final Collection<INode> smallFiles1yr =
        queryEngine.combinedFilter(
            smallFiles, new String[] {"accessTime"}, new String[] {"olderThanYears:1"});
    long system1yrSuggFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.system1yr took: {} ms.", system1yrSuggFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.systemCount;
    final long emptyFilesCount = emptyFiles.size();
    final long emptyDirsCount = emptyDirs.size();
    final long emptyFilesMem = queryEngine.sum(emptyFiles, "memoryConsumed");
    final long emptyDirsMem = queryEngine.sum(emptyDirs, "memoryConsumed");
    final long tinyFilesCount = tinyFiles.size();
    final long smallFilesCount = smallFiles.size();
    final long mediumFilesCount = mediumFiles.size();
    final long largeFilesCount =
        numFiles - emptyFilesCount - tinyFilesCount - smallFilesCount - mediumFilesCount;
    final long tinyFilesMem = queryEngine.sum(tinyFiles, "memoryConsumed");
    final long smallFilesMem = queryEngine.sum(smallFiles, "memoryConsumed");
    final long tinyFilesDs = queryEngine.sum(tinyFiles, "diskspaceConsumed");
    final long smallFilesDs = queryEngine.sum(smallFiles, "diskspaceConsumed");

    final long emptyFiles24hCount = emptyFiles24h.size();
    final long emptyDirs24hCount = emptyDirs24h.size();
    final long emptyFiles24hMem = queryEngine.sum(emptyFiles24h, "memoryConsumed");
    final long emptyDirs24hMem = queryEngine.sum(emptyDirs24h, "memoryConsumed");
    final long tinyFiles24hCount = tinyFiles24h.size();
    final long smallFiles24hCount = smallFiles24h.size();
    final long tinyFiles24hMem = queryEngine.sum(tinyFiles24h, "memoryConsumed");
    final long smallFiles24hMem = queryEngine.sum(smallFiles24h, "memoryConsumed");
    final long tinyFiles24hDs = queryEngine.sum(tinyFiles24h, "diskspaceConsumed");
    final long smallFiles24hDs = queryEngine.sum(smallFiles24h, "diskspaceConsumed");

    final long emptyFiles1yrCount = emptyFiles1yr.size();
    final long emptyDirs1yrCount = emptyDirs1yr.size();
    final long tinyFiles1yrCount = tinyFiles1yr.size();
    final long smallFiles1yrCount = smallFiles1yr.size();

    final long oldFiles1yrCount =
        oldFiles1yrCountUsers.entrySet().parallelStream().mapToLong(Entry::getValue).sum();
    final long oldFiles2yrCount =
        oldFiles2yrCountUsers.entrySet().parallelStream().mapToLong(Entry::getValue).sum();
    final long oldFiles1yrDs =
        oldFiles1yrDsUsers.entrySet().parallelStream().mapToLong(Entry::getValue).sum();
    final long oldFiles2yrDs =
        oldFiles2yrDsUsers.entrySet().parallelStream().mapToLong(Entry::getValue).sum();
    long systemCountsFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.systemCount took: {} ms.", systemCountsFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.perUserFilter;
    final Map<String, Long> emptyFilesUsers =
        queryEngine.byUserHistogram(emptyFiles.parallelStream(), "count", null);
    final Map<String, Long> emptyDirsUsers =
        queryEngine.byUserHistogram(emptyDirs.parallelStream(), "count", null);
    final Map<String, Long> tinyFilesUsers =
        queryEngine.byUserHistogram(tinyFiles.parallelStream(), "count", null);
    final Map<String, Long> smallFilesUsers =
        queryEngine.byUserHistogram(smallFiles.parallelStream(), "count", null);
    final Map<String, Long> mediumFilesUsers =
        queryEngine.byUserHistogram(mediumFiles.parallelStream(), "count", null);
    final Map<String, Long> largeFilesUsers = new HashMap<>(users.size());
    users.forEach(
        u -> {
          long largeFiles =
              filesUsers.getOrDefault(u, 0L)
                  - emptyFilesUsers.getOrDefault(u, 0L)
                  - tinyFilesUsers.getOrDefault(u, 0L)
                  - smallFilesUsers.getOrDefault(u, 0L)
                  - mediumFilesUsers.getOrDefault(u, 0L);
          largeFilesUsers.put(u, largeFiles);
        });
    long perUserSuggFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.perUserFilter took: {} ms.", perUserSuggFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.perUser24h;
    final Map<String, Long> emptyFiles24hUsers =
        queryEngine.byUserHistogram(emptyFiles24h.parallelStream(), "count", null);
    final Map<String, Long> emptyDirs24hUsers =
        queryEngine.byUserHistogram(emptyDirs24h.parallelStream(), "count", null);
    final Map<String, Long> tinyFiles24hUsers =
        queryEngine.byUserHistogram(tinyFiles24h.parallelStream(), "count", null);
    final Map<String, Long> smallFiles24hUsers =
        queryEngine.byUserHistogram(smallFiles24h.parallelStream(), "count", null);
    final Map<String, Long> emptyFiles24hMemUsers =
        queryEngine.byUserHistogram(emptyFiles24h.parallelStream(), "memoryConsumed", null);
    final Map<String, Long> emptyDirs24hMemUsers =
        queryEngine.byUserHistogram(emptyDirs24h.parallelStream(), "memoryConsumed", null);
    final Map<String, Long> tinyFiles24hMemUsers =
        queryEngine.byUserHistogram(tinyFiles24h.parallelStream(), "memoryConsumed", null);
    final Map<String, Long> smallFiles24hMemUsers =
        queryEngine.byUserHistogram(smallFiles24h.parallelStream(), "memoryConsumed", null);
    final Map<String, Long> tinyFiles24hDsUsers =
        queryEngine.byUserHistogram(tinyFiles24h.parallelStream(), "diskspaceConsumed", null);
    final Map<String, Long> smallFiles24hDsUsers =
        queryEngine.byUserHistogram(smallFiles24h.parallelStream(), "diskspaceConsumed", null);
    long perUser24hSuggFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.perUser24h took: {} ms.", perUser24hSuggFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.perUser1y;
    final Map<String, Long> emptyFiles1yrUsers =
        queryEngine.byUserHistogram(emptyFiles1yr.parallelStream(), "count", null);
    final Map<String, Long> emptyDirs1yrUsers =
        queryEngine.byUserHistogram(emptyDirs1yr.parallelStream(), "count", null);
    final Map<String, Long> tinyFiles1yrUsers =
        queryEngine.byUserHistogram(tinyFiles1yr.parallelStream(), "count", null);
    final Map<String, Long> smallFiles1yrUsers =
        queryEngine.byUserHistogram(smallFiles1yr.parallelStream(), "count", null);
    long perUser1yrSuggFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.perUser1yr took: {} ms.", perUser1yrSuggFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.perUserMem;
    final Map<String, Long> emptyFilesMemUsers =
        queryEngine.byUserHistogram(emptyFiles.parallelStream(), "memoryConsumed", null);
    final Map<String, Long> emptyDirsMemUsers =
        queryEngine.byUserHistogram(emptyDirs.parallelStream(), "memoryConsumed", null);
    final Map<String, Long> tinyFilesMemUsers =
        queryEngine.byUserHistogram(tinyFiles.parallelStream(), "memoryConsumed", null);
    final Map<String, Long> smallFilesMemUsers =
        queryEngine.byUserHistogram(smallFiles.parallelStream(), "memoryConsumed", null);
    long perUserMemFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.perUserMem took: {} ms.", perUserMemFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.perUserDs;
    final Map<String, Long> tinyFilesDsUsers =
        queryEngine.byUserHistogram(tinyFiles.parallelStream(), "diskspaceConsumed", null);
    final Map<String, Long> smallFilesDsUsers =
        queryEngine.byUserHistogram(smallFiles.parallelStream(), "diskspaceConsumed", null);
    long perUserDsFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.perUserDs took: {} ms.", perUserDsFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.directories24h;
    Map<String, LongSummaryStatistics> dirCountAndDisk24h =
        queryEngine.genericSummarizingHistogram(
            files.parallelStream(),
            Helper.getDirectoryAtDepthFunction(3),
            queryEngine.getSumFunctionForINode("diskspaceConsumed"));
    dirCountAndDisk24h.remove("NO_MAPPING");
    Map<String, Long> dirCount24h =
        dirCountAndDisk24h
            .entrySet()
            .parallelStream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getCount()));
    Map<String, Long> dirDs24h =
        dirCountAndDisk24h
            .entrySet()
            .parallelStream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSum()));
    dirDs = Histograms.sliceToTop(dirDs24h, 1000);
    dirCount = Histograms.sliceToTop(dirCount24h, 1000);
    currentState = AnalysisState.cachedDirectories24h;
    cachedDirectories.analyze(queryEngine, files24h, dirCount24h, dirDs24h);
    long directories24hFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.directories24h took: {} ms.", directories24hFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.cachedQuotas;
    long nsQuotaCount = 0;
    long dsQuotaCount = 0;
    long nsQuotaThreshCount = 0;
    long dsQuotaThreshCount = 0;
    final Map<String, Long> nsQuotaThreshCountsUsers = new HashMap<>();
    final Map<String, Long> dsQuotaThreshCountsUsers = new HashMap<>();
    final Map<String, Long> nsQuotaCountsUsers = new HashMap<>();
    final Map<String, Long> dsQuotaCountsUsers = new HashMap<>();
    for (String user : users) {
      Collection<INode> quotaDirs =
          queryEngine.combinedFilter(
              dirs, new String[] {"user", "hasQuota"}, new String[] {"eq:" + user, "eq:true"});
      Map<String, Long> nsQuotaRatio =
          queryEngine.dirQuotaHistogram(quotaDirs.parallelStream(), "nsQuotaRatioUsed");
      Map<String, Long> dsQuotaRatio =
          queryEngine.dirQuotaHistogram(quotaDirs.parallelStream(), "dsQuotaRatioUsed");
      final long nsThreshExceeded =
          nsQuotaRatio.values().parallelStream().filter(v -> v > 85L).count();
      final long dsThreshExceeded =
          dsQuotaRatio.values().parallelStream().filter(v -> v > 85L).count();
      cachedUserNsQuotas.put(user, nsQuotaRatio);
      cachedUserDsQuotas.put(user, dsQuotaRatio);
      nsQuotaThreshCountsUsers.put(user, nsThreshExceeded);
      dsQuotaThreshCountsUsers.put(user, dsThreshExceeded);
      nsQuotaCount += nsQuotaRatio.size();
      dsQuotaCount += dsQuotaRatio.size();
      nsQuotaThreshCount += nsThreshExceeded;
      dsQuotaThreshCount += dsThreshExceeded;
      nsQuotaCountsUsers.put(user, (long) nsQuotaRatio.size());
      dsQuotaCountsUsers.put(user, (long) dsQuotaRatio.size());
    }
    long quotaFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.cachedQuotas took: {} ms.", quotaFetchTime);

    timer = System.currentTimeMillis();
    currentState = AnalysisState.cachedLogins;
    cachedLogins.putAll(nameNodeLoader.getTokenExtractor().getTokenLastLogins());
    users.forEach(u -> cachedLogins.putIfAbsent(u, -1L));
    cachedLogins.keySet().removeIf(u -> !fileUsers.contains(u) && !dirUsers.contains(u));
    long cachedLoginsFetchTime = System.currentTimeMillis() - timer;
    LOG.info("Performing SuggestionsEngine.cachedLogins took: {} ms.", cachedLoginsFetchTime);

    long e1 = System.currentTimeMillis();
    final long timeTaken = (e1 - s1);

    final long s2 = System.currentTimeMillis();
    cachedUsers.clear();
    cachedUsers.addAll(users);
    cachedValues.put("timeTaken", timeTaken);
    cachedValues.put("reportTime", e1);
    cachedValues.put("nextReportEstimate", e1 + timeTaken + suggestionsReloadSleepMs);
    cachedValues.put("capacity", capacity);
    cachedValues.put("diskspace", diskspace);
    cachedValues.put("diskspace24h", diskspace24h);
    cachedValues.put("numFiles", numFiles);
    cachedValues.put("numFiles24h", numFiles24h);
    cachedValues.put("numDirs", numDirs);
    cachedValues.put("totalFiles", numFiles);
    cachedValues.put("totalDirs", numDirs);
    cachedValues.put("emptyFiles", emptyFilesCount);
    cachedValues.put("emptyDirs", emptyDirsCount);
    cachedValues.put("tinyFiles", tinyFilesCount);
    cachedValues.put("smallFiles", smallFilesCount);
    cachedValues.put("emptyFiles24h", emptyFiles24hCount);
    cachedValues.put("emptyDirs24h", emptyDirs24hCount);
    cachedValues.put("tinyFiles24h", tinyFiles24hCount);
    cachedValues.put("smallFiles24h", smallFiles24hCount);
    cachedValues.put("emptyFiles1yr", emptyFiles1yrCount);
    cachedValues.put("emptyDirs1yr", emptyDirs1yrCount);
    cachedValues.put("tinyFiles1yr", tinyFiles1yrCount);
    cachedValues.put("smallFiles1yr", smallFiles1yrCount);
    cachedValues.put("mediumFiles", mediumFilesCount);
    cachedValues.put("largeFiles", largeFilesCount);
    cachedValues.put("emptyFilesMem", emptyFilesMem);
    cachedValues.put("emptyDirsMem", emptyDirsMem);
    cachedValues.put("tinyFilesMem", tinyFilesMem);
    cachedValues.put("tinyFilesDs", tinyFilesDs);
    cachedValues.put("smallFilesMem", smallFilesMem);
    cachedValues.put("smallFilesDs", smallFilesDs);
    cachedValues.put("emptyFiles24hMem", emptyFiles24hMem);
    cachedValues.put("emptyDirs24hMem", emptyDirs24hMem);
    cachedValues.put("tinyFiles24hMem", tinyFiles24hMem);
    cachedValues.put("smallFiles24hMem", smallFiles24hMem);
    cachedValues.put("tinyFiles24hDs", tinyFiles24hDs);
    cachedValues.put("smallFiles24hDs", smallFiles24hDs);
    cachedValues.put("oldFiles1yr", oldFiles1yrCount);
    cachedValues.put("oldFiles1yrDs", oldFiles1yrDs);
    cachedValues.put("oldFiles2yr", oldFiles2yrCount);
    cachedValues.put("oldFiles2yrDs", oldFiles2yrDs);
    cachedValues.put("nsQuotaCount", nsQuotaCount);
    cachedValues.put("dsQuotaCount", dsQuotaCount);
    cachedValues.put("nsQuotaThreshCount", nsQuotaThreshCount);
    cachedValues.put("dsQuotaThreshCount", dsQuotaThreshCount);
    cachedMaps.put("diskspaceUsers", diskspaceUsers);
    cachedMaps.put("numFilesUsers", filesUsers);
    cachedMaps.put("numDirsUsers", dirsUsers);
    cachedMaps.put("emptyFilesUsers", emptyFilesUsers);
    cachedMaps.put("emptyDirsUsers", emptyDirsUsers);
    cachedMaps.put("emptyFilesMemUsers", emptyFilesMemUsers);
    cachedMaps.put("emptyDirsMemUsers", emptyDirsMemUsers);
    cachedMaps.put("tinyFilesUsers", tinyFilesUsers);
    cachedMaps.put("smallFilesUsers", smallFilesUsers);
    cachedMaps.put("tinyFilesMemUsers", tinyFilesMemUsers);
    cachedMaps.put("smallFilesMemUsers", smallFilesMemUsers);
    cachedMaps.put("tinyFilesDsUsers", tinyFilesDsUsers);
    cachedMaps.put("smallFilesDsUsers", smallFilesDsUsers);
    cachedMaps.put("diskspace24hUsers", diskspace24hUsers);
    cachedMaps.put("numFiles24hUsers", numFiles24hUsers);
    cachedMaps.put("emptyFiles24hUsers", emptyFiles24hUsers);
    cachedMaps.put("emptyDirs24hUsers", emptyDirs24hUsers);
    cachedMaps.put("emptyFiles24hMemUsers", emptyFiles24hMemUsers);
    cachedMaps.put("emptyDirs24hMemUsers", emptyDirs24hMemUsers);
    cachedMaps.put("tinyFiles24hUsers", tinyFiles24hUsers);
    cachedMaps.put("smallFiles24hUsers", smallFiles24hUsers);
    cachedMaps.put("tinyFiles24hMemUsers", tinyFiles24hMemUsers);
    cachedMaps.put("smallFiles24hMemUsers", smallFiles24hMemUsers);
    cachedMaps.put("tinyFiles24hDsUsers", tinyFiles24hDsUsers);
    cachedMaps.put("smallFiles24hDsUsers", smallFiles24hDsUsers);
    cachedMaps.put("emptyFiles1yrUsers", emptyFiles1yrUsers);
    cachedMaps.put("emptyDirs1yrUsers", emptyDirs1yrUsers);
    cachedMaps.put("tinyFiles1yrUsers", tinyFiles1yrUsers);
    cachedMaps.put("smallFiles1yrUsers", smallFiles1yrUsers);
    cachedMaps.put("mediumFilesUsers", mediumFilesUsers);
    cachedMaps.put("largeFilesUsers", largeFilesUsers);
    cachedMaps.put("oldFiles1yrUsers", oldFiles1yrCountUsers);
    cachedMaps.put("oldFiles1yrDsUsers", oldFiles1yrDsUsers);
    cachedMaps.put("oldFiles2yrUsers", oldFiles2yrCountUsers);
    cachedMaps.put("oldFiles2yrDsUsers", oldFiles2yrDsUsers);
    cachedMaps.put("dirCount", dirCount);
    cachedMaps.put("dirDs", dirDs);
    cachedMaps.put("dirCount24h", dirCount24h);
    cachedMaps.put("dirDs24h", dirDs24h);
    cachedMaps.put("modTimeCount", modTimeCount);
    cachedMaps.put("modTimeDiskspace", modTimeDiskspace);
    cachedMaps.put("nsQuotaCountsUsers", nsQuotaCountsUsers);
    cachedMaps.put("dsQuotaCountsUsers", dsQuotaCountsUsers);
    cachedMaps.put("nsQuotaThreshCountsUsers", nsQuotaThreshCountsUsers);
    cachedMaps.put("dsQuotaThreshCountsUsers", dsQuotaThreshCountsUsers);

    long e2 = System.currentTimeMillis();
    LOG.info("Sync-switch of suggestions took: {} ms.", (e2 - s2));
    LOG.info("Reloading suggestions matrices took: {} ms.", timeTaken);
    loaded.set(true);

    HsqlDriver historyDbDriver = nameNodeLoader.getEmbeddedHistoryDatabaseDriver();
    if (historyDbDriver != null && nameNodeLoader.isInit() && nameNodeLoader.isHistorical()) {
      long s3 = System.currentTimeMillis();
      currentState = AnalysisState.history;
      try {
        historyDbDriver.logHistoryPerUser(cachedValues, cachedMaps, cachedUsers);
      } catch (SQLException e) {
        LOG.error("Failed to write historical data.", e);
      }
      long e3 = System.currentTimeMillis();
      LOG.info("Writing to embedded SQL DB took: {} ms.", (e3 - s3));
    } else {
      LOG.info("No historical data written as it is disabled.");
    }

    long s4 = System.currentTimeMillis();
    currentState = AnalysisState.cachedQueries;
    try {
      performCustomQueries(nameNodeLoader);
    } catch (Exception e) {
      LOG.error("Failed to write custom query data.", e);
    }
    long e4 = System.currentTimeMillis();
    LOG.info("Performing SuggestionsEngine.cachedQueries took: {} ms.", (e4 - s4));

    long s5 = System.currentTimeMillis();
    currentState = AnalysisState.writeMapDb;
    try {
      cacheManager.commit();
    } catch (Exception e) {
      LOG.error("Failed to write cache data.", e);
    }
    long e5 = System.currentTimeMillis();
    LOG.info("Writing to embedded MapDB took: {} ms.", (e5 - s5));
    currentState = AnalysisState.sleep;
  }

  public String getTokens() {
    return Histograms.toJson(Histograms.sortByValue(cachedLogins, true));
  }

  /**
   * Adds a directory to get count and size analysis for specifically.
   *
   * @param directory directory to add to reporting
   * @throws IOException directory could not be added to reporting
   */
  public void addDirectoryToAnalysis(String directory) throws IOException {
    if (directory == null || directory.isEmpty()) {
      throw new IllegalArgumentException("Directory parameter 'dir' not defined.");
    }
    if (directory.endsWith("/")) {
      directory = directory.substring(0, directory.length() - 1);
    }
    boolean existed = cachedDirectories.add(directory);
    if (existed) {
      throw new IOException(directory + " already set for analysis.");
    }
  }

  /**
   * Removes a directory from count and size analysis.
   *
   * @param directory directory to remove from reporting
   * @throws IOException directory could not be removed from reporting
   */
  public void removeDirectoryFromAnalysis(String directory) throws IOException {
    if (directory == null || directory.isEmpty()) {
      throw new IllegalArgumentException("Directory parameter 'dir' not defined.");
    }
    if (directory.endsWith("/")) {
      directory = directory.substring(0, directory.length() - 1);
    }
    boolean removed = cachedDirectories.remove(directory);
    if (!removed) {
      throw new FileNotFoundException(directory + " was not scheduled for analysis.");
    }
  }

  public Set<String> getDirectoriesForAnalysis() {
    return cachedDirectories.getCachedDirSet();
  }

  /**
   * Adds a query to run alongside suggestions cache reporting.
   *
   * @param query query to add to reporting
   * @throws IOException query could not be added to reporting
   */
  public void setQueryToAnalysis(String queryName, String query) throws IOException {
    if (query == null || query.isEmpty()) {
      throw new IllegalArgumentException("Query not defined.");
    }
    Map<String, String> params = splitQuery(query);
    String queryType = params.get("queryType");
    switch (queryType) {
      case "filter":
      case "histogram":
        break;
      default:
        throw new IllegalArgumentException("Query type " + queryType + " is not a valid query.");
    }
    String set = params.get("set");
    String[] filters = Helper.parseFilters(params.get("filters"));
    String[] filterOps = Helper.parseFilterOps(params.get("filters"));
    String find = params.get("find");
    String sum = params.get("sum");
    String type = params.get("type");
    QueryChecker.isValidQuery(set, filters, type, sum, filterOps, find);
    String oldQuery = cachedQueries.put(queryName, query);
    if (oldQuery != null) {
      LOG.info(query + " has replaced " + oldQuery + " as analysis for " + queryName + ".");
    }
  }

  /**
   * Removes a query from cache reporting.
   *
   * @param queryName query to remove from reporting
   * @throws IOException query could not be removed from reporting
   */
  public void removeQueryFromAnalysis(String queryName) throws IOException {
    if (queryName == null || queryName.isEmpty()) {
      throw new IllegalArgumentException("Query not defined.");
    }
    String query = cachedQueries.remove(queryName);
    if (query == null) {
      throw new FileNotFoundException(queryName + " was not scheduled for analysis.");
    } else {
      cachedValueQueries.remove(queryName);
      cachedMapQueries.remove(queryName);
    }
  }

  public Map<String, String> getQueriesForAnalysis() {
    return cachedQueries;
  }

  /**
   * Fetches latest cached value or histogram.
   *
   * @param queryName query to get cached result from
   * @param response http response for setting correct header
   * @return a String representing a Long value or a JSON string representing histogram result
   * @throws IOException cached query could not be found
   */
  public String getLatestCacheQueryResult(String queryName, HttpServletResponse response)
      throws IOException {
    if (queryName == null || queryName.isEmpty()) {
      throw new IllegalArgumentException("Query not defined.");
    }
    String query = cachedQueries.get(queryName);
    if (query == null) {
      throw new FileNotFoundException(queryName + " was not scheduled for analysis.");
    }
    Map<String, String> params = splitQuery(query);
    String queryType = params.get("queryType");
    switch (queryType) {
      case "filter":
        response.setContentType(MediaType.TEXT_PLAIN);
        return String.valueOf(cachedValueQueries.get(queryName));
      case "histogram":
        String outputTypeStr = params.get("histogramOutput");
        final String outputType = (outputTypeStr != null) ? outputTypeStr : "json";
        String rawTimestampsStr = params.get("rawTimestamps");
        String sortAscendingStr = params.get("sortAscending");
        final Boolean sortAscending =
            sortAscendingStr == null ? null : Boolean.parseBoolean(sortAscendingStr);
        String sortDescendingStr = params.get("sortDescending");
        final Boolean sortDescending =
            sortDescendingStr == null ? null : Boolean.parseBoolean(sortDescendingStr);
        final boolean rawTimestamps = Boolean.parseBoolean(rawTimestampsStr);
        String topStr = params.get("top");
        final Integer top = (topStr == null) ? null : Integer.parseInt(topStr);
        String bottomStr = params.get("bottom");
        final Integer bottom = (bottomStr == null) ? null : Integer.parseInt(bottomStr);
        final String find = params.get("find");

        Map<String, Long> histogram = cachedMapQueries.get(queryName);
        if (histogram == null) {
          response.setContentType(MediaType.TEXT_PLAIN);
          return "null";
        }

        // Slice top and bottom.
        if (top != null && bottom != null) {
          throw new IllegalArgumentException("Please choose only one type of slice.");
        } else if (top != null && top > 0) {
          histogram = Histograms.sliceToTop(histogram, top);
        } else if (bottom != null && bottom > 0) {
          histogram = Histograms.sliceToBottom(histogram, bottom);
        }

        // Sort results.
        if (sortAscending != null && sortDescending != null) {
          throw new IllegalArgumentException("Please choose one type of sort.");
        } else if (sortAscending != null && sortAscending) {
          histogram = Histograms.sortByValue(histogram, true);
        } else if (sortDescending != null && sortDescending) {
          histogram = Histograms.sortByValue(histogram, false);
        }

        HistogramOutput output = HistogramOutput.valueOf(outputType);
        switch (output) {
          case json:
            response.setContentType(MediaType.APPLICATION_JSON);
            return Histograms.toJson(histogram);
          case csv:
            response.setContentType(MediaType.TEXT_PLAIN);
            return Histograms.toCsv(histogram, find, rawTimestamps);
          default:
            throw new IllegalArgumentException(
                "Could not determine output type: "
                    + outputType
                    + ".\nPlease check /histogramOutputs for available histogram outputs.");
        }
      default:
        throw new IllegalArgumentException("Query type " + queryType + " is not a valid query.");
    }
  }

  private Map<String, String> splitQuery(String query) {
    Map<String, String> queryPairs = new HashMap<>();
    String[] pairs = query.split("&");
    try {
      for (String pair : pairs) {
        int idx = pair.indexOf("=");
        queryPairs.put(
            URLDecoder.decode(pair.substring(0, idx), Constants.CHARSET.name()),
            URLDecoder.decode(pair.substring(idx + 1), Constants.CHARSET.name()));
      }
    } catch (UnsupportedEncodingException ex) {
      LOG.error("Failed to parse query: " + query, ex);
      return Collections.emptyMap();
    }
    return queryPairs;
  }

  private void performCustomQueries(NameNodeLoader nameNodeLoader) {
    cachedQueries.forEach(
        (queryName, query) -> {
          Map<String, String> params = splitQuery(query);
          String queryType = params.get("queryType");
          String set = params.get("set");
          String[] filters = Helper.parseFilters(params.get("filters"));
          String[] filterOps = Helper.parseFilterOps(params.get("filters"));
          String find = params.get("find");
          String sum = params.get("sum");
          if ("filter".equals(queryType)) {
            Collection<INode> filteredINodes =
                Helper.performFilters(nameNodeLoader, set, filters, filterOps, find);
            long value = nameNodeLoader.getQueryEngine().sum(filteredINodes, sum);
            cachedValueQueries.put(queryName, value);
          } else if ("histogram".equals(queryType)) {
            Stream<INode> filteredINodes =
                Helper.setFilters(nameNodeLoader, set, filters, filterOps);
            String histogramType = params.get("type");
            String timeRangeStr = params.get("timeRange");
            String timeRange = (timeRangeStr != null) ? timeRangeStr : "weekly";
            String parentDirDepthStr = params.get("parentDirDepth");
            final Integer parentDirDepth =
                (parentDirDepthStr == null) ? null : Integer.parseInt(parentDirDepthStr);
            Histogram htEnum = Histogram.valueOf(histogramType);
            Map<String, Long> histogram;
            switch (htEnum) {
              case user:
                histogram =
                    nameNodeLoader.getQueryEngine().byUserHistogram(filteredINodes, sum, find);
                break;
              case group:
                histogram =
                    nameNodeLoader.getQueryEngine().byGroupHistogram(filteredINodes, sum, find);
                break;
              case accessTime:
                histogram =
                    nameNodeLoader
                        .getQueryEngine()
                        .accessTimeHistogram(filteredINodes, sum, find, timeRange);
                break;
              case modTime:
                histogram =
                    nameNodeLoader
                        .getQueryEngine()
                        .modTimeHistogram(filteredINodes, sum, find, timeRange);
                break;
              case fileSize:
                histogram =
                    nameNodeLoader.getQueryEngine().fileSizeHistogram(filteredINodes, sum, find);
                break;
              case diskspaceConsumed:
                histogram =
                    nameNodeLoader
                        .getQueryEngine()
                        .diskspaceConsumedHistogram(
                            filteredINodes, sum, find, Collections.emptyMap());
                break;
              case fileReplica:
                histogram =
                    nameNodeLoader
                        .getQueryEngine()
                        .fileReplicaHistogram(filteredINodes, sum, find, Collections.emptyMap());
                break;
              case storageType:
                histogram =
                    nameNodeLoader.getQueryEngine().storageTypeHistogram(filteredINodes, sum, find);
                break;
              case memoryConsumed:
                histogram =
                    nameNodeLoader
                        .getQueryEngine()
                        .memoryConsumedHistogram(filteredINodes, sum, find);
                break;
              case parentDir:
                histogram =
                    nameNodeLoader
                        .getQueryEngine()
                        .parentDirHistogram(filteredINodes, parentDirDepth, sum, find);
                break;
              case fileType:
                histogram =
                    nameNodeLoader.getQueryEngine().fileTypeHistogram(filteredINodes, sum, find);
                break;
              case dirQuota:
                histogram = nameNodeLoader.getQueryEngine().dirQuotaHistogram(filteredINodes, sum);
                break;
              default:
                throw new IllegalArgumentException(
                    "Could not determine histogram type: "
                        + histogramType
                        + ".\nPlease check /histograms for available histograms.");
            }
            cachedMapQueries.put(queryName, histogram);
          } else {
            throw new IllegalArgumentException(
                "Query type " + queryType + " is not a valid query.");
          }
        });
  }

  /**
   * Get all quota information from cache as a JSON String.
   *
   * @return quota info returned as JSON string
   */
  public String getAllQuotasAsJson() {
    Map<String, Map<String, Map<String, Long>>> allQuotaRatios = new HashMap<>();
    Map<String, Map<String, Long>> nsQuotas = cachedUserNsQuotas;
    Map<String, Map<String, Long>> dsQuotas = cachedUserDsQuotas;
    allQuotaRatios.put("nsQuotas", nsQuotas);
    allQuotaRatios.put("dsQuotas", dsQuotas);
    return Histograms.toJson(allQuotaRatios);
  }

  /**
   * Get quota information from cache as a JSON String.
   *
   * @param user optional; username to get quota info for
   * @param sum required; which type of quota to get
   * @return quota info returned as JSON string
   */
  public String getQuotaAsJson(String user, String sum) {
    if (sum == null || sum.length() == 0) {
      throw new IllegalArgumentException(
          "Please define a sum of either diskspaceConsumed or count for Quotas.");
    }
    if (user != null && user.length() > 0) {
      switch (sum) {
        case "dsQuotaRatioUsed":
          return Histograms.toJson(Histograms.sortByValue(cachedUserDsQuotas.get(user), false));
        case "nsQuotaRatioUsed":
          return Histograms.toJson(Histograms.sortByValue(cachedUserNsQuotas.get(user), false));
        default:
          throw new IllegalArgumentException(
              "Please choose between diskspaceConsumed or count for Quotas.");
      }
    } else {
      switch (sum) {
        case "dsQuotaRatioUsed":
          return Histograms.toJson(cachedUserDsQuotas);
        case "nsQuotaRatioUsed":
          return Histograms.toJson(cachedUserNsQuotas);
        default:
          throw new IllegalArgumentException(
              "Please choose between diskspaceConsumed or count for Quotas.");
      }
    }
  }

  /**
   * Get file age histogram from cache as a JSON String.
   *
   * @param sum required; either count or diskspaceConsumed
   * @return file ages returned as JSON string
   */
  public String getFileAgeAsJson(String sum) {
    if (sum == null || sum.length() == 0) {
      throw new IllegalArgumentException(
          "Please define a sum of either diskspaceConsumed or count for File ages.");
    }
    switch (sum) {
      case "diskspaceConsumed":
        return Histograms.toJson(getCachedMap("modTimeDiskspace"));
      case "count":
        return Histograms.toJson(getCachedMap("modTimeCount"));
      default:
        throw new IllegalArgumentException(
            "Please choose between diskspaceConsumed or count for File ages.");
    }
  }

  /**
   * Get issue analysis from cache as a JSON String.
   *
   * @param suggestion the issue to look for from cache
   * @return the cached issue dump as a JSON string
   */
  public String getUsersAsJson(String suggestion) {
    if (suggestion == null || suggestion.isEmpty()) {
      return Histograms.toJson(cachedUsers);
    } else {
      Map<String, Long> userSuggestions = getSuggestion(suggestion);
      if (userSuggestions == null) {
        throw new IllegalArgumentException(suggestion + " is not a valid suggestion query.");
      }
      return Histograms.toJson(userSuggestions);
    }
  }

  /**
   * Get all users' analysis of a particular suggestion cache as Histogram.
   *
   * @param suggestion the suggestion to fetch histogram for
   * @return a histogram or null
   */
  public Map<String, Long> getSuggestion(String suggestion) {
    if (suggestion == null || suggestion.isEmpty()) {
      return null;
    }
    return cachedMaps.get(suggestion);
  }

  /**
   * Get all users' analysis from cache as a JSON String.
   *
   * @return the cached user dump as a JSON string
   */
  public String getAllSuggestionsAsJson() {
    Map<String, Map<String, Long>> allUsersSuggestions = new HashMap<>();
    for (String user : cachedUsers) {
      Map<String, Long> userMap = getUserMapFromCachedMaps(user);
      userMap.remove("totalFiles");
      userMap.remove("totalDirs");
      userMap.remove("capacity");
      userMap.remove("timeTaken");
      userMap.remove("reportTime");
      userMap.remove("nextReportEstimate");
      allUsersSuggestions.put(user, userMap);
    }
    return Histograms.toJson(allUsersSuggestions);
  }

  /**
   * Get user analysis from cache as a JSON String.
   *
   * @param user the user to look for from cache
   * @return the cached user dump as a JSON string
   */
  public String getSuggestionsAsJson(String user) {
    Map<String, Long> userMap = getUserMapFromCachedMaps(user);
    return Histograms.toJson(userMap);
  }

  private Map<String, Long> getUserMapFromCachedMaps(String user) {
    Map<String, Long> userMap = new HashMap<>(cachedValues);
    if (user == null || user.isEmpty()) {
      return userMap;
    }

    userMap.put("diskspace", getCachedMap("diskspaceUsers").getOrDefault(user, 0L));
    userMap.put("diskspace24h", getCachedMap("diskspace24hUsers").getOrDefault(user, 0L));
    userMap.put("numFiles", getCachedMap("numFilesUsers").getOrDefault(user, 0L));
    userMap.put("numFiles24h", getCachedMap("numFiles24hUsers").getOrDefault(user, 0L));
    userMap.put("numDirs", getCachedMap("numDirsUsers").getOrDefault(user, 0L));
    userMap.put("emptyFiles", getCachedMap("emptyFilesUsers").getOrDefault(user, 0L));
    userMap.put("emptyFiles24h", getCachedMap("emptyFiles24hUsers").getOrDefault(user, 0L));
    userMap.put("emptyFiles1yr", getCachedMap("emptyFiles1yrUsers").getOrDefault(user, 0L));
    userMap.put("emptyFilesMem", getCachedMap("emptyFilesMemUsers").getOrDefault(user, 0L));
    userMap.put("emptyFiles24hMem", getCachedMap("emptyFiles24hMemUsers").getOrDefault(user, 0L));
    userMap.put("emptyDirs", getCachedMap("emptyDirsUsers").getOrDefault(user, 0L));
    userMap.put("emptyDirs24h", getCachedMap("emptyDirs24hUsers").getOrDefault(user, 0L));
    userMap.put("emptyDirs1yr", getCachedMap("emptyDirs1yrUsers").getOrDefault(user, 0L));
    userMap.put("emptyDirsMem", getCachedMap("emptyDirsMemUsers").getOrDefault(user, 0L));
    userMap.put("emptyDirs24hMem", getCachedMap("emptyDirs24hMemUsers").getOrDefault(user, 0L));
    userMap.put("tinyFiles", getCachedMap("tinyFilesUsers").getOrDefault(user, 0L));
    userMap.put("tinyFiles24h", getCachedMap("tinyFiles24hUsers").getOrDefault(user, 0L));
    userMap.put("tinyFiles1yr", getCachedMap("tinyFiles1yrUsers").getOrDefault(user, 0L));
    userMap.put("tinyFilesMem", getCachedMap("tinyFilesMemUsers").getOrDefault(user, 0L));
    userMap.put("tinyFiles24hMem", getCachedMap("tinyFiles24hMemUsers").getOrDefault(user, 0L));
    userMap.put("tinyFilesDs", getCachedMap("tinyFilesDsUsers").getOrDefault(user, 0L));
    userMap.put("tinyFiles24hDs", getCachedMap("tinyFiles24hDsUsers").getOrDefault(user, 0L));
    userMap.put("smallFiles", getCachedMap("smallFilesUsers").getOrDefault(user, 0L));
    userMap.put("smallFiles24h", getCachedMap("smallFiles24hUsers").getOrDefault(user, 0L));
    userMap.put("smallFiles1yr", getCachedMap("smallFiles1yrUsers").getOrDefault(user, 0L));
    userMap.put("smallFilesMem", getCachedMap("smallFilesMemUsers").getOrDefault(user, 0L));
    userMap.put("smallFiles24hMem", getCachedMap("smallFiles24hMemUsers").getOrDefault(user, 0L));
    userMap.put("smallFilesDs", getCachedMap("smallFilesDsUsers").getOrDefault(user, 0L));
    userMap.put("smallFiles24hDs", getCachedMap("smallFiles24hDsUsers").getOrDefault(user, 0L));
    userMap.put("mediumFiles", getCachedMap("mediumFilesUsers").getOrDefault(user, 0L));
    userMap.put("largeFiles", getCachedMap("largeFilesUsers").getOrDefault(user, 0L));
    userMap.put("oldFiles1yr", getCachedMap("oldFiles1yrUsers").getOrDefault(user, 0L));
    userMap.put("oldFiles1yrDs", getCachedMap("oldFiles1yrDsUsers").getOrDefault(user, 0L));
    userMap.put("oldFiles2yr", getCachedMap("oldFiles2yrUsers").getOrDefault(user, 0L));
    userMap.put("oldFiles2yrDs", getCachedMap("oldFiles2yrDsUsers").getOrDefault(user, 0L));
    userMap.put("nsQuotaCount", getCachedMap("nsQuotaCountsUsers").getOrDefault(user, 0L));
    userMap.put("dsQuotaCount", getCachedMap("dsQuotaCountsUsers").getOrDefault(user, 0L));
    userMap.put(
        "nsQuotaThreshCount", getCachedMap("nsQuotaThreshCountsUsers").getOrDefault(user, 0L));
    userMap.put(
        "dsQuotaThreshCount", getCachedMap("dsQuotaThreshCountsUsers").getOrDefault(user, 0L));
    userMap.put("lastLogin", cachedLogins.getOrDefault(user, -1L));
    return userMap;
  }

  /**
   * Get directory analysis from cache as a JSON String.
   *
   * @param directory the dir path to look for
   * @param sum required; either count or diskspaceConsumed
   * @return the cached directory dump as a JSON string
   */
  public String getDirectoriesAsJson(String directory, String sum) {
    Map<String, Long> dirMap;
    switch (sum) {
      case "count":
        dirMap = getCachedMap("dirCount");
        break;
      case "diskspaceConsumed":
        dirMap = getCachedMap("dirDs");
        break;
      default:
        throw new IllegalArgumentException("Invalid sum type: " + sum);
    }
    if (directory != null && !directory.isEmpty()) {
      dirMap = Collections.singletonMap(directory, dirMap.get(directory));
    }
    return Histograms.toJson(dirMap);
  }

  /**
   * Get issues list from cache as a JSON String.
   *
   * @param limit the number of users to get for each issue
   * @param ascending whether to represent the top users or bottom users
   * @return the issue list as a JSON string
   */
  public String getIssuesAsJson(Integer limit, boolean ascending) {
    Map<String, Map<String, Long>> issuesMap = new LinkedHashMap<>();
    Map<String, Long> topEmptyFileUsers =
        Histograms.sortByValue(getCachedMap("emptyFilesUsers"), ascending);
    Map<String, Long> topEmptyDirUsers =
        Histograms.sortByValue(getCachedMap("emptyDirsUsers"), ascending);
    Map<String, Long> topTinyFilesUsers =
        Histograms.sortByValue(getCachedMap("tinyFilesUsers"), ascending);
    Map<String, Long> topSmallFilesUsers =
        Histograms.sortByValue(getCachedMap("smallFilesUsers"), ascending);
    Map<String, Long> topEmptyFile24hUsers =
        Histograms.sortByValue(getCachedMap("emptyFiles24hUsers"), ascending);
    Map<String, Long> topEmptyDir24hUsers =
        Histograms.sortByValue(getCachedMap("emptyDirs24hUsers"), ascending);
    Map<String, Long> topTinyFiles24hUsers =
        Histograms.sortByValue(getCachedMap("tinyFiles24hUsers"), ascending);
    Map<String, Long> topSmallFiles24hUsers =
        Histograms.sortByValue(getCachedMap("smallFiles24hUsers"), ascending);
    Map<String, Long> topOldFiles1yrUsers =
        Histograms.sortByValue(getCachedMap("oldFiles1yrUsers"), ascending);
    Map<String, Long> topDirCount = Histograms.sortByValue(getCachedMap("dirCount"), ascending);
    Map<String, Long> topDirDiskspace = Histograms.sortByValue(getCachedMap("dirDs"), ascending);
    Map<String, Long> topDirCount24h =
        Histograms.sortByValue(getCachedMap("dirCount24h"), ascending);
    Map<String, Long> topDirDiskspace24h =
        Histograms.sortByValue(getCachedMap("dirDs24h"), ascending);
    Function<Map<String, Long>, Map<String, Long>> sliceFunc =
        (histogramMap) ->
            (ascending
                ? Histograms.sliceToBottom(histogramMap, limit)
                : Histograms.sliceToTop(histogramMap, limit));
    issuesMap.put("emptyFiles", sliceFunc.apply(topEmptyFileUsers));
    issuesMap.put("emptyDirs", sliceFunc.apply(topEmptyDirUsers));
    issuesMap.put("tinyFiles", sliceFunc.apply(topTinyFilesUsers));
    issuesMap.put("smallFiles", sliceFunc.apply(topSmallFilesUsers));
    issuesMap.put("emptyFiles24h", sliceFunc.apply(topEmptyFile24hUsers));
    issuesMap.put("emptyDirs24h", sliceFunc.apply(topEmptyDir24hUsers));
    issuesMap.put("tinyFiles24h", sliceFunc.apply(topTinyFiles24hUsers));
    issuesMap.put("smallFiles24h", sliceFunc.apply(topSmallFiles24hUsers));
    issuesMap.put("oldFiles1yr", sliceFunc.apply(topOldFiles1yrUsers));
    issuesMap.put("dirCount", sliceFunc.apply(topDirCount));
    issuesMap.put("dirDiskspace", sliceFunc.apply(topDirDiskspace));
    issuesMap.put("dirCount24h", sliceFunc.apply(topDirCount24h));
    issuesMap.put("dirDiskspace24h", sliceFunc.apply(topDirDiskspace24h));
    return Histograms.toJson(issuesMap);
  }

  public void stop() {
    cacheManager.stop();
  }

  /**
   * Starts the SuggestionsEngine and cache'ing layer.
   *
   * @param conf the application configuration
   * @throws IOException an error occurred starting the cache engine
   */
  public void start(ApplicationConfiguration conf) throws IOException {
    cacheManager.start(conf);
    this.cachedDirectories.start(cacheManager);
    this.cachedUsers = Collections.synchronizedSet(cacheManager.getCachedSet("cachedUsers"));
    this.cachedValues = Collections.synchronizedMap(cacheManager.getCachedMap("cachedValues"));
    this.cachedLogins = Collections.synchronizedMap(cacheManager.getCachedMap("cachedLogins"));
    this.cachedMaps = Collections.synchronizedMap(cacheManager.getCachedMapToMap("cachedMaps"));
    this.cachedUserNsQuotas =
        Collections.synchronizedMap(cacheManager.getCachedMapToMap("cachedUserNsQuotas"));
    this.cachedUserDsQuotas =
        Collections.synchronizedMap(cacheManager.getCachedMapToMap("cachedUserDsQuotas"));
    this.cachedQueries =
        Collections.synchronizedMap(cacheManager.getCachedStringMap("cachedQueries"));
    this.cachedValueQueries =
        Collections.synchronizedMap(cacheManager.getCachedMap("cachedValueQueries"));
    this.cachedMapQueries =
        Collections.synchronizedMap(cacheManager.getCachedMapToMap("cachedMapQueries"));
    this.suggestionsReloadSleepMs = conf.getSuggestionsReloadSleepMs();
  }

  /**
   * Returns the set of all cached maps available for reporting.
   *
   * @return set of strings representing all cached maps available
   */
  public Set<String> getCachedMapKeys() {
    return cachedMaps.keySet();
  }
}
