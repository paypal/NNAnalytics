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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hdfs.server.namenode.analytics.ApplicationConfiguration;
import org.apache.hadoop.util.GSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface QueryEngine {

  Logger LOG = LoggerFactory.getLogger(QueryEngine.class.getName());

  void setVersionContext(VersionInterface versionLoader);

  void handleGSet(
      GSet<INode, INodeWithAdditionalFields> gset,
      ApplicationConfiguration nnaConf,
      FSNamesystem namesystem)
      throws Exception;

  Collection<INode> getINodeSet(String set);

  Collection<INode> combinedFilter(Collection<INode> inodes, String[] filters, String[] filterOps);

  Stream<INode> combinedFilterToStream(
      Collection<INode> inodes, String[] filters, String[] filterOps);

  Collection<INode> findFilter(Collection<INode> inodes, String find);

  Long sum(Collection<INode> inodes, String sum);

  Function<INode, String> getGroupingFunctionToStringForINode(
      String filter, Integer parentDirDepth, String timeRange);

  Function<INode, String> getCustomLongToStringGroupingFunction(
      Function<INode, Long> toLongFunction, Function<Long, String> longToStringFunction);

  Function<INode, Long> getFilterFunctionToLongForINode(String filter);

  Function<INode, String> getFilterFunctionToStringForINode(String filter);

  Function<INode, Boolean> getFilterFunctionToBooleanForINode(String filter);

  Function<String, Boolean> getFilterFunctionForString(String value, String op);

  Function<Boolean, Boolean> getFilterFunctionForBoolean(Boolean value, String op);

  Function<Long, Boolean> getFilterFunctionForLong(Long value, String op);

  void dumpINodePaths(Collection<INode> inodes, Integer limit, HttpServletResponse resp)
      throws IOException;

  Map<String, Long> genericSumOrFindHistogram(
      Stream<INode> inodes,
      Function<INode, String> namingFunction,
      ToLongFunction<INode> dataFunction,
      String find);

  Map<String, Long> genericSummingHistogram(
      Stream<INode> inodes,
      Function<INode, String> namingFunction,
      Function<INode, Long> dataFunction);

  Map<String, LongSummaryStatistics> genericSummarizingHistogram(
      Stream<INode> inodes,
      Function<INode, String> namingFunction,
      Function<INode, Long> dataFunction);

  Map<String, Map<String, Long>> genericTwoLevelHistogram(
      Stream<INode> inodes,
      Function<INode, String> namingFunction1,
      Function<INode, String> namingFunction2,
      Function<INode, Long> dataFunction);

  Map<String, Long> diskspaceConsumedHistogram(
      Stream<INode> inodes,
      String sum,
      String find,
      Map<String, Function<INode, Long>> transformMap);

  Map<String, Long> memoryConsumedHistogram(Stream<INode> inodes, String sum, String find);

  Map<String, Long> fileSizeHistogram(Stream<INode> inodes, String sum, String find);

  Map<String, Long> fileReplicaHistogram(
      Stream<INode> inodes,
      String sum,
      String find,
      Map<String, Function<INode, Long>> transformMap);

  Map<String, Long> storageTypeHistogram(Stream<INode> inodes, String sum, String find);

  Map<String, Long> accessTimeHistogram(
      Stream<INode> inodes, String sum, String find, String timeRange);

  Map<String, Long> modTimeHistogram(
      Stream<INode> inodes, String sum, String find, String timeRange);

  Map<String, Long> byUserHistogram(Stream<INode> inodes, String sum, String find);

  Map<String, Long> byGroupHistogram(Stream<INode> inodes, String sum, String find);

  Map<String, Long> parentDirHistogram(
      Stream<INode> inodes, Integer parentDirDepth, String sum, String find);

  Map<String, Long> fileTypeHistogram(Stream<INode> inodes, String sum, String find);

  Map<String, Long> dirQuotaHistogram(Stream<INode> inodes, String sum);

  Function<INode, Long> getSumFunctionForINode(
      String sum, Map<String, Function<INode, Long>> transformMap);

  Map<String, Long> removeKeysOnConditional(
      Map<String, Long> histogram, String histogramConditionsStr);

  Map<String, List<Long>> removeKeysOnConditional2(
      Map<String, List<Long>> histogram, String histogramConditionsStr);

  List<Function<Long, Boolean>> createComparisons(String conditionsStr);

  boolean check(List<Function<Long, Boolean>> comparisons, long value);

  void clear();
}
