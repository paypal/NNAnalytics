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
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.hadoop.hdfs.server.namenode.Constants.Histogram;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.QueryEngine;
import org.apache.hadoop.hdfs.server.namenode.queries.Histograms;
import org.apache.hadoop.hdfs.server.namenode.queries.TimeHistogram;

/**
 * This class is meant to be used to invoke the appropriate histogram function based on input.
 * Primarily created to de-duplicate code.
 */
public class HistogramInvoker {

  private QueryEngine queryEngine;
  private String histType;
  private String sum;
  private Integer parentDirDepth;
  private String timeRange;
  private String find;
  private Stream<INode> filteredINodes;
  private Histogram histogramType;
  private Map<String, Function<INode, Long>> transformMap;
  private String histogramConditionsStr;
  private Integer top;
  private Integer bottom;
  private Boolean sortAscending;
  private Boolean sortDescending;
  private Map<String, Long> histogram;
  private String binLabels;

  /**
   * Constructor.
   *
   * @param queryEngine the query engine
   * @param histType the type of grouping being done
   * @param sum the sum aggregation field
   * @param parentDirDepth the parent dir depth
   * @param timeRange the time range used
   * @param find the min/max/avg aggregation field
   * @param filteredINodes the inode stream
   * @param transformMap (optional) a transform map
   * @param histogramConditionsStr string dictating removal conditions from results
   * @param top top results to keep by value
   * @param bottom bottom results to keep by value
   * @param sortAscending should sort ascending
   * @param sortDescending should sort descending
   */
  public HistogramInvoker(
      QueryEngine queryEngine,
      String histType,
      String sum,
      Integer parentDirDepth,
      String timeRange,
      String find,
      Stream<INode> filteredINodes,
      Map<String, Function<INode, Long>> transformMap,
      String histogramConditionsStr,
      Integer top,
      Integer bottom,
      Boolean sortAscending,
      Boolean sortDescending) {
    this.queryEngine = queryEngine;
    this.histType = histType;
    this.sum = sum;
    this.parentDirDepth = parentDirDepth;
    this.timeRange = timeRange;
    this.find = find;
    this.filteredINodes = filteredINodes;
    this.histogramType = Histogram.valueOf(histType);
    this.transformMap = transformMap;
    this.histogramConditionsStr = histogramConditionsStr;
    this.top = top;
    this.bottom = bottom;
    this.sortAscending = sortAscending;
    this.sortDescending = sortDescending;
  }

  /**
   * More basic constructor for specific basic histograms.
   *
   * @param queryEngine the query engine
   * @param histType the type of grouping being done
   * @param sum the sum aggregation field
   * @param find the min/max/avg aggregation field
   * @param filteredINodes the inode stream
   */
  public HistogramInvoker(
      QueryEngine queryEngine,
      String histType,
      String sum,
      String find,
      Stream<INode> filteredINodes) {
    this.queryEngine = queryEngine;
    this.histType = histType;
    this.sum = sum;
    this.find = find;
    this.filteredINodes = filteredINodes;
    this.histogramType = Histogram.valueOf(histType);
  }

  public Map<String, Long> getHistogram() {
    return histogram;
  }

  public String getBinLabels() {
    return binLabels;
  }

  /**
   * Performs the histogram function and stores data within this same object.
   *
   * @return the current object with histogram and labelling complete
   */
  public HistogramInvoker invoke() {
    switch (histogramType) {
      case user:
        histogram = callSingelGroupingHistogram("user");
        binLabels = "User Names";
        break;
      case group:
        histogram = callSingelGroupingHistogram("group");
        binLabels = "Group Names";
        break;
      case accessTime:
        histogram = callSingelGroupingHistogram("accessTime");
        histogram = Histograms.orderByKeyOrder(histogram, TimeHistogram.getKeys(timeRange));
        binLabels = "Last Accessed Time";
        break;
      case modTime:
        histogram = callSingelGroupingHistogram("modTime");
        histogram = Histograms.orderByKeyOrder(histogram, TimeHistogram.getKeys(timeRange));
        binLabels = "Last Modified Time";
        break;
      case fileSize:
        histogram = callSingelGroupingHistogram("fileSize");
        binLabels = "File Sizes (No Replication Factor)";
        break;
      case diskspaceConsumed:
        histogram = callSingelGroupingHistogram("diskspaceConsumed");
        binLabels = "Diskspace Consumed (File Size * Replication Factor)";
        break;
      case fileReplica:
        histogram = callSingelGroupingHistogram("fileReplica");
        binLabels = "File Replication Factor";
        break;
      case storageType:
        histogram = callSingelGroupingHistogram("storageType");
        binLabels = "Storage Type Policy";
        break;
      case memoryConsumed:
        histogram = callSingelGroupingHistogram("memoryConsumed");
        binLabels = "Memory Consumed";
        break;
      case parentDir:
        histogram = callSingelGroupingHistogram("parentDir");
        histogram.remove("NO_MAPPING");
        binLabels = "Directory Path";
        break;
      case fileType:
        histogram = callSingelGroupingHistogram("fileType");
        histogram = removeKeysOnConditional("gt:0", histogram);
        binLabels = "File Type";
        break;
      case dirQuota:
        histogram = callSingelGroupingHistogram("dirQuota");
        histogram = removeKeysOnConditional("gt:0", histogram);
        binLabels = "Directory Path";
        break;
      default:
        throw new IllegalArgumentException(
            "Could not determine histogram type: "
                + histType
                + ".\nPlease check /histograms for available histograms.");
    }
    histogram = removeKeysOnConditional(histogramConditionsStr, histogram);
    histogram = sliceTopBottom(top, bottom, histogram);
    histogram = sortHistogramAscDesc(sortAscending, sortDescending, histogram);
    return this;
  }

  private Map<String, Long> callSingelGroupingHistogram(String grouping) {
    return queryEngine.genericSumOrFindHistogram(
        filteredINodes,
        queryEngine.getGroupingFunctionToStringForINode(grouping, parentDirDepth, timeRange),
        Helper.convertToLongFunction(queryEngine.getSumFunctionForINode(sum, transformMap)),
        find);
  }

  private Map<String, Long> sortHistogramAscDesc(
      Boolean sortAscending, Boolean sortDescending, Map<String, Long> histogram) {
    if (sortAscending != null && sortDescending != null) {
      throw new IllegalArgumentException("Please choose one type of sort.");
    } else if (sortAscending != null && sortAscending) {
      return Histograms.sortByValue(histogram, true);
    } else if (sortDescending != null && sortDescending) {
      return Histograms.sortByValue(histogram, false);
    }
    return histogram;
  }

  private Map<String, Long> removeKeysOnConditional(
      String histogramConditionsStr, Map<String, Long> histogram) {
    if (histogramConditionsStr != null && !histogramConditionsStr.isEmpty()) {
      return queryEngine.removeKeysOnConditional(histogram, histogramConditionsStr);
    }
    return histogram;
  }

  private Map<String, Long> sliceTopBottom(
      Integer top, Integer bottom, Map<String, Long> histogram) {
    if (top != null && bottom != null) {
      throw new IllegalArgumentException("Please choose only one type of slice.");
    } else if (top != null && top > 0) {
      return Histograms.sliceToTop(histogram, top);
    } else if (bottom != null && bottom > 0) {
      return Histograms.sliceToBottom(histogram, bottom);
    }
    return histogram;
  }
}
