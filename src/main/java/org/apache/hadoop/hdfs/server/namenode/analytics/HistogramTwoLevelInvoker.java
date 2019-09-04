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
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.QueryEngine;

/**
 * This class is meant to be used to invoke the appropriate histogram function based on input.
 * Primarily created to de-duplicate code.
 */
public class HistogramTwoLevelInvoker {

  private QueryEngine queryEngine;
  private String groupingOne;
  private String groupingTwo;
  private String sum;
  private Integer parentDirDepth;
  private String timeRange;
  private Stream<INode> filteredINodes;
  private Map<String, Map<String, Long>> histogram;
  private String binLabels;

  /**
   * Constructor.
   *
   * @param queryEngine the query engine
   * @param groupingOne the top level grouping field
   * @param groupingTwo the secondary grouping field
   * @param sum the sum aggregation field
   * @param parentDirDepth the parent dir depth
   * @param timeRange the time range used
   * @param filteredINodes the inode stream
   */
  public HistogramTwoLevelInvoker(
      QueryEngine queryEngine,
      String groupingOne,
      String groupingTwo,
      String sum,
      Integer parentDirDepth,
      String timeRange,
      Stream<INode> filteredINodes) {
    this.queryEngine = queryEngine;
    this.groupingOne = groupingOne;
    this.groupingTwo = groupingTwo;
    this.sum = sum;
    this.parentDirDepth = parentDirDepth;
    this.timeRange = timeRange;
    this.filteredINodes = filteredINodes;
  }

  public Map<String, Map<String, Long>> getHistogram() {
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
  public HistogramTwoLevelInvoker invoke() {
    histogram = callTwoLevelGroupingHistogram();
    switch (groupingOne) {
      case "user":
        binLabels = "User Names";
        break;
      case "group":
        binLabels = "Group Names";
        break;
      case "accessTime":
        //        histogram = Histograms.orderByKeyOrder(histogram, TimeHistogram.getKeys(timeRange));
        binLabels = "Last Accessed Time";
        break;
      case "modTime":
        //        histogram = Histograms.orderByKeyOrder(histogram, TimeHistogram.getKeys(timeRange));
        binLabels = "Last Modified Time";
        break;
      case "fileSize":
        binLabels = "File Sizes (No Replication Factor)";
        break;
      case "diskspaceConsumed":
        binLabels = "Diskspace Consumed (File Size * Replication Factor)";
        break;
      case "fileReplica":
        binLabels = "File Replication Factor";
        break;
      case "storageType":
        binLabels = "Storage Type Policy";
        break;
      case "memoryConsumed":
        binLabels = "Memory Consumed";
        break;
      case "parentDir":
        histogram.remove("NO_MAPPING");
        binLabels = "Directory Path";
        break;
      case "fileType":
        //        histogram = removeKeysOnConditional("gt:0", histogram);
        binLabels = "File Type";
        break;
      case "dirQuota":
        //        histogram = removeKeysOnConditional("gt:0", histogram);
        binLabels = "Directory Path";
        break;
      default:
        binLabels = "";
        break;
    }
    //    histogram = removeKeysOnConditional(histogramConditionsStr, histogram);
    //    histogram = sliceTopBottom(top, bottom, histogram);
    //    histogram = sortHistogramAscDesc(sortAscending, sortDescending, histogram);
    return this;
  }

  private Map<String, Map<String, Long>> callTwoLevelGroupingHistogram() {
    Function<INode, String> groupingFunc1 =
        queryEngine.getGroupingFunctionToStringForINode(groupingOne, parentDirDepth, timeRange);
    Function<INode, String> groupingFunc2 =
        queryEngine.getGroupingFunctionToStringForINode(groupingTwo, parentDirDepth, timeRange);
    if (groupingFunc1 == null) {
      throwIllegalHistogramException(groupingOne);
    }
    if (groupingFunc2 == null) {
      throwIllegalHistogramException(groupingTwo);
    }
    return queryEngine.genericTwoLevelHistogram(
        filteredINodes,
        groupingFunc1,
        groupingFunc2,
        queryEngine.getSumFunctionForINode(sum, null));
  }

  private void throwIllegalHistogramException(String group) {
    throw new IllegalArgumentException(
        "Could not determine histogram type: "
            + group
            + ".\nPlease check /histograms for available histograms.");
  }
}
