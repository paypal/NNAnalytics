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
import org.apache.hadoop.hdfs.server.namenode.NameNodeLoader;

/**
 * This class is meant to be used to invoke the appropriate histogram function based on input.
 * Primarily created to de-duplicate code.
 */
public class HistogramInvoker {

  private NameNodeLoader nameNodeLoader;
  private String histType;
  private String sum;
  private Integer parentDirDepth;
  private String timeRange;
  private String find;
  private Stream<INode> filteredINodes;
  private Histogram htEnum;
  private Map<String, Function<INode, Long>> transformMap;
  private Map<String, Long> histogram;
  private String binLabels;

  /**
   * Constructor.
   *
   * @param nameNodeLoader the namenode loader
   * @param histType the type of grouping being done
   * @param sum the sum aggregation field
   * @param parentDirDepth the parent dir depth
   * @param timeRange the time range used
   * @param find the min/max/avg aggregation field
   * @param filteredINodes the inode stream
   * @param transformMap (optional) a transform map
   */
  public HistogramInvoker(
      NameNodeLoader nameNodeLoader,
      String histType,
      String sum,
      Integer parentDirDepth,
      String timeRange,
      String find,
      Stream<INode> filteredINodes,
      Map<String, Function<INode, Long>> transformMap) {
    this.nameNodeLoader = nameNodeLoader;
    this.histType = histType;
    this.sum = sum;
    this.parentDirDepth = parentDirDepth;
    this.timeRange = timeRange;
    this.find = find;
    this.filteredINodes = filteredINodes;
    this.htEnum = Histogram.valueOf(histType);
    this.transformMap = transformMap;
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
    switch (htEnum) {
      case user:
        histogram = nameNodeLoader.getQueryEngine().byUserHistogram(filteredINodes, sum, find);
        binLabels = "User Names";
        break;
      case group:
        histogram = nameNodeLoader.getQueryEngine().byGroupHistogram(filteredINodes, sum, find);
        binLabels = "Group Names";
        break;
      case accessTime:
        histogram =
            nameNodeLoader
                .getQueryEngine()
                .accessTimeHistogram(filteredINodes, sum, find, timeRange);
        binLabels = "Last Accessed Time";
        break;
      case modTime:
        histogram =
            nameNodeLoader.getQueryEngine().modTimeHistogram(filteredINodes, sum, find, timeRange);
        binLabels = "Last Modified Time";
        break;
      case fileSize:
        histogram = nameNodeLoader.getQueryEngine().fileSizeHistogram(filteredINodes, sum, find);
        binLabels = "File Sizes (No Replication Factor)";
        break;
      case diskspaceConsumed:
        histogram =
            nameNodeLoader
                .getQueryEngine()
                .diskspaceConsumedHistogram(filteredINodes, sum, find, transformMap);
        binLabels = "Diskspace Consumed (File Size * Replication Factor)";
        break;
      case fileReplica:
        histogram =
            nameNodeLoader
                .getQueryEngine()
                .fileReplicaHistogram(filteredINodes, sum, find, transformMap);
        binLabels = "File Replication Factor";
        break;
      case storageType:
        histogram = nameNodeLoader.getQueryEngine().storageTypeHistogram(filteredINodes, sum, find);
        binLabels = "Storage Type Policy";
        break;
      case memoryConsumed:
        histogram =
            nameNodeLoader.getQueryEngine().memoryConsumedHistogram(filteredINodes, sum, find);
        binLabels = "Memory Consumed";
        break;
      case parentDir:
        histogram =
            nameNodeLoader
                .getQueryEngine()
                .parentDirHistogram(filteredINodes, parentDirDepth, sum, find);
        binLabels = "Directory Path";
        break;
      case fileType:
        histogram = nameNodeLoader.getQueryEngine().fileTypeHistogram(filteredINodes, sum, find);
        binLabels = "File Type";
        break;
      case dirQuota:
        histogram = nameNodeLoader.getQueryEngine().dirQuotaHistogram(filteredINodes, sum);
        binLabels = "Directory Path";
        break;
      default:
        throw new IllegalArgumentException(
            "Could not determine histogram type: "
                + histType
                + ".\nPlease check /histograms for available histograms.");
    }
    return this;
  }
}
