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
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.queries.Histograms;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.io.IOUtils;

public class VersionContext implements VersionInterface {

  private FSNamesystem namesystem;

  @Override // VersionInterface
  public void setNamesystem(FSNamesystem namesystem) {
    this.namesystem = namesystem;
  }

  @Override // VersionInterface
  public void dumpINodeInDetail(String path, HttpServletResponse resp) throws IOException {
    PrintWriter writer = resp.getWriter();
    try {
      if (namesystem == null) {
        writer.write("Namesystem is not fully initialized.\n");
        writer.flush();
        return;
      }
      Map<String, Object> nodeDetails = new TreeMap<>();
      INode node = namesystem.getFSDirectory().getINode(path);
      nodeDetails.put("path", node.getFullPathName());
      nodeDetails.put("permisssions", node.getPermissionStatus().toString());
      nodeDetails.put("accessTime", new Date(node.getAccessTime()));
      nodeDetails.put("modTime", new Date(node.getModificationTime()));
      nodeDetails.put("nodeId", node.getId());
      nodeDetails.put(
          "storagePolicy",
          BlockStoragePolicySuite.createDefaultSuite().getPolicy(node.getStoragePolicyID()));
      nodeDetails.put("nsQuota", node.getQuotaCounts().getNameSpace());
      nodeDetails.put("dsQuota", node.getQuotaCounts().getStorageSpace());
      XAttrFeature xattrs = node.getXAttrFeature();
      nodeDetails.put("xAttrs", ((xattrs == null) ? "NONE" : xattrs.getXAttrs()));
      AclFeature aclFeature = node.getAclFeature();
      nodeDetails.put("aclsCount", ((aclFeature == null) ? "NONE" : aclFeature.getEntriesSize()));
      if (node.isFile()) {
        nodeDetails.put("type", "file");
        INodeFile file = node.asFile();
        nodeDetails.put("underConstruction", file.isUnderConstruction());
        nodeDetails.put("isWithSnapshot", file.isWithSnapshot());
        nodeDetails.put("fileSize", file.computeFileSize());
        nodeDetails.put("replicationFactor", file.getFileReplication());
        nodeDetails.put("numBlocks", file.getBlocks().length);
        nodeDetails.put(
            "blocks",
            Arrays.stream(file.getBlocks())
                .map(k -> k.getBlockName() + "_" + k.getGenerationStamp() + " " + k.getNumBytes())
                .collect(Collectors.toList()));
      } else {
        nodeDetails.put("type", "directory");
        INodeDirectory dir = node.asDirectory();
        nodeDetails.put("snapshottable", dir.isSnapshottable());
        nodeDetails.put("isWithSnapshot", dir.isWithSnapshot());
        nodeDetails.put(
            "children",
            StreamSupport.stream(
                    dir.getChildrenList(Snapshot.CURRENT_STATE_ID).spliterator(), false)
                .map(INode::getLocalName)
                .collect(Collectors.toList()));
      }
      String json = Histograms.toJson(nodeDetails);
      writer.write(json);
      writer.flush();
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  @Override // VersionInterface
  public Function<INode, Long> getFilterFunctionToLongForINode(String filter) {
    switch (filter) {
      case "diskspaceConsumed":
        return node -> {
          byte storagePolicyId = node.getStoragePolicyID();
          return node.asFile()
              .storagespaceConsumed(
                  BlockStoragePolicySuite.createDefaultSuite().getPolicy(storagePolicyId))
              .getStorageSpace();
        };
      case "dirNumChildren":
        return x -> ((long) x.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID).size());
      case "dirSubTreeSize":
        return x -> {
          FSDirectory fsd = namesystem.dir;
          ContentSummaryComputationContext cscc =
              new ContentSummaryComputationContext(
                  fsd,
                  fsd.getFSNamesystem(),
                  fsd.getContentCountLimit(),
                  fsd.getContentSleepMicroSec());
          return x.computeContentSummary(Snapshot.CURRENT_STATE_ID, cscc)
              .getCounts()
              .getStoragespace();
        };
      case "dirSubTreeNumFiles":
        return x -> {
          FSDirectory fsd = namesystem.dir;
          ContentSummaryComputationContext cscc =
              new ContentSummaryComputationContext(
                  fsd,
                  fsd.getFSNamesystem(),
                  fsd.getContentCountLimit(),
                  fsd.getContentSleepMicroSec());
          return x.computeContentSummary(Snapshot.CURRENT_STATE_ID, cscc)
              .getCounts()
              .getFileCount();
        };
      case "dirSubTreeNumDirs":
        return x -> {
          FSDirectory fsd = namesystem.dir;
          ContentSummaryComputationContext cscc =
              new ContentSummaryComputationContext(
                  fsd,
                  fsd.getFSNamesystem(),
                  fsd.getContentCountLimit(),
                  fsd.getContentSleepMicroSec());
          return x.computeContentSummary(Snapshot.CURRENT_STATE_ID, cscc)
              .getCounts()
              .getDirectoryCount();
        };
      case "storageType":
        return x -> ((long) x.getStoragePolicyID());
      default:
        return null;
    }
  }

  @Override // VersionInterface
  public Function<INode, Boolean> getFilterFunctionToBooleanForINode(String filter) {
    switch (filter) {
      case "hasQuota":
        return node -> node.asDirectory().isWithQuota();
      case "hasEcPolicy":
        return node -> false;
      default:
        return null;
    }
  }

  @Override
  public Function<INode, String> getGroupingFunctionToStringForINode(String grouping) {
    switch (grouping) {
      case "fileReplica":
        return node -> String.valueOf(node.asFile().getFileReplication());
      default:
        return null;
    }
  }

  @Override // VersionInterface
  public void saveNamespace() throws IOException {
    namesystem.saveNamespace();
  }

  @Override // VersionInterface
  public void saveLegacyOivImage(String dir) throws IOException {
    namesystem.getFSImage().saveLegacyOIVImage(namesystem, dir, new Canceler());
  }

  @Override // VersionInterface
  public Long getNsQuota(INode node) {
    return node.getQuotaCounts().getNameSpace();
  }

  @Override // VersionInterface
  public Long getNsQuotaUsed(INode node) {
    return node.computeQuotaUsage(BlockStoragePolicySuite.createDefaultSuite()).getNameSpace();
  }

  @Override // VersionInterface
  public Long getDsQuota(INode node) {
    return node.getQuotaCounts().getStorageSpace();
  }

  @Override // VersionInterface
  public Long getDsQuotaUsed(INode node) {
    return node.computeQuotaUsage(BlockStoragePolicySuite.createDefaultSuite()).getStorageSpace();
  }

  @Override // VersionInterface
  public void startStandbyServices(Configuration conf) throws IOException {
    namesystem.startStandbyServices(conf);
  }
}
