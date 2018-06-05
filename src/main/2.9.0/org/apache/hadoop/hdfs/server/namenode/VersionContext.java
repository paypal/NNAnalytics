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
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;
import org.apache.hadoop.hdfs.server.namenode.queries.StorageTypeHistogram;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.Canceler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;

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
      INode node = namesystem.getFSDirectory().getINode(path);
      writer.write("Full Path: " + node.getFullPathName() + "\n");
      writer.write("Permissions: " + node.getPermissionStatus().toString() + "\n");
      writer.write("Access Time: " + new Date(node.getAccessTime()) + "\n");
      writer.write("Mod Time: " + new Date(node.getModificationTime()) + "\n");
      writer.write("ID: " + node.getId() + "\n");
      writer.write(
          "Storage Policy: "
              + BlockStoragePolicySuite.createDefaultSuite().getPolicy(node.getStoragePolicyID())
              + "\n");
      writer.write("Parent: " + node.getParentString() + "\n");
      writer.write("Namespace Quota: " + node.getQuotaCounts().getNameSpace() + "\n");
      writer.write("Diskspace Quota: " + node.getQuotaCounts().getStorageSpace() + "\n");
      XAttrFeature xattrs = node.getXAttrFeature();
      writer.write("XAttrs: " + ((xattrs == null) ? "NONE" : xattrs.getXAttrs()) + "\n");
      AclFeature aclFeature = node.getAclFeature();
      writer.write(
          "ACLs (size): " + ((aclFeature == null) ? "NONE" : aclFeature.getEntriesSize()) + "\n");
      if (node.isFile()) {
        INodeFile file = node.asFile();
        writer.write("Under Construction?: " + file.isUnderConstruction() + "\n");
        writer.write("Under Snapshot?: " + file.isWithSnapshot() + "\n");
        writer.write("File Size: " + file.computeFileSize() + "\n");
        writer.write(
            "File Size w/o UC Block: " + file.computeFileSizeNotIncludingLastUcBlock() + "\n");
        writer.write("Replication Factor: " + file.getFileReplication() + "\n");
        writer.write("Number of Blocks: " + file.getBlocks().length + "\n");
        writer.write(
            "Blocks:\n"
                + Arrays.stream(file.getBlocks())
                    .map(
                        k ->
                            k.getBlockName()
                                + "_"
                                + k.getGenerationStamp()
                                + " "
                                + k.getNumBytes()
                                + "\n")
                    .collect(Collectors.toList()));
      } else {
        INodeDirectory dir = node.asDirectory();
        writer.write("Has Quotas?: " + dir.isWithQuota() + "\n");
        writer.write("Is Snapshottable?: " + dir.isSnapshottable() + "\n");
        writer.write("Under Snapshot?: " + dir.isWithSnapshot() + "\n");
        writer.write("Number of Children: " + dir.getChildrenNum(Snapshot.CURRENT_STATE_ID) + "\n");
        writer.write(
            "Children:\n"
                + StreamSupport.stream(
                        dir.getChildrenList(Snapshot.CURRENT_STATE_ID).spliterator(), false)
                    .map(child -> child.getFullPathName() + "\n")
                    .collect(Collectors.toList()));
        writer.flush();
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  @Override // VersionInterface
  public Function<INode, Long> getFilterFunctionToLongForINode(String filter) {
    switch (filter) {
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
          try {
            return x.computeContentSummary(Snapshot.CURRENT_STATE_ID, cscc)
                .getCounts()
                .getStoragespace();
          } catch (AccessControlException e) {
            throw new RuntimeException(e);
          }
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
          try {
            return x.computeContentSummary(Snapshot.CURRENT_STATE_ID, cscc)
                .getCounts()
                .getFileCount();
          } catch (AccessControlException e) {
            throw new RuntimeException(e);
          }
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
          try {
            return x.computeContentSummary(Snapshot.CURRENT_STATE_ID, cscc)
                .getCounts()
                .getDirectoryCount();
          } catch (AccessControlException e) {
            throw new RuntimeException(e);
          }
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
      default:
        return null;
    }
  }

  @Override // VersionInterface
  public Map<String, Long> storageTypeHistogramCpu(
      Collection<INode> inodes, String sum, NNLoader nnLoader) {
    List<Long> distinctStorageIds = StorageTypeHistogram.bins;
    List<String> distinctStorageKeys = StorageTypeHistogram.keys;
    Map<String, Long> storageIdToIndexToKeyMap =
        distinctStorageIds
            .parallelStream()
            .mapToInt(distinctStorageIds::indexOf)
            .boxed()
            .collect(Collectors.toMap(distinctStorageKeys::get, k -> (long) k));

    return nnLoader.binMappingHistogram(
        inodes,
        sum,
        nnLoader.getSumFunctionForINode(sum),
        node -> (long) distinctStorageIds.indexOf((long) node.getStoragePolicyID()),
        storageIdToIndexToKeyMap);
  }

  @Override // VersionInterface
  public Map<String, Long> storageTypeHistogramCpuWithFind(
      Collection<INode> inodes, String find, NNLoader nnLoader) {
    List<Long> distinctStorageIds = StorageTypeHistogram.bins;
    List<String> distinctStorageKeys = StorageTypeHistogram.keys;
    Map<String, Long> storageIdToIndexToKeyMap =
        distinctStorageIds
            .parallelStream()
            .mapToInt(distinctStorageIds::indexOf)
            .boxed()
            .collect(Collectors.toMap(distinctStorageKeys::get, k -> (long) k));
    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];

    return nnLoader.binMappingHistogramWithFind(
        inodes,
        findOp,
        nnLoader.getFilterFunctionToLongForINode(findField),
        node -> (long) distinctStorageIds.indexOf((long) node.getStoragePolicyID()),
        storageIdToIndexToKeyMap);
  }

  @Override // VersionInterface
  public void saveNamespace() throws IOException {
    namesystem.saveNamespace();
  }

  @Override // VersionInterface
  public void saveLegacyOIVImage(String dir) throws IOException {
    namesystem.getFSImage().saveLegacyOIVImage(namesystem, dir, new Canceler());
  }

  @Override // VersionInterface
  public Long getNSQuota(INode node) {
    return node.getQuotaCounts().getNameSpace();
  }

  @Override // VersionInterface
  public Long getNSQuotaUsed(INode node) {
    return node.computeQuotaUsage(BlockStoragePolicySuite.createDefaultSuite()).getNameSpace();
  }

  @Override // VersionInterface
  public Long getDSQuota(INode node) {
    return node.getQuotaCounts().getStorageSpace();
  }

  @Override // VersionInterface
  public Long getDSQuotaUsed(INode node) {
    return node.computeQuotaUsage(BlockStoragePolicySuite.createDefaultSuite()).getStorageSpace();
  }
}
