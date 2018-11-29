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

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.function.Function;
import org.apache.hadoop.fs.Path;

public abstract class AbstractQueryEngine implements QueryEngine {

  VersionInterface versionLoader;
  NameNodeLoader nameNodeLoader;

  @Override // QueryEngine
  public void setContexts(NameNodeLoader nameNodeLoader, VersionInterface versionLoader) {
    this.nameNodeLoader = nameNodeLoader;
    this.versionLoader = versionLoader;
  }

  @Override // QueryEngine
  public Collection<INode> getINodeSet(String set) {
    return nameNodeLoader.getINodeSetInternal(set);
  }

  /**
   * Get a Function to convert INode to a Long value.
   *
   * @param filter the filter to look for
   * @return the function representing the filter transform
   */
  @Override // QueryEngine
  public Function<INode, Long> getFilterFunctionToLongForINode(String filter) {
    switch (filter) {
      case "id":
        return INode::getId;
      case "fileSize":
        return node -> node.asFile().computeFileSize();
      case "diskspaceConsumed":
        return node -> node.asFile().computeFileSize() * node.asFile().getFileReplication();
      case "fileReplica":
        return node -> ((long) node.asFile().getFileReplication());
      case "blockSize":
        return node -> ((long) node.asFile().getPreferredBlockSize());
      case "numBlocks":
        return node -> ((long) node.asFile().numBlocks());
      case "numReplicas":
        return node -> ((long) node.asFile().numBlocks() * node.asFile().getFileReplication());
      case "accessTime":
        return INode::getAccessTime;
      case "modTime":
        return INode::getModificationTime;
      case "memoryConsumed":
        return node -> {
          long inodeSize = 100L;
          if (node.isFile()) {
            inodeSize += node.asFile().numBlocks() * 150L;
          }
          return inodeSize;
        };
      case "depth":
        return node -> {
          String path = node.getFullPathName();
          int depth = 0;
          int slash = path.length() == 1 && path.charAt(0) == '/' ? -1 : 0;
          while (slash != -1) {
            depth++;
            slash = path.indexOf(Path.SEPARATOR, slash + 1);
          }
          return (long) depth;
        };
      case "permission":
        return node -> Long.valueOf(Integer.toOctalString(node.getFsPermissionShort()));
      default:
        return versionLoader.getFilterFunctionToLongForINode(filter);
    }
  }

  /**
   * Get a Function to convert INode to a String value.
   *
   * @param filter the filter to look for
   * @return the function representing the filter transform
   */
  @Override // QueryEngine
  public Function<INode, String> getFilterFunctionToStringForINode(String filter) {
    switch (filter) {
      case "name":
        return INode::getLocalName;
      case "path":
        return INode::getFullPathName;
      case "user":
        return INode::getUserName;
      case "group":
        return INode::getGroupName;
      case "modDate":
        return n -> {
          SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
          try {
            Date date = new Date(n.getModificationTime());
            return sdf.format(date);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };
      case "accessDate":
        return n -> {
          SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
          try {
            Date date = new Date(n.getAccessTime());
            return sdf.format(date);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };
      default:
        return null;
    }
  }

  /**
   * Get a Function to convert INode to a Boolean value.
   *
   * @param filter the filter to look for
   * @return the function representing the filter transform
   */
  @Override // QueryEngine
  public Function<INode, Boolean> getFilterFunctionToBooleanForINode(String filter) {
    switch (filter) {
      case "isUnderConstruction":
        return node -> node.asFile().isUnderConstruction();
      case "isWithSnapshot":
        return node -> node.asFile().isWithSnapshot();
      case "hasAcl":
        return node -> (node.getAclFeature() != null);
      default:
        return versionLoader.getFilterFunctionToBooleanForINode(filter);
    }
  }

  /**
   * Get a Function that converts an INode to a single Long for summation. Used mostly by Histogram
   * functions.
   *
   * @param sum the sum to look for
   * @return the function representing the sum transform
   */
  @Override // QueryEngine
  public Function<INode, Long> getSumFunctionForINode(String sum) {
    switch (sum) {
      case "count":
        return node -> 1L;
      case "fileSize":
        return node -> node.asFile().computeFileSize();
      case "diskspaceConsumed":
        return node -> node.asFile().computeFileSize() * node.asFile().getFileReplication();
      case "blockSize":
        return node -> node.asFile().getPreferredBlockSize();
      case "numBlocks":
        return node -> ((long) node.asFile().numBlocks());
      case "numReplicas":
        return node -> ((long) node.asFile().numBlocks() * node.asFile().getFileReplication());
      case "memoryConsumed":
        return node -> {
          long inodeSize = 100L;
          if (node.isFile()) {
            inodeSize += node.asFile().numBlocks() * 150L;
          }
          return inodeSize;
        };
      case "nsQuotaRatioUsed":
        return node ->
            (long)
                ((double) versionLoader.getNsQuotaUsed(node)
                    / (double) versionLoader.getNsQuota(node)
                    * 100);
      case "dsQuotaRatioUsed":
        return node ->
            (long)
                ((double) versionLoader.getDsQuotaUsed(node)
                    / (double) versionLoader.getDsQuota(node)
                    * 100);
      case "nsQuotaUsed":
        return versionLoader::getNsQuotaUsed;
      case "dsQuotaUsed":
        return versionLoader::getDsQuotaUsed;
      case "nsQuota":
        return versionLoader::getNsQuota;
      case "dsQuota":
        return versionLoader::getDsQuota;
      default:
        throw new IllegalArgumentException(
            "Could not determine sum type: " + sum + ".\nPlease check /sums for available sums.");
    }
  }
}
