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

import static org.apache.hadoop.hdfs.protocol.HdfsConstants.ALLSSD_STORAGE_POLICY_ID;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.COLD_STORAGE_POLICY_ID;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.HOT_STORAGE_POLICY_ID;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.MEMORY_STORAGE_POLICY_ID;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.ONESSD_STORAGE_POLICY_ID;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.WARM_STORAGE_POLICY_ID;
import static org.apache.hadoop.hdfs.server.namenode.Constants.CHARSET;
import static org.apache.hadoop.util.Time.now;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.util.GSet;

public class GSetGenerator extends GSetGeneratorBase {

  private static final byte[] policyIDs =
      new byte[] {
        MEMORY_STORAGE_POLICY_ID,
        ALLSSD_STORAGE_POLICY_ID,
        ONESSD_STORAGE_POLICY_ID,
        HOT_STORAGE_POLICY_ID,
        WARM_STORAGE_POLICY_ID,
        COLD_STORAGE_POLICY_ID,
        0x00
      };

  public void generateGSet(
      GSet<INode, INodeWithAdditionalFields> newGSet,
      INodeDirectory parent,
      int filesPerDir,
      int numOfDirsPerDepth,
      short depth)
      throws IOException {
    if (depth == 0) {
      return;
    }
    List<INodeDirectory> childrenDirs = new ArrayList<>();
    for (int j = 1; j <= numOfDirsPerDepth; j++) {
      long mAndAtime = now - rand.nextLong(TimeUnit.DAYS.toMillis(365)); // 1 year
      int dirId = ID++;
      INodeDirectory dir =
          new INodeDirectory(dirId, ("dir" + j).getBytes(CHARSET), status, mAndAtime);
      boolean childAdded = parent.addChild(dir);
      if (rand.nextBoolean()) {
        dir.setQuota(9000L, 9999999999L);
        dir.getDirectoryWithQuotaFeature()
            .setSpaceConsumed(rand.nextLong(5000L), rand.nextLong(9999999991L));
      }
      if (childAdded) {
        generateFilesForDirectory(gset, dir, filesPerDir);
        dir.setParent(parent);
        INodeWithAdditionalFields booted = newGSet.put(dir);
        if (booted == null) {
          DIRS_MADE++;
        }
      }
      childrenDirs.add(dir);
    }
    for (INodeDirectory childrenDir : childrenDirs) {
      generateGSet(newGSet, childrenDir, filesPerDir, numOfDirsPerDepth, (short) (depth - 1));
    }
  }

  public void generateFilesForDirectory(
      GSet<INode, INodeWithAdditionalFields> newGSet, INodeDirectory parent, int filesToCreate)
      throws IOException {
    for (int i = 1; i <= filesToCreate; i++) {
      int fileId = ID++;
      long mAndAtime = now - rand.nextLong(TimeUnit.DAYS.toMillis(365)); // 1 year
      short replFact = (rand.nextBoolean()) ? (short) 3 : (short) (rand.nextInt(10) + 1);
      int numOfBlocks = rand.nextInt(4);
      BlockInfo[] blks = new BlockInfo[numOfBlocks];
      for (int k = 0; k < numOfBlocks; k++) {
        boolean isLastBlock = (k + 1 >= numOfBlocks);
        int blockSize = (isLastBlock) ? rand.nextInt(DEFAULT_BLOCK_SIZE) + 1 : DEFAULT_BLOCK_SIZE;
        Block blk =
            new Block(fileId * 10 + k, blockSize, GenerationStamp.GRANDFATHER_GENERATION_STAMP);
        blks[k] = new BlockInfo(blk, replFact);
      }
      INodeFile file =
          new INodeFile(
              fileId,
              ("file" + i).getBytes(CHARSET),
              status,
              mAndAtime,
              mAndAtime,
              blks,
              replFact,
              DEFAULT_BLOCK_SIZE);
      byte policyID = policyIDs[rand.nextInt(policyIDs.length)];
      file.setStoragePolicyID(policyID, Snapshot.CURRENT_STATE_ID);
      for (BlockInfo blk : blks) {
        blk.setBlockCollection(file);
      }
      boolean added = parent.addChild(file);
      if (added) {
        file.setParent(parent);
        INodeWithAdditionalFields booted = newGSet.put(file);
        if (booted == null) {
          FILES_MADE++;
        }
      }
    }
  }
}
