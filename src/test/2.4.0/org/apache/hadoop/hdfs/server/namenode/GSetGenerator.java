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

import static org.apache.hadoop.hdfs.server.namenode.NNAConstants.CHARSET;
import static org.apache.hadoop.util.Time.now;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;
import org.slf4j.Logger;

public class GSetGenerator {

  public static final Logger LOG = NNLoader.LOG;

  private static final FsPermission permission = FsPermission.getDefault();
  private static final PermissionStatus status =
      PermissionStatus.createImmutable("hdfs", "hdfs", permission);
  private static final long now = now();
  private static final SRandom rand = new SRandom();

  private static final INodeDirectory root =
      new INodeDirectory(0, "/root".getBytes(CHARSET), status, now);
  private static GSet<INode, INodeWithAdditionalFields> gset;

  private static final int DEFAULT_BLOCK_SIZE = (int) DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
  private static final int DEFAULT_NUM_FILES = 100;
  private static final int DEFAULT_NUM_DIRS = 10;
  private static final short DEFAULT_DEPTH = 3;

  public static int ID = 1;
  public static int FILES_MADE = 0;
  public static int DIRS_MADE = 0;
  public static Function<Void, Integer> TOTAL_MADE = (i -> FILES_MADE + DIRS_MADE);

  public static GSet<INode, INodeWithAdditionalFields> getEmptyGSet() {
    return new LightWeightGSet<>(LightWeightGSet.computeCapacity(1, "test"));
  }

  public static GSet<INode, INodeWithAdditionalFields> getGSet() {
    return getGSet(DEFAULT_DEPTH, DEFAULT_NUM_DIRS, DEFAULT_NUM_FILES);
  }

  public static void clear() {
    if (gset != null) {
      gset.clear();
      gset = null;
    }
    ID = 1;
    FILES_MADE = 0;
    DIRS_MADE = 0;
  }

  public static GSet<INode, INodeWithAdditionalFields> getGSet(
      short depth, int numDirsPerDepth, int numFilesPerDir) {
    if (gset == null || gset.size() == 0) {
      try {
        gset = getEmptyGSet();
        gset.put(root);
        DIRS_MADE++;
        generateGSet(gset, root, numFilesPerDir, numDirsPerDepth, depth);
        LOG.info("Generated GSet size is: {}", gset.size());
        assert FILES_MADE + DIRS_MADE == gset.size();
      } catch (IOException ignored) {
        /* Defaults not expected to throw Exceptions */
      }
    }
    return gset;
  }

  private static void generateGSet(
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

  private static void generateFilesForDirectory(
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
