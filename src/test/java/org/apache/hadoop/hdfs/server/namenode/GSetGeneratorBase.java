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

import static org.apache.hadoop.util.Time.now;

import java.io.IOException;
import java.util.function.Function;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.LightWeightGSet;
import org.slf4j.Logger;

public abstract class GSetGeneratorBase {

  public static final Logger LOG = NameNodeLoader.LOG;

  protected static final FsPermission permission = FsPermission.getDefault();
  protected static final PermissionStatus status =
      PermissionStatus.createImmutable("hdfs", "hdfs", permission);
  protected static final long now = now();
  protected static final SRandom rand = new SRandom();

  protected final INodeDirectory root =
      new INodeDirectory(0, INodeDirectory.ROOT_NAME, status, now);
  protected GSet<INode, INodeWithAdditionalFields> gset;

  protected static final int DEFAULT_BLOCK_SIZE = (int) DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
  protected static final int DEFAULT_NUM_FILES = 100;
  protected static final int DEFAULT_NUM_DIRS = 10;
  protected static final short DEFAULT_DEPTH = 3;

  public static int ID = 1;
  public static int FILES_MADE = 0;
  public static int DIRS_MADE = 0;
  public static Function<Void, Integer> TOTAL_MADE = (i -> FILES_MADE + DIRS_MADE);

  public static GSet<INode, INodeWithAdditionalFields> getEmptyGSet() {
    return new LightWeightGSet<>(LightWeightGSet.computeCapacity(1, "test"));
  }

  public GSet<INode, INodeWithAdditionalFields> getGSet() {
    return getGSet(DEFAULT_DEPTH, DEFAULT_NUM_DIRS, DEFAULT_NUM_FILES);
  }

  public void clear() {
    if (gset != null) {
      gset.clear();
      gset = null;
    }
    ID = 1;
    FILES_MADE = 0;
    DIRS_MADE = 0;
  }

  public GSet<INode, INodeWithAdditionalFields> getGSet(
      short depth, int numDirsPerDepth, int numFilesPerDir) {
    if (gset == null || gset.size() == 0) {
      try {
        gset = getEmptyGSet();
        gset.put(root);
        DIRS_MADE++;
        generateGSet(gset, root, numFilesPerDir, numDirsPerDepth, depth);
        LOG.info("Generated GSet size is: " + gset.size());
        assert FILES_MADE + DIRS_MADE == gset.size();
      } catch (IOException ignored) {
        /* Defaults not expected to throw Exceptions */
      }
    }
    return gset;
  }

  protected abstract void generateGSet(
      GSet<INode, INodeWithAdditionalFields> newGSet,
      INodeDirectory parent,
      int filesPerDir,
      int numOfDirsPerDepth,
      short depth)
      throws IOException;

  protected abstract void generateFilesForDirectory(
      GSet<INode, INodeWithAdditionalFields> newGSet, INodeDirectory parent, int filesToCreate)
      throws IOException;
}
