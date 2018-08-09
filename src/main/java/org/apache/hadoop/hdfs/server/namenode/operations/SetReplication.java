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

package org.apache.hadoop.hdfs.server.namenode.operations;

import java.util.Collection;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.namenode.Constants;
import org.apache.hadoop.hdfs.server.namenode.INode;

public class SetReplication extends BaseOperation {

  private final short newRepFactor;

  public SetReplication(
      Collection<INode> toSetRep,
      String query,
      String owner,
      String logBaseDir,
      FileSystem fs,
      short newRepFactor) {
    super(toSetRep, owner, query, logBaseDir, fs);
    this.newRepFactor = newRepFactor;
  }

  @Override
  public synchronized boolean performOp() {
    if (!hasNext()) {
      return false;
    }
    String path = nextToOperate.getFullPathName();
    LOG.info("About to setRep: {}", path);
    boolean file = nextToOperate.isFile();
    boolean success = true;
    String inodeType;
    /*
     * TODO:: SETREP WILL LOOK LIKE THIS:
     * try {
     *   success = fs.setReplication(new Path(path), newRepFactor);
     * } catch (IOException e) {
     *   success = false;
     * }
     */
    if (file) {
      /* TODO: Insert actual setRep code here. */
      LOG.info("SetRep'd file.");
      inodeType = "FILE";
    } else {
      LOG.info("INode type was not a file. Did not setRep.");
      return false;
    }
    synchronized (pathsOperated) {
      synchronized (toOperate) {
        log.logOp(path, inodeType, success);
        pathsOperated.add(path);
        iterator.remove();
      }
    }
    nextToOperate = iterator.hasNext() ? iterator.next() : null;
    return true;
  }

  @Override
  public String type() {
    return Constants.Operation.setReplication.name();
  }
}
