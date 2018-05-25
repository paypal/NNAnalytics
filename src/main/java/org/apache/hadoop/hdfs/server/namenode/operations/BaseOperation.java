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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseOperation implements Operation {

  public static final Logger LOG =
      LoggerFactory.getLogger(BaseOperation.class.getName());

  final OperationLog log;
  final Collection<INode> toOperate;
  final List<String> pathsOperated;
  final int totalToOperate;
  final String identity;
  final String owner;
  final String query;
  final FileSystem fs;
  final Iterator<INode> iterator;
  INode nextToOperate;

  BaseOperation(Collection<INode> toPerform,
      String owner,
      String query,
      FileSystem fs) {
    this.pathsOperated = new ArrayList<>();
    this.iterator = toPerform.iterator();
    this.nextToOperate = iterator.hasNext() ? iterator.next() : null;
    this.toOperate = toPerform;
    this.owner = owner;
    this.identity = UUID.randomUUID().toString();
    this.totalToOperate = toPerform.size();
    this.query = query;
    final boolean gzipLog = toPerform.size() >= (100 * 1000);
    this.log = new OperationLog(identity, query, owner, gzipLog);
    this.fs = fs;
  }

  @Override
  public int hashCode() {
    return identity.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj != this) {
      return false;
    }
    if (obj.hashCode() != this.hashCode()) {
      return false;
    }
    if (obj instanceof Delete) {
      BaseOperation that = (BaseOperation) obj;
      if (!that.identity.equals(this.identity)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public synchronized void initialize() {
    log.startLog();
  }

  @Override
  public synchronized void close() {
    IOUtils.closeQuietly(fs);
    log.close();
  }

  @Override
  public synchronized List<String> lastPerformed(int numOfLast) {
    int lastIndex = pathsOperated.size();
    if (numOfLast >= lastIndex) {
      return pathsOperated;
    }
    return new ArrayList<>(pathsOperated.subList(lastIndex - numOfLast, lastIndex));
  }

  @Override
  public synchronized String upNext() {
    if (nextToOperate == null) {
      return "ABORTED";
    }
    return nextToOperate.getFullPathName();
  }

  @Override
  public abstract boolean performOp();

  @Override
  public synchronized boolean hasNext() {
    return nextToOperate != null;
  }

  @Override
  public synchronized void abort() {
    nextToOperate = null;
    if (fs != null) {
      IOUtils.closeQuietly(fs);
    }
    if (log != null) {
      log.close();
    }
  }

  @Override
  public String identity() {
    return identity;
  }

  @Override
  public int totalToPerform() {
    return totalToOperate;
  }

  @Override
  public int numPerformed() {
    return pathsOperated.size();
  }

  @Override
  public String query() {
    return query;
  }

  @Override
  public String owner() {
    return owner;
  }

  public abstract String type();
}
