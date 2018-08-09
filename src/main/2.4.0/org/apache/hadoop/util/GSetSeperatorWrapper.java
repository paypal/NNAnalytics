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

package org.apache.hadoop.util;

import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;

public class GSetSeperatorWrapper implements GSet<INode, INodeWithAdditionalFields> {

  private final GSet<INode, INodeWithAdditionalFields> inodes;
  private final Map<INode, INode> fileSet;
  private final Map<INode, INode> dirSet;

  /**
   * Constructor.
   *
   * @param inodes original inode set
   * @param files mapping of inode files to maintain
   * @param dirs mapping of inode dirs to maintain
   */
  public GSetSeperatorWrapper(
      GSet<INode, INodeWithAdditionalFields> inodes,
      Map<INode, INode> files,
      Map<INode, INode> dirs) {
    this.inodes = inodes;
    this.fileSet = files;
    this.dirSet = dirs;
  }

  @Override
  public int size() {
    return inodes.size();
  }

  @Override
  public boolean contains(INode key) {
    return inodes.contains(key);
  }

  @Override
  public INodeWithAdditionalFields get(INode key) {
    return inodes.get(key);
  }

  @Override
  public INodeWithAdditionalFields put(INodeWithAdditionalFields element) {
    if (element.isFile()) {
      fileSet.put(element, element);
    } else if (element.isDirectory()) {
      dirSet.put(element, element);
    }
    return inodes.put(element);
  }

  @Override
  public INodeWithAdditionalFields remove(INode key) {
    if (key.isFile()) {
      fileSet.remove(key);
    } else if (key.isDirectory()) {
      dirSet.remove(key);
    }
    return inodes.remove(key);
  }

  @Override
  public void clear() {
    inodes.clear();
    fileSet.clear();
    dirSet.clear();
  }

  @Override
  public Iterator<INodeWithAdditionalFields> iterator() {
    Iterator<INodeWithAdditionalFields> iterator = inodes.iterator();
    if (iterator instanceof LightWeightGSet.SetIterator) {
      ((LightWeightGSet.SetIterator) iterator).setTrackModification(false);
    }
    return iterator;
  }
}
