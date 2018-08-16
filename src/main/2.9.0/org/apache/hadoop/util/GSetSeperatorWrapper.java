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

import com.google.common.collect.Iterators;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.jetbrains.annotations.NotNull;

public class GSetSeperatorWrapper implements GSet<INode, INodeWithAdditionalFields> {

  private final Map<INode, INodeWithAdditionalFields> fileSet;
  private final Map<INode, INodeWithAdditionalFields> dirSet;

  /**
   * Constructor.
   *
   * @param files mapping of inode files to maintain
   * @param dirs mapping of inode dirs to maintain
   */
  public GSetSeperatorWrapper(
      Map<INode, INodeWithAdditionalFields> files, Map<INode, INodeWithAdditionalFields> dirs) {
    this.fileSet = files;
    this.dirSet = dirs;
  }

  @Override
  public int size() {
    return fileSet.size() + dirSet.size();
  }

  @Override
  public boolean contains(INode key) {
    return fileSet.containsKey(key) || dirSet.containsKey(key);
  }

  @Override
  public INodeWithAdditionalFields get(INode key) {
    INodeWithAdditionalFields val;
    val = fileSet.get(key);
    if (val != null) {
      return val;
    }
    return dirSet.get(key);
  }

  @Override
  public INodeWithAdditionalFields put(INodeWithAdditionalFields element) {
    if (element.isFile()) {
      return fileSet.put(element, element);
    }
    return dirSet.put(element, element);
  }

  @Override
  public INodeWithAdditionalFields remove(INode key) {
    INodeWithAdditionalFields removed;
    removed = fileSet.remove(key);
    if (removed != null) {
      return removed;
    }
    return dirSet.remove(key);
  }

  @Override
  public void clear() {
    fileSet.clear();
    dirSet.clear();
  }

  @Override
  public Collection<INodeWithAdditionalFields> values() {
    return CollectionsView.combine(fileSet.values(), dirSet.values());
  }

  @NotNull
  @Override
  public Iterator<INodeWithAdditionalFields> iterator() {
    return Iterators.concat(fileSet.values().iterator(), dirSet.values().iterator());
  }
}
