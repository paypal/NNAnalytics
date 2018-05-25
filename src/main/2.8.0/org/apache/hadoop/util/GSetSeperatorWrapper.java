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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;

public class GSetSeperatorWrapper implements GSet<INode, INodeWithAdditionalFields> {

  private final GSet<INode, INodeWithAdditionalFields> gSet;
  private final Map<INode, INode> fileSet;
  private final Map<INode, INode> dirSet;


  public GSetSeperatorWrapper(GSet<INode, INodeWithAdditionalFields> g,
      Map<INode, INode> files,
      Map<INode, INode> dirs) {
    this.gSet = g;
    this.fileSet = files;
    this.dirSet = dirs;
  }

  @Override
  public int size() {
    return gSet.size();
  }

  @Override
  public boolean contains(INode key) {
    return gSet.contains(key);
  }

  @Override
  public INodeWithAdditionalFields get(INode key) {
    return gSet.get(key);
  }

  @Override
  public INodeWithAdditionalFields put(INodeWithAdditionalFields element) {
    if (element.isFile()) {
      fileSet.put(element, element);
    } else if (element.isDirectory()) {
      dirSet.put(element, element);
    }
    return gSet.put(element);
  }

  @Override
  public INodeWithAdditionalFields remove(INode key) {
    if (key.isFile()) {
      fileSet.remove(key);
    } else if (key.isDirectory()) {
      dirSet.remove(key);
    }
    return gSet.remove(key);
  }

  @Override
  public void clear() {
    gSet.clear();
    fileSet.clear();
    dirSet.clear();
  }

  @Override
  public Collection<INodeWithAdditionalFields> values() {
    return gSet.values();
  }

  @Override
  public Iterator<INodeWithAdditionalFields> iterator() {
    Iterator<INodeWithAdditionalFields> iterator = gSet.iterator();
    if (iterator instanceof LightWeightGSet.SetIterator) {
      ((LightWeightGSet.SetIterator) iterator).setTrackModification(false);
    }
    return iterator;
  }
}
