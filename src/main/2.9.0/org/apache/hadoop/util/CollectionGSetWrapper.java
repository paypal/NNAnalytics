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

import com.googlecode.cqengine.IndexedCollection;
import java.util.Collection;
import java.util.Iterator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.jetbrains.annotations.NotNull;

public class CollectionGSetWrapper implements GSet<INode, INodeWithAdditionalFields> {

  private final IndexedCollection<INode> wrapped;
  private final GSet<INode, INodeWithAdditionalFields> backing;

  public CollectionGSetWrapper(
      IndexedCollection<INode> wrapped, GSet<INode, INodeWithAdditionalFields> backing) {
    this.wrapped = wrapped;
    this.backing = backing;
  }

  @Override
  public int size() {
    return backing.size();
  }

  @Override
  public boolean contains(INode element) {
    return backing.contains(element);
  }

  @Override
  public INodeWithAdditionalFields get(INode element) {
    return backing.get(element);
  }

  @Override
  public INodeWithAdditionalFields put(INodeWithAdditionalFields element) {
    if (backing.contains(element)) {
      INodeWithAdditionalFields prev = backing.get(element);
      wrapped.add(element);
      return prev;
    } else {
      wrapped.add(element);
      return null;
    }
  }

  @Override
  public INodeWithAdditionalFields remove(INode element) {
    if (backing.contains(element)) {
      INodeWithAdditionalFields prev = backing.get(element);
      wrapped.remove(element);
      return prev;
    }
    return null;
  }

  @Override
  public void clear() {
    wrapped.clear();
  }

  @Override
  public Collection<INodeWithAdditionalFields> values() {
    return backing.values();
  }

  @NotNull
  @Override
  public Iterator<INodeWithAdditionalFields> iterator() {
    return backing.iterator();
  }
}
