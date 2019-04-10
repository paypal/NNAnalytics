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

import java.util.AbstractCollection;
import java.util.Iterator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.jetbrains.annotations.NotNull;

public class GSetCollectionWrapper extends AbstractCollection<INode> {

  private final GSet<INode, INodeWithAdditionalFields> gset;

  public GSetCollectionWrapper(GSet<INode, INodeWithAdditionalFields> gset) {
    this.gset = gset;
  }

  @Override // AbstractCollection
  public int size() {
    return gset.size();
  }

  @Override // AbstractCollection
  public boolean contains(Object o) {
    if (o instanceof INode) {
      return gset.contains((INode) o);
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  @NotNull
  @Override // AbstractCollection
  public Iterator<INode> iterator() {
    Iterator<? extends INode> iterator = gset.iterator();
    return (Iterator<INode>) iterator;
  }

  @Override // AbstractCollection
  public boolean add(INode element) {
    if (element instanceof INodeWithAdditionalFields) {
      gset.put((INodeWithAdditionalFields) element);
      return true;
    } else {
      return false;
    }
  }

  @Override // AbstractCollection
  public boolean remove(Object o) {
    if (o instanceof INode) {
      INodeWithAdditionalFields removed = gset.remove((INode) o);
      return (removed != null);
    }
    return false;
  }

  @Override // AbstractCollection
  public void clear() {
    gset.clear();
  }

  public GSet<INode, INodeWithAdditionalFields> getBackingSet() {
    return gset;
  }
}
