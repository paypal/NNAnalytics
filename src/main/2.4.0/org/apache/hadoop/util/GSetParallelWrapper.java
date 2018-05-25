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

public class GSetParallelWrapper<K, E extends K> implements GSet<K, E> {

  private final GSet<K, E> innerSet;

  public GSetParallelWrapper(GSet<K, E> set) {
    innerSet = set;
  }

  @Override
  public int size() {
    return innerSet.size();
  }

  @Override
  public boolean contains(K key) {
    return innerSet.contains(key);
  }

  @Override
  public E get(K key) {
    return innerSet.get(key);
  }

  @Override
  public E put(E element) {
    return innerSet.put(element);
  }

  @Override
  public E remove(K key) {
    return innerSet.remove(key);
  }

  @Override
  public void clear() {
    innerSet.clear();
  }

  @Override
  public Iterator<E> iterator() {
    Iterator<E> iterator = innerSet.iterator();
    if (iterator instanceof LightWeightGSet.SetIterator) {
      ((LightWeightGSet.SetIterator) iterator).setTrackModification(false);
    }
    return iterator;
  }
}
