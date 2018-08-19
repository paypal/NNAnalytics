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

import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.Iterator;

public final class CollectionsView {

  static class JoinedCollectionView<E> implements Collection<E> {

    private final Collection<? extends E>[] items;

    public JoinedCollectionView(final Collection<? extends E>[] items) {
      this.items = items;
    }

    @Override
    public boolean addAll(final Collection<? extends E> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      for (final Collection<? extends E> coll : items) {
        coll.clear();
      }
    }

    @Override
    public boolean contains(final Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
      return !iterator().hasNext();
    }

    @Override
    public Iterator<E> iterator() {
      return Iterables.concat(items).iterator();
    }

    @Override
    public boolean remove(final Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
      int ct = 0;
      for (final Collection<? extends E> coll : items) {
        ct += coll.size();
      }
      return ct;
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(E e) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Returns a live aggregated collection view of the collections passed in.
   *
   * <p>All methods except {@link Collection#size()}, {@link Collection#clear()}, {@link
   * Collection#isEmpty()} and {@link Iterable#iterator()} throw {@link
   * UnsupportedOperationException} in the returned Collection.
   *
   * <p>None of the above methods is thread safe (nor would there be an easy way of making them).
   */
  public static <T> Collection<T> combine(final Collection<? extends T>... items) {
    return new JoinedCollectionView<>(items);
  }

  private CollectionsView() {}
}
