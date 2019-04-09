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

import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class MapsView {

  static class JoinedMapView<E, K> implements Map<E, K> {

    private final Map<E, K> map1;
    private final Map<E, K> map2;

    public JoinedMapView(final Map<E, K> map1, final Map<E, K> map2) {
      this.map1 = map1;
      this.map2 = map2;
    }

    @Override
    public void clear() {
      map1.clear();
      map2.clear();
    }

    @NotNull
    @Override
    public Set<E> keySet() {
      return Sets.union(map1.keySet(), map2.keySet());
    }

    @SuppressWarnings("unchecked") /* We do unchecked casting to extract GSets */
    @NotNull
    @Override
    public Collection<K> values() {
      return CollectionsView.combine(map1.values(), map2.values());
    }

    @NotNull
    @Override
    public Set<Entry<E, K>> entrySet() {
      return Sets.union(map1.entrySet(), map2.entrySet());
    }

    @Override
    public boolean isEmpty() {
      return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
      return map1.containsKey(key) || map2.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
      return map1.containsValue(value) || map2.containsValue(value);
    }

    @Override
    public K get(Object key) {
      K k1 = map1.get(key);
      K k2 = map2.get(key);
      if (k1 != null && k2 != null) {
        throw new IllegalStateException("MapsView key collision.");
      }
      if (k1 != null) {
        return k1;
      }
      return k2;
    }

    @Nullable
    @Override
    public K put(E key, K value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public K remove(Object key) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(@NotNull Map<? extends E, ? extends K> m) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
      return map1.size() + map2.size();
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
  public static <E, K> Map<E, K> combine(final Map<E, K> map1, final Map<E, K> map2) {
    return new JoinedMapView<>(map1, map2);
  }

  private MapsView() {}
}
