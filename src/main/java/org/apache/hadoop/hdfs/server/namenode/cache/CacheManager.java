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

package org.apache.hadoop.hdfs.server.namenode.cache;

import java.util.Map;
import java.util.Set;
import org.apache.hadoop.util.MapSerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

/**
 * This class is simply a wrapper around MapDB classes for easier storage and possible future
 * abstraction if something better is discovered.
 *
 * <p>Any memory objects returned from this class are backed by mmap'd files and fsync'd upon
 * calling commit.
 */
public class CacheManager {

  private DB cache;

  public CacheManager() {}

  public Map<String, Map<String, Long>> getCachedMapToMap(String mapToMapName) {
    return cache.hashMap(mapToMapName, Serializer.STRING, new MapSerializer()).createOrOpen();
  }

  public Map<String, Long> getCachedMap(String mapName) {
    return cache.hashMap(mapName, Serializer.STRING, Serializer.LONG).createOrOpen();
  }

  public Set<String> getCachedSet(String setName) {
    return cache.hashSet(setName, Serializer.STRING).createOrOpen();
  }

  public void commit() {
    cache.commit();
  }

  public void stop() {
    cache.close();
  }

  public void start() {
    cache =
        DBMaker.fileDB("/usr/local/nn-analytics/db/nna_cache")
            .fileMmapEnable()
            .transactionEnable()
            .closeOnJvmShutdown()
            .cleanerHackEnable()
            .make();
  }
}
