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

package org.apache.hadoop.hdfs.server.namenode.queries;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;

public class StorageTypeHistogram {

  /* We can lower the number of days to improve performance on multi-filter bin'ing. */
  private static final BlockStoragePolicy[] keys0 =
      BlockStoragePolicySuite.createDefaultSuite().getAllPolicies();
  public static final List<String> keys =
      Collections.unmodifiableList(
          Arrays.stream(keys0).map(BlockStoragePolicy::getName).collect(Collectors.toList()));
  public static final List<Long> bins =
      Collections.unmodifiableList(
          Arrays.stream(keys0).map(k -> ((long) k.getId())).collect(Collectors.toList()));
}
