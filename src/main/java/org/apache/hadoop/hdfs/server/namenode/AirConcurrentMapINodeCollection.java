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

package org.apache.hadoop.hdfs.server.namenode;

import com.infinitydb.map.air.AirConcurrentMap;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.hadoop.util.GSet;

/**
 * Filters the INode GSet into java.util.ConcurrentHashMap sets. This has great performance in
 * memory space vs lookup speed.
 */
public class AirConcurrentMapINodeCollection implements INodeFilterer {

  @Override
  public Map<INode, INodeWithAdditionalFields> filterFiles(
      GSet<INode, INodeWithAdditionalFields> gset) {
    AirConcurrentMap<INode, INodeWithAdditionalFields> acmMap =
        new AirConcurrentMap<>(Comparator.comparing(INode::getId));
    StreamSupport.stream(gset.spliterator(), true)
        .filter(INode::isFile)
        .forEach(node -> acmMap.put(node, node));
    return acmMap;
  }

  @Override
  public Map<INode, INodeWithAdditionalFields> filterDirs(
      GSet<INode, INodeWithAdditionalFields> gset) {
    AirConcurrentMap<INode, INodeWithAdditionalFields> acmMap =
        new AirConcurrentMap<>(Comparator.comparing(INode::getId));
    StreamSupport.stream(gset.spliterator(), true)
        .filter(INode::isDirectory)
        .forEach(node -> acmMap.put(node, node));
    return acmMap;
  }
}
