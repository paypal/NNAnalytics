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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import javax.servlet.http.HttpServletResponse;

public interface VersionInterface {

  void setNamesystem(FSNamesystem namesystem);

  void dumpINodeInDetail(String path, HttpServletResponse resp) throws IOException;

  Function<INode, Long> getFilterFunctionToLongForINode(String filter);

  Function<INode, Boolean> getFilterFunctionToBooleanForINode(String filter);

  Map<String, Long> storageTypeHistogramCpu(
      Collection<INode> inodes, String sum, QueryEngine queryEngine);

  Map<String, Long> storageTypeHistogramCpuWithFind(
      Collection<INode> inodes, String find, QueryEngine queryEngine);

  void saveNamespace() throws IOException;

  void saveLegacyOivImage(String dir) throws IOException;

  Long getNsQuota(INode node);

  Long getNsQuotaUsed(INode node);

  Long getDsQuota(INode node);

  Long getDsQuotaUsed(INode node);
}
