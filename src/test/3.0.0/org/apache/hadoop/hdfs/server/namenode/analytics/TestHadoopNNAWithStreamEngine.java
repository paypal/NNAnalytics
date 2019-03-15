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

package org.apache.hadoop.hdfs.server.namenode.analytics;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.GSetGenerator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.JavaStreamQueryEngine;
import org.apache.hadoop.util.GSet;
import org.apache.http.HttpHost;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestHadoopNNAWithStreamEngine extends TestNNAnalyticsBase {

  /** Long running execution that will launch an NNA instance with a non-updating namespace. */
  public static void main(String[] args) throws Exception {
    beforeClass();
    while (true) {
      // Let the server run.
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    GSetGenerator gSetGenerator = new GSetGenerator();
    gSetGenerator.clear();
    GSet<INode, INodeWithAdditionalFields> gset = gSetGenerator.getGSet((short) 3, 10, 500);
    nna = new HadoopWebServerMain();
    ApplicationConfiguration conf = new ApplicationConfiguration();
    conf.set("ldap.enable", "false");
    conf.set("authorization.enable", "false");
    conf.set("nna.historical", "false");
    conf.set("nna.base.dir", MiniDFSCluster.getBaseDirectory());
    conf.set("nna.query.engine.impl", JavaStreamQueryEngine.class.getCanonicalName());
    nna.init(conf, gset);
    hostPort = new HttpHost("localhost", 4567);
  }
}
