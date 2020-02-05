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
import org.apache.hadoop.hdfs.server.namenode.ConcurrentHashMapINodeCollection;
import org.apache.hadoop.hdfs.server.namenode.EclipseINodeCollection;
import org.apache.hadoop.hdfs.server.namenode.GSetGenerator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.JavaStreamQueryEngine;
import org.apache.hadoop.hdfs.server.namenode.NonBlockingHashMapINodeCollection;
import org.apache.hadoop.util.GSet;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFiltererLoading {

  private static GSet<INode, INodeWithAdditionalFields> gset;

  private ApplicationMain nna;
  private ApplicationConfiguration conf;

  @BeforeClass
  public static void setUp() {
    GSetGenerator gSetGenerator = new GSetGenerator();
    gSetGenerator.clear();
    gset = gSetGenerator.getGSet((short) 3, 10, 500);
  }

  @Before
  public void before() {
    nna = new WebServerMain();
    conf = new ApplicationConfiguration();
    conf.set("ldap.enable", "false");
    conf.set("authorization.enable", "false");
    conf.set("nna.historical", "false");
    conf.set("nna.base.dir", MiniDFSCluster.getBaseDirectory());
    conf.set("nna.web.base.dir", "src/main/resources/webapps/nna");
    conf.set("nna.query.engine.impl", JavaStreamQueryEngine.class.getCanonicalName());
  }

  @After
  public void after() {
    if (nna != null) {
      nna.shutdown();
    }
  }

  @Test
  public void testLoadConcurrentHashMapFilterer() throws Exception {
    conf.set(
        "nna.inode.collection.impl", ConcurrentHashMapINodeCollection.class.getCanonicalName());
    nna.init(conf, gset);
  }

  @Test
  public void testLoadHashMapFilterer() throws Exception {
    conf.set(
        "nna.inode.collection.impl", NonBlockingHashMapINodeCollection.class.getCanonicalName());
    nna.init(conf, gset);
  }

  @Test
  public void testLoadEclipseFilterer() throws Exception {
    conf.set("nna.inode.collection.impl", EclipseINodeCollection.class.getCanonicalName());
    nna.init(conf, gset);
  }
}
