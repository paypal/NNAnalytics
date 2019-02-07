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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.JavaCollectionQEngine;
import org.apache.hadoop.hdfs.server.namenode.analytics.security.SecurityConfiguration;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestWithMiniClusterWithCQEngine extends TestWithMiniClusterBase {

  /**
   * Long running execution that will launch an NNA instance backed by a live updating HA-enabled
   * HDFS instance. NNA will update every minute or so with new analysis.
   */
  public static void main(String[] args) throws Exception {
    beforeClass();
    TestWithMiniClusterWithCQEngine test = new TestWithMiniClusterWithCQEngine();
    test.addFiles(10000, 300L);
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    RANDOM.nextBytes(TINY_FILE_BYTES);
    RANDOM.nextBytes(SMALL_FILE_BYTES);
    RANDOM.nextBytes(MEDIUM_FILE_BYTES);

    // Speed up editlog tailing.
    CONF.setInt(DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY, 1);
    CONF.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, 1);
    CONF.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);
    CONF.setInt(DFSConfigKeys.DFS_CLIENT_RETRY_WINDOW_BASE, 10);
    CONF.setBoolean("fs.hdfs.impl.disable.cache", true);
    CONF.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "hdfs://" + NAMESERVICE);

    MiniQJMHACluster.Builder qjmBuilder = new MiniQJMHACluster.Builder(CONF);
    qjmBuilder.getDfsBuilder().numDataNodes(NUMDATANODES);
    cluster = qjmBuilder.build();
    cluster.getDfsCluster().waitActive();
    cluster.getDfsCluster().transitionToActive(0);

    HATestUtil.setFailoverConfigurations(cluster.getDfsCluster(), CONF, NAMESERVICE, 0);
    CONF.set("dfs.nameservice.id", NAMESERVICE);

    nna = new WebServerMain();
    SecurityConfiguration nnaConf = new SecurityConfiguration();
    nnaConf.set("nna.support.bootstrap.overrides", "true");
    nnaConf.set("ldap.enable", "false");
    nnaConf.set("authorization.enable", "false");
    nnaConf.set("nna.historical", "true");
    nnaConf.set("nna.base.dir", MiniDFSCluster.getBaseDirectory());
    nnaConf.set("nna.query.engine.impl", JavaCollectionQEngine.class.getCanonicalName());
    nna.init(nnaConf, null, CONF);
    hostPort = new HttpHost("localhost", 4567);
    client = new DefaultHttpClient();

    // Fetch NNA Namespace.
    HttpGet fetch = new HttpGet("http://localhost:4567/fetchNamespace");
    HttpResponse fetchRes = client.execute(hostPort, fetch);
    assertThat(fetchRes.getStatusLine().getStatusCode(), is(200));
    IOUtils.readLines(fetchRes.getEntity().getContent());

    // Reload NNA Namespace.
    HttpGet reload = new HttpGet("http://localhost:4567/reloadNamespace");
    HttpResponse reloadRes = client.execute(hostPort, reload);
    assertThat(reloadRes.getStatusLine().getStatusCode(), is(200));
    IOUtils.readLines(reloadRes.getEntity().getContent());
  }
}
