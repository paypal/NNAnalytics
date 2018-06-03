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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringContains.containsString;

import com.paypal.namenode.NNAnalyticsRestAPI;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.NNLoader;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import spark.Spark;

@RunWith(JUnit4.class)
public class TestWithMiniCluster {

  private static final int NUMDATANODES = 1;
  private static final Configuration CONF = new HdfsConfiguration();
  private static final String NAMESERVICE = MiniQJMHACluster.NAMESERVICE;

  private static MiniQJMHACluster cluster;
  private static HttpHost hostPort;
  private static HttpClient client;
  private static NNLoader nn;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // disable block scanner
    CONF.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1);
    // Set short retry timeouts so this test runs faster
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

    NNAnalyticsRestAPI nna = new NNAnalyticsRestAPI();
    nna.initAuth(false, false);
    nna.initRestServer();
    nn = nna.initLoader(null, null, CONF);
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

  @AfterClass
  public static void tearDown() throws IOException {
    nn.clear();
    cluster.shutdown();
    Spark.stop();
  }

  @Before
  public void before() throws IOException {
    client = new DefaultHttpClient();
  }

  public static void main(String[] args) throws Exception {
    beforeClass();
    while (true) {
      // Let the server run.
    }
  }

  @Test
  public void testInfo() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/info");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
    assertThat(IOUtils.toString(res.getEntity().getContent()), containsString("INode GSet size: "));
  }

  @Ignore("Test is just for showcasing")
  @Test
  public void testAddFiles() throws IOException, InterruptedException {
    for (int i = 0; i < 90000000; i++) {
      DFSTestUtil.writeFile(FileSystem.get(CONF), new Path("/file" + i), "abc");
      Thread.sleep(1000L);
    }
  }
}
