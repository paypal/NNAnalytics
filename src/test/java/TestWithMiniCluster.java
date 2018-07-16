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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringContains.containsString;

import com.paypal.namenode.NNAnalyticsRestAPI;
import com.paypal.security.SecurityConfiguration;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.hdfs.server.namenode.SRandom;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestWithMiniCluster {

  private static final SRandom RANDOM = new SRandom();
  private static final int NUMDATANODES = 1;
  private static final Configuration CONF = new HdfsConfiguration();
  private static final String NAMESERVICE = MiniQJMHACluster.NAMESERVICE;

  private static final byte[] TINY_FILE_BYTES = new byte[512];
  private static final byte[] SMALL_FILE_BYTES = new byte[1024];
  private static final byte[] MEDIUM_FILE_BYTES = new byte[1024 * 1024];
  private static final String[] USERS = new String[] {"hdfs", "test_user"};

  private static MiniQJMHACluster cluster;
  private static HttpHost hostPort;
  private static HttpClient client;
  private static NNAnalyticsRestAPI nna;

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

    nna = new NNAnalyticsRestAPI();
    SecurityConfiguration nnaConf = new SecurityConfiguration();
    nnaConf.set("ldap.enable", "false");
    nnaConf.set("authorization.enable", "false");
    nnaConf.set("nna.historical", "true");

    // Create temporary DB directory.
    String baseDir = MiniDFSCluster.getBaseDirectory();
    FileUtils.forceMkdir(new File(baseDir + "/db"));

    nnaConf.set("nna.base.dir", baseDir);
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

  @AfterClass
  public static void tearDown() throws IOException {
    nna.shutdown();
    cluster.shutdown();
  }

  @Before
  public void before() throws IOException {
    client = new DefaultHttpClient();
  }

  /**
   * Long running execution that will launch an NNA instance backed by a live updating HA-enabled
   * HDFS instance. NNA will update every minute or so with new analysis.
   */
  public static void main(String[] args) throws Exception {
    beforeClass();
    TestWithMiniCluster test = new TestWithMiniCluster();
    test.addFiles(10000, 300L);
  }

  @Test
  public void testInfo() throws Exception {
    HttpGet get = new HttpGet("http://localhost:4567/info");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
    assertThat(IOUtils.toString(res.getEntity().getContent()), containsString("INode GSet size: "));
  }

  /**
   * In 2.4.0, MiniQJMHACluster has a bug starting DNs so we will use directories to track updates
   * instead of files.
   */
  @Test(timeout = 60000L)
  public void testUpdateSeen() throws Exception {
    HttpGet get = new HttpGet("http://localhost:4567/filter?set=files&sum=count");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
    String content = IOUtils.toString(res.getEntity().getContent());
    int startingCount = Integer.parseInt(content);

    // Trigger file system updates.
    addDirs(100, 0L);

    // Ensure NNA sees those updates in query.
    int checkCount;
    do {
      HttpGet check = new HttpGet("http://localhost:4567/filter?set=dirs&sum=count");
      HttpResponse checkRes = client.execute(hostPort, check);
      assertThat(checkRes.getStatusLine().getStatusCode(), is(200));
      String checkContent = IOUtils.toString(checkRes.getEntity().getContent());
      checkCount = Integer.parseInt(checkContent);
      Thread.sleep(200L);
    } while (checkCount == startingCount);

    assertThat(checkCount, is(greaterThan(startingCount)));
  }

  private void addFiles(int numOfFiles, long sleepBetweenMs) throws Exception {
    DistributedFileSystem fileSystem = (DistributedFileSystem) FileSystem.get(CONF);
    for (int i = 0; i < numOfFiles; i++) {
      int dirNumber1 = RANDOM.nextInt(10);
      Path dirPath = new Path("/dir" + dirNumber1);
      int dirNumber2 = RANDOM.nextInt(10);
      dirPath = dirPath.suffix("/dir" + dirNumber2);
      int dirNumber3 = RANDOM.nextInt(10);
      dirPath = dirPath.suffix("/dir" + dirNumber3);
      fileSystem.mkdirs(dirPath);
      Path filePath = dirPath.suffix("/file" + i);
      int fileType = RANDOM.nextInt(5);
      switch (fileType) {
        case 0:
          filePath = filePath.suffix(".zip");
          break;
        case 1:
          filePath = filePath.suffix(".avro");
          break;
        case 2:
          filePath = filePath.suffix(".orc");
          break;
        case 3:
          filePath = filePath.suffix(".txt");
          break;
        case 4:
          filePath = filePath.suffix(".json");
          break;
        default:
          break;
      }
      int fileSize = RANDOM.nextInt(4);
      switch (fileSize) {
        case 0:
          DFSTestUtil.writeFile(fileSystem, filePath, "");
          break;
        case 1:
          DFSTestUtil.writeFile(fileSystem, filePath, new String(TINY_FILE_BYTES));
          break;
        case 2:
          DFSTestUtil.writeFile(fileSystem, filePath, new String(SMALL_FILE_BYTES));
          break;
        case 3:
          DFSTestUtil.writeFile(fileSystem, filePath, new String(MEDIUM_FILE_BYTES));
          break;
        default:
          DFSTestUtil.writeFile(fileSystem, filePath, "");
          break;
      }
      int user = RANDOM.nextInt(3);
      switch (user) {
        case 0:
          break;
        case 1:
          fileSystem.setOwner(filePath, USERS[0], USERS[0]);
          break;
        case 2:
          fileSystem.setOwner(filePath, USERS[1], USERS[1]);
          break;
        default:
          break;
      }
      short repFactor = (short) RANDOM.nextInt(4);
      if (repFactor != 0) {
        fileSystem.setReplication(filePath, repFactor);
      }
      int weeksAgo = RANDOM.nextInt(10);
      long timeStamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(weeksAgo * 7);
      if (weeksAgo != 0) {
        fileSystem.setTimes(filePath, timeStamp, timeStamp);
      }
      if (sleepBetweenMs != 0L) {
        Thread.sleep(sleepBetweenMs);
      }
    }
  }

  private void addDirs(int numOfDirs, long sleepBetweenMs) throws Exception {
    DistributedFileSystem fileSystem = (DistributedFileSystem) FileSystem.get(CONF);
    for (int i = 0; i < numOfDirs; i++) {
      int dirNumber1 = RANDOM.nextInt(10);
      Path dirPath = new Path("/dir" + dirNumber1);
      int dirNumber2 = RANDOM.nextInt(10);
      dirPath = dirPath.suffix("/dir" + dirNumber2);
      int dirNumber3 = RANDOM.nextInt(10);
      dirPath = dirPath.suffix("/dir" + dirNumber3);
      fileSystem.mkdirs(dirPath);
      if (sleepBetweenMs != 0L) {
        Thread.sleep(sleepBetweenMs);
      }
    }
  }
}
