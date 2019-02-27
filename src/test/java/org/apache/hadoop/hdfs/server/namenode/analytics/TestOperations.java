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

import java.io.IOException;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.GSetGenerator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.analytics.security.SecurityConfiguration;
import org.apache.hadoop.util.GSet;
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
public class TestOperations {

  private static HttpHost hostPort;
  private static HttpClient client;
  private static WebServerMain nna;

  @BeforeClass
  public static void beforeClass() throws Exception {
    GSetGenerator gSetGenerator = new GSetGenerator();
    gSetGenerator.clear();
    GSet<INode, INodeWithAdditionalFields> gset = gSetGenerator.getGSet((short) 3, 10, 500);
    nna = new WebServerMain();
    SecurityConfiguration conf = new SecurityConfiguration();
    conf.set("ldap.enable", "false");
    conf.set("authorization.enable", "false");
    conf.set("nna.historical", "false");
    conf.set("nna.base.dir", MiniDFSCluster.getBaseDirectory());
    nna.init(conf, gset);
    hostPort = new HttpHost("localhost", 4567);
  }

  @AfterClass
  public static void tearDown() {
    if (nna != null) {
      nna.shutdown();
    }
  }

  @Before
  public void before() {
    client = new DefaultHttpClient();
  }

  @Test(timeout = 10000)
  public void testDelete() throws IOException, InterruptedException {
    HttpGet post =
        new HttpGet(
            "http://localhost:4567/submitOperation?set=files&filters=fileSize:eq:0,accessTime:daysAgo:3&sleep=0&operation=delete");
    HttpResponse res = client.execute(hostPort, post);
    String deleteID = IOUtils.readLines(res.getEntity().getContent()).get(0);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
    int statusCode;
    while (true) {
      client = new DefaultHttpClient();
      HttpGet get = new HttpGet("http://localhost:4567/listOperations?identity=" + deleteID);
      res = client.execute(hostPort, get);
      statusCode = res.getStatusLine().getStatusCode();
      if (statusCode != 404) {
        assertThat(statusCode, is(200));
      } else {
        break;
      }
    }
    assertThat(statusCode, is(404));
  }

  @Test(timeout = 10000)
  public void testSetReplication() throws IOException, InterruptedException {
    HttpGet post =
        new HttpGet(
            "http://localhost:4567/submitOperation?set=files&filters=fileSize:eq:0,accessTime:daysAgo:3&sleep=0&operation=setReplication:1");
    HttpResponse res = client.execute(hostPort, post);
    String setRepID = IOUtils.readLines(res.getEntity().getContent()).get(0);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
    int statusCode;
    while (true) {
      client = new DefaultHttpClient();
      HttpGet get = new HttpGet("http://localhost:4567/listOperations?identity=" + setRepID);
      res = client.execute(hostPort, get);
      statusCode = res.getStatusLine().getStatusCode();
      if (statusCode != 404) {
        assertThat(statusCode, is(200));
      } else {
        break;
      }
    }
    assertThat(statusCode, is(404));
  }

  @Test(timeout = 10000)
  public void testSetStoragePolicy() throws IOException, InterruptedException {
    HttpGet post =
        new HttpGet(
            "http://localhost:4567/submitOperation?set=files&filters=fileSize:eq:0,accessTime:daysAgo:3&sleep=0&operation=setStoragePolicy:COLD");
    HttpResponse res = client.execute(hostPort, post);
    String setPolicyID = IOUtils.readLines(res.getEntity().getContent()).get(0);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
    int statusCode;
    while (true) {
      client = new DefaultHttpClient();
      HttpGet get = new HttpGet("http://localhost:4567/listOperations?identity=" + setPolicyID);
      res = client.execute(hostPort, get);
      statusCode = res.getStatusLine().getStatusCode();
      if (statusCode != 404) {
        assertThat(statusCode, is(200));
      } else {
        break;
      }
    }
    assertThat(statusCode, is(404));
  }

  @Test
  public void testGetNonExistantDelete() throws IOException, InterruptedException {
    HttpGet get = new HttpGet("http://localhost:4567/abortOperation?identity=FAKEID");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(404));
  }

  @Test
  public void testAbortNonExistantDelete() throws IOException, InterruptedException {
    HttpGet delete = new HttpGet("http://localhost:4567/listOperations?identity=FAKEID");
    HttpResponse res = client.execute(hostPort, delete);
    assertThat(res.getStatusLine().getStatusCode(), is(404));
  }

  @Test(timeout = 10000)
  public void testAbortDeletes() throws IOException, InterruptedException {
    HttpGet post =
        new HttpGet(
            "http://localhost:4567/submitOperation?set=files&filters=fileSize:lte:1048576,fileSize:gt:1024&sleep=1000&operation=delete");
    HttpResponse res = client.execute(hostPort, post);
    String deleteID1 = IOUtils.readLines(res.getEntity().getContent()).get(0);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
    client = new DefaultHttpClient();
    post =
        new HttpGet(
            "http://localhost:4567/submitOperation?set=files&filters=fileSize:eq:0&sleep=1000&operation=delete");
    res = client.execute(hostPort, post);
    String deleteID2 = IOUtils.readLines(res.getEntity().getContent()).get(0);
    assertThat(res.getStatusLine().getStatusCode(), is(200));

    client = new DefaultHttpClient();
    HttpGet delete = new HttpGet("http://localhost:4567/abortOperation?identity=" + deleteID1);
    res = client.execute(hostPort, delete);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
    client = new DefaultHttpClient();
    delete = new HttpGet("http://localhost:4567/abortOperation?identity=" + deleteID2);
    res = client.execute(hostPort, delete);
    assertThat(res.getStatusLine().getStatusCode(), is(200));

    while (true) {
      client = new DefaultHttpClient();
      HttpGet get = new HttpGet("http://localhost:4567/listOperations");
      res = client.execute(hostPort, get);
      assertThat(res.getStatusLine().getStatusCode(), is(200));
      List<String> text = IOUtils.readLines(res.getEntity().getContent());
      if (text.size() == 1) {
        break;
      }
    }
  }
}
