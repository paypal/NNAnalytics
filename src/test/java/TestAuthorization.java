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

import com.paypal.namenode.NNAnalyticsRestAPI;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.server.namenode.GSetGenerator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.NNLoader;
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
import spark.Spark;

public class TestAuthorization {

  private static HttpHost hostPort;
  private static HttpClient client;
  private static NNLoader nn;

  @BeforeClass
  public static void beforeClass() throws Exception {
    GSetGenerator gSetGenerator = new GSetGenerator();
    gSetGenerator.clear();
    GSet<INode, INodeWithAdditionalFields> gset = gSetGenerator.getGSet((short) 3, 10, 500);
    NNAnalyticsRestAPI nna = new NNAnalyticsRestAPI();
    nna.initAuth(false, true);
    nna.initRestServer();
    nn = nna.initLoader(gset, false);
    hostPort = new HttpHost("localhost", 4567);
  }

  @AfterClass
  public static void tearDown() {
    nn.clear();
    Spark.stop();
  }

  @Before
  public void before() {
    client = new DefaultHttpClient();
  }

  @Test
  public void testUnsecuredAuthorization() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/sets?proxy=badUser");
    HttpResponse res = client.execute(hostPort, get);
    System.out.println(IOUtils.toString(res.getEntity().getContent()));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAdminAuthorizationFail() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/config?proxy=badUser");
    HttpResponse res = client.execute(hostPort, get);
    System.out.println(IOUtils.toString(res.getEntity().getContent()));
    assertThat(res.getStatusLine().getStatusCode(), is(403));
  }

  @Test
  public void testWriterAuthorizationFail() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/config?proxy=badUser");
    HttpResponse res = client.execute(hostPort, get);
    System.out.println(IOUtils.toString(res.getEntity().getContent()));
    assertThat(res.getStatusLine().getStatusCode(), is(403));
  }

  @Test
  public void testReaderAuthorizationFail() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/info?proxy=badUser");
    HttpResponse res = client.execute(hostPort, get);
    System.out.println(IOUtils.toString(res.getEntity().getContent()));
    assertThat(res.getStatusLine().getStatusCode(), is(403));
  }

  @Test
  public void testReaderAuthorization() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/info?proxy=hdfsR");
    HttpResponse res = client.execute(hostPort, get);
    System.out.println(IOUtils.toString(res.getEntity().getContent()));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testWriterAuthorization() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/listOperations?proxy=hdfsW");
    HttpResponse res = client.execute(hostPort, get);
    System.out.println(IOUtils.toString(res.getEntity().getContent()));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAdminAuthorization() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/refresh?proxy=hdfs");
    HttpResponse res = client.execute(hostPort, get);
    System.out.println(IOUtils.toString(res.getEntity().getContent()));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testLocalUserAdminAuthorization() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/refresh?proxy=hdfs");
    HttpResponse res = client.execute(hostPort, get);
    System.out.println(IOUtils.toString(res.getEntity().getContent()));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }
}
