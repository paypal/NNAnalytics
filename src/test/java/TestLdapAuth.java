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
import java.util.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.server.namenode.GSetGenerator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.util.GSet;
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

public class TestLdapAuth {

  private static HttpHost hostPort;
  private static HttpClient client;
  private static NNAnalyticsRestAPI nna;

  @BeforeClass
  public static void beforeClass() throws Exception {
    GSetGenerator gSetGenerator = new GSetGenerator();
    gSetGenerator.clear();
    GSet<INode, INodeWithAdditionalFields> gset = gSetGenerator.getGSet((short) 3, 10, 500);
    nna = new NNAnalyticsRestAPI();
    nna.initAuth(false, false);
    nna.initRestServer();
    nna.initLoader(gset, false);
    hostPort = new HttpHost("localhost", 4567);
  }

  @AfterClass
  public static void tearDown() {
    nna.shutdown();
  }

  @Before
  public void before() {
    client = new DefaultHttpClient();
  }

  @Ignore("Test ignored -- for self test only.")
  @Test
  public void testBasicAuthentication() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/info");
    HttpResponse res = client.execute(hostPort, get);
    System.out.println(IOUtils.toString(res.getEntity().getContent()));
    assertThat(res.getStatusLine().getStatusCode(), is(401));
    // Clear body.
    byte[] encode = Base64.getEncoder().encode("USERNAME@COMPANY:PASSWORD".getBytes());
    get.addHeader("Authorization", "Basic " + new String(encode));
    HttpResponse res2 = client.execute(hostPort, get);
    System.out.println(IOUtils.toString(res2.getEntity().getContent()));
    assertThat(res2.getStatusLine().getStatusCode(), is(200));
    HttpGet get2 = new HttpGet("http://localhost:4567/info");
    HttpResponse res3 = client.execute(hostPort, get2);
    assertThat(res3.getStatusLine().getStatusCode(), is(200));
    System.out.println(IOUtils.toString(res3.getEntity().getContent()));
    HttpGet get3 = new HttpGet("http://localhost:4567/info");
    HttpResponse res4 = client.execute(hostPort, get3);
    assertThat(res4.getStatusLine().getStatusCode(), is(200));
    System.out.println(IOUtils.toString(res4.getEntity().getContent()));
  }

  @Ignore("Test ignored -- for self test only.")
  @Test
  public void testLocalAuthentication() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/info");
    HttpResponse res = client.execute(hostPort, get);
    System.out.println(IOUtils.toString(res.getEntity().getContent()));
    assertThat(res.getStatusLine().getStatusCode(), is(401));
    // Clear body.
    byte[] encode = Base64.getEncoder().encode("hdfs:hdfs".getBytes());
    get.addHeader("Authorization", "Basic " + new String(encode));
    HttpResponse res2 = client.execute(hostPort, get);
    System.out.println(IOUtils.toString(res2.getEntity().getContent()));
    assertThat(res2.getStatusLine().getStatusCode(), is(200));
    HttpGet get2 = new HttpGet("http://localhost:4567/threads");
    get2.addHeader("Authorization", "Basic " + new String(encode));
    HttpResponse res3 = client.execute(hostPort, get2);
    assertThat(res3.getStatusLine().getStatusCode(), is(200));
    System.out.println(IOUtils.toString(res3.getEntity().getContent()));
    HttpGet get3 = new HttpGet("http://localhost:4567/threads");
    get3.addHeader("Authorization", "Basic " + new String(encode));
    HttpResponse res4 = client.execute(hostPort, get3);
    assertThat(res4.getStatusLine().getStatusCode(), is(200));
    System.out.println(IOUtils.toString(res4.getEntity().getContent()));
  }
}
