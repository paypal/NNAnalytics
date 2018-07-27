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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.paypal.namenode.NNAnalyticsRestAPI;
import com.paypal.security.SecurityConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.GSetGenerator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.util.GSet;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
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
    SecurityConfiguration conf = new SecurityConfiguration();
    conf.set("ldap.enable", "false");
    conf.set("authorization.enable", "false");
    conf.set("nna.historical", "false");
    conf.set("nna.localonly.users", "hdfs:hdfs,hdfsW:hdfsW,hdfsR:hdfsR");
    conf.set("nna.base.dir", MiniDFSCluster.getBaseDirectory());
    nna.init(conf, gset);
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

  @Test
  public void testLocalAuthentication() throws IOException {
    // Test authentication required.
    HttpGet get = new HttpGet("http://localhost:4567/info");
    HttpResponse res = client.execute(hostPort, get);
    System.out.println(IOUtils.toString(res.getEntity().getContent()));
    assertThat(res.getStatusLine().getStatusCode(), is(401));

    // Do local auth.
    HttpPost post = new HttpPost("http://localhost:4567/login");
    List<NameValuePair> postParams = new ArrayList<>();
    postParams.add(new BasicNameValuePair("username", "hdfs"));
    postParams.add(new BasicNameValuePair("password", "hdfs"));
    post.setEntity(new UrlEncodedFormEntity(postParams, "UTF-8"));
    HttpResponse res2 = client.execute(hostPort, post);
    System.out.println(IOUtils.toString(res2.getEntity().getContent()));
    assertThat(res2.getStatusLine().getStatusCode(), is(200));

    // Use JWT to auth again.
    Header tokenHeader = res2.getFirstHeader("Set-Cookie");
    HttpGet get2 = new HttpGet("http://localhost:4567/threads");
    get2.addHeader("Cookie", tokenHeader.getValue());
    HttpResponse res3 = client.execute(hostPort, get2);
    IOUtils.readLines(res3.getEntity().getContent()).clear();
    assertThat(res3.getStatusLine().getStatusCode(), is(200));

    // Check credentials exist.
    HttpGet get3 = new HttpGet("http://localhost:4567/credentials");
    get3.addHeader("Cookie", tokenHeader.getValue());
    HttpResponse res4 = client.execute(hostPort, get3);
    IOUtils.readLines(res4.getEntity().getContent()).clear();
    assertThat(res4.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testLocalLogout() throws IOException {
    // Do local auth.
    HttpPost post = new HttpPost("http://localhost:4567/login");
    List<NameValuePair> postParams = new ArrayList<>();
    postParams.add(new BasicNameValuePair("username", "hdfs"));
    postParams.add(new BasicNameValuePair("password", "hdfs"));
    post.setEntity(new UrlEncodedFormEntity(postParams, "UTF-8"));
    HttpResponse res = client.execute(hostPort, post);
    System.out.println(IOUtils.toString(res.getEntity().getContent()));
    assertThat(res.getStatusLine().getStatusCode(), is(200));

    // Logout.
    Header tokenHeader = res.getFirstHeader("Set-Cookie");
    HttpPost post2 = new HttpPost("http://localhost:4567/logout");
    post2.addHeader("Cookie", tokenHeader.getValue());
    HttpResponse res2 = client.execute(hostPort, post2);
    assertThat(IOUtils.toString(res2.getEntity().getContent()), containsString("logged out"));
    assertThat(res2.getStatusLine().getStatusCode(), is(200));

    // Logout again; no JWT.
    HttpGet get3 = new HttpGet("http://localhost:4567/logout");
    HttpResponse res3 = client.execute(hostPort, get3);
    assertThat(
        IOUtils.toString(res3.getEntity().getContent()),
        containsString("Authentication required."));
    assertThat(res3.getStatusLine().getStatusCode(), is(401));
  }
}
