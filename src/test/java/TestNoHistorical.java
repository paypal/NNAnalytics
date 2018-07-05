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
import org.junit.Test;

public class TestNoHistorical {

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

  public static void main(String[] args) throws Exception {
    beforeClass();
    while (true) {
      // Let the server run.
    }
  }

  @AfterClass
  public static void tearDown() {
    nna.shutdown();
  }

  @Before
  public void before() {
    client = new DefaultHttpClient();
  }

  @Test
  public void testHistoryClosed() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/history");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(400));
    assertThat(
        IOUtils.toString(res.getEntity().getContent()),
        containsString("DB connection is not open."));
  }
}
