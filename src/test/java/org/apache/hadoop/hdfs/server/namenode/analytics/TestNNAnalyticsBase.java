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

import static org.apache.hadoop.hdfs.server.namenode.Constants.UNSECURED_ENDPOINTS;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.fail;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.server.namenode.Constants;
import org.apache.hadoop.hdfs.server.namenode.Constants.Endpoint;
import org.apache.hadoop.hdfs.server.namenode.Constants.Filter;
import org.apache.hadoop.hdfs.server.namenode.Constants.Find;
import org.apache.hadoop.hdfs.server.namenode.Constants.FindField;
import org.apache.hadoop.hdfs.server.namenode.Constants.Histogram;
import org.apache.hadoop.hdfs.server.namenode.Constants.INodeSet;
import org.apache.hadoop.hdfs.server.namenode.Constants.Sum;
import org.apache.hadoop.hdfs.server.namenode.GSetGenerator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLoader;
import org.apache.hadoop.hdfs.server.namenode.queries.Transforms;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public abstract class TestNNAnalyticsBase {

  protected static HttpHost hostPort;
  protected static ApplicationMain nna;
  private static HttpClient client;
  private static int count = 0;
  private static long timeTaken = 0;

  @AfterClass
  public static void tearDown() {
    if (nna != null) {
      nna.shutdown();
    }
  }

  @Before
  public void before() {
    client = new DefaultHttpClient();
    count = 0;
    timeTaken = 0;
  }

  @Test
  public void testInfo() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/info");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
    assertThat(
        IOUtils.toString(res.getEntity().getContent()),
        containsString("INode GSet size: " + GSetGenerator.TOTAL_MADE.apply(null)));
  }

  @Test
  public void testSQL1() throws IOException {
    HttpPost post = new HttpPost("http://localhost:4567/sql");
    List<NameValuePair> postParams = new ArrayList<>();
    postParams.add(
        new BasicNameValuePair("sqlStatement", "SELECT * FROM FILES WHERE fileSize = 0"));
    post.setEntity(new UrlEncodedFormEntity(postParams, "UTF-8"));
    HttpResponse res = client.execute(hostPort, post);
    if (res.getStatusLine().getStatusCode() != HttpStatus.SC_NOT_FOUND) {
      List<String> text = IOUtils.readLines(res.getEntity().getContent());
      assertThat(text.size(), is(greaterThan(100)));
      assertThat(res.getStatusLine().getStatusCode(), is(HttpStatus.SC_OK));
    }
  }

  @Test
  public void testSQL2() throws IOException {
    HttpPost post = new HttpPost("http://localhost:4567/sql");
    List<NameValuePair> postParams = new ArrayList<>();
    postParams.add(
        new BasicNameValuePair("sqlStatement", "SELECT * FROM DIRS WHERE dirNumChildren = 0"));
    post.setEntity(new UrlEncodedFormEntity(postParams, "UTF-8"));
    HttpResponse res = client.execute(hostPort, post);
    if (res.getStatusLine().getStatusCode() != HttpStatus.SC_NOT_FOUND) {
      List<String> text = IOUtils.readLines(res.getEntity().getContent());
      assertThat(text.size(), is(0));
      assertThat(res.getStatusLine().getStatusCode(), is(HttpStatus.SC_OK));
    }
  }

  @Test
  public void testRefresh() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/refresh");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
    assertThat(IOUtils.toString(res.getEntity().getContent()), containsString("UserSet"));
  }

  @Test
  public void testUserMetrics() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/metrics");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
    assertThat(IOUtils.toString(res.getEntity().getContent()), containsString("users"));
  }

  @Test
  public void testTokens() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/token");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testDump() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/dump?path=/");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testBadDump1() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/dump?path=");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(500));
    IOUtils.readLines(res.getEntity().getContent()).clear();
    HttpGet verify = new HttpGet("http://localhost:4567/info");
    HttpResponse verifyRes = client.execute(hostPort, verify);
    List<String> output = IOUtils.readLines(verifyRes.getEntity().getContent());
    assertThat(output, not(hasItem("/dump?path=:default_unsecured_user")));
  }

  @Test
  public void testBadDump2() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/dump?path=bad");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(500));
    IOUtils.readLines(res.getEntity().getContent()).clear();
    HttpGet verify = new HttpGet("http://localhost:4567/info");
    HttpResponse verifyRes = client.execute(hostPort, verify);
    List<String> output = IOUtils.readLines(verifyRes.getEntity().getContent());
    assertThat(output, not(hasItem("/dump?path=bad:default_unsecured_user")));
  }

  @Test
  public void testTop() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/top");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testBottom() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/bottom");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testSuggestions() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/suggestions");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAllSuggestions() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/suggestions?all");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testSuggestionsUser() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/suggestions?user=hdfs");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testDirectories() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/directories");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testFileAgeCount() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/fileAge?sum=count");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testFileAgeDs() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/fileAge?sum=diskspaceConsumed");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testUsers() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/users");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testUsersSuggestions() throws IOException {
    NameNodeLoader loader = nna.getLoader();
    loader.getSuggestionsEngine().reloadSuggestions(loader);
    HttpGet get = new HttpGet("http://localhost:4567/users?suggestion=emptyFilesUsers");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAllQuotas() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/quotas?all");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testDsQuotas() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/quotas?sum=dsQuotaRatioUsed");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testNsQuotas() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/quotas?sum=nsQuotaRatioUsed");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testThreads() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/threads");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testSystem() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/system");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testEndpoints() throws IOException {
    EnumSet<Endpoint> clone = UNSECURED_ENDPOINTS.clone();
    clone.remove(Endpoint.login);
    clone.remove(Endpoint.logout);
    clone.remove(Endpoint.credentials);
    clone.forEach(
        x -> {
          String url = "http://localhost:4567/" + x.name();
          System.out.println("Calling: " + url);
          HttpGet get = new HttpGet(url);
          HttpResponse res = null;
          try {
            res = client.execute(hostPort, get);
            IOUtils.readLines(res.getEntity().getContent()).clear();
          } catch (IOException e) {
            fail();
          }
          assertThat(res.getStatusLine().getStatusCode(), is(200));
        });
  }

  @Test
  public void testUnsecureLogout() throws IOException {
    HttpPost post = new HttpPost("http://localhost:4567/logout");
    HttpResponse res = client.execute(hostPort, post);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(400));
  }

  @Test
  public void testUnsecureCredentials() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/credentials");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(400));
  }

  @Test
  public void testAddRemoveDirectoryToCache() throws IOException {
    HttpGet addDir = new HttpGet("http://localhost:4567/addDirectory?dir=/test");
    HttpResponse res = client.execute(hostPort, addDir);
    IOUtils.readLines(res.getEntity().getContent()).clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));

    HttpGet rmNonExistDir = new HttpGet("http://localhost:4567/removeDirectory?dir=/test2");
    res = client.execute(hostPort, rmNonExistDir);
    IOUtils.readLines(res.getEntity().getContent()).clear();
    assertThat(res.getStatusLine().getStatusCode(), is(404));

    HttpGet rmDir = new HttpGet("http://localhost:4567/removeDirectory?dir=/test");
    res = client.execute(hostPort, rmDir);
    IOUtils.readLines(res.getEntity().getContent()).clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAddRemoveQueryToCache() throws IOException {
    HttpGet addQuery =
        new HttpGet(
            "http://localhost:4567/setCachedQuery?queryName=test&queryType=filter&set=files&filters=fileSize:eq:0&sum=count");
    HttpResponse res = client.execute(hostPort, addQuery);
    IOUtils.readLines(res.getEntity().getContent()).clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));

    HttpGet rmNonExistDir = new HttpGet("http://localhost:4567/removeCachedQuery?queryName=test2");
    res = client.execute(hostPort, rmNonExistDir);
    IOUtils.readLines(res.getEntity().getContent()).clear();
    assertThat(res.getStatusLine().getStatusCode(), is(404));

    HttpGet getQuery = new HttpGet("http://localhost:4567/getCachedQuery?queryName=test");
    res = client.execute(hostPort, getQuery);
    IOUtils.readLines(res.getEntity().getContent()).clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));

    HttpGet rmQuery = new HttpGet("http://localhost:4567/removeCachedQuery?queryName=test");
    res = client.execute(hostPort, rmQuery);
    IOUtils.readLines(res.getEntity().getContent()).clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testInodeId() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/filter?set=files&filters=id:gt:0,id:lt:10000000&sum=count");
    HttpResponse res = client.execute(hostPort, get);
    List<String> result = IOUtils.readLines(res.getEntity().getContent());
    assertThat(result.size(), is(1));
    assertThat(result.get(0), is(String.valueOf(GSetGenerator.FILES_MADE)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testModDateFilterGt() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/filter?set=files&filters=modDate:dateGt:01/01/1990&sum=count");
    HttpResponse res = client.execute(hostPort, get);
    List<String> result = IOUtils.readLines(res.getEntity().getContent());
    assertThat(result.size(), is(1));
    assertThat(result.get(0), is(String.valueOf(GSetGenerator.FILES_MADE)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testIsUnderConstruction() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/filter?set=files&filters=isUnderConstruction:notEq:false&sum=count");
    HttpResponse res = client.execute(hostPort, get);
    List<String> result = IOUtils.readLines(res.getEntity().getContent());
    assertThat(result.size(), is(1));
    assertThat(result.get(0), is("0"));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testIsWithSnapshot() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/filter?set=dirs&filters=isWithSnapshot:notEq:false&sum=count");
    HttpResponse res = client.execute(hostPort, get);
    List<String> result = IOUtils.readLines(res.getEntity().getContent());
    assertThat(result.size(), is(1));
    assertThat(result.get(0), is("0"));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testHasQuota() throws IOException {
    HttpGet get =
        new HttpGet("http://localhost:4567/filter?set=dirs&filters=hasQuota:eq:true&sum=count");
    HttpResponse res = client.execute(hostPort, get);
    List<String> result = IOUtils.readLines(res.getEntity().getContent());
    assertThat(result.size(), is(1));
    assertThat(result.get(0), is(not("0")));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testHasQuotaList() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/filter?set=dirs&filters=hasQuota:eq:true");
    HttpResponse res = client.execute(hostPort, get);
    List<String> result = IOUtils.readLines(res.getEntity().getContent());
    System.out.println(result);
    assertThat(result.size(), is(not(0)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testModDateFilterGtAndLt() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/filter?set=files&filters=modDate:dateGt:01/01/1990,modDate:dateLt:01/01/2050&sum=count");
    HttpResponse res = client.execute(hostPort, get);
    List<String> result = IOUtils.readLines(res.getEntity().getContent());
    assertThat(result.size(), is(1));
    assertThat(result.get(0), is(not(String.valueOf(0))));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAccessDateFilterGtAndLt() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/filter?set=files&filters=accessDate:dateGt:01/01/1990,accessDate:dateLt:01/01/2050&sum=count");
    HttpResponse res = client.execute(hostPort, get);
    List<String> result = IOUtils.readLines(res.getEntity().getContent());
    assertThat(result.size(), is(1));
    assertThat(result.get(0), is(not(String.valueOf(0))));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testFileSizeFilterBetweenKBandMB() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/filter?set=files&filters=fileSize:lte:1048576,fileSize:gt:1024&sum=count");
    HttpResponse res = client.execute(hostPort, get);
    List<String> text = IOUtils.readLines(res.getEntity().getContent());
    int count = Integer.parseInt(text.get(0));
    assertThat(count, is(not(0)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testFindMinFileSize() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/filter?set=files&find=min:fileSize");
    HttpResponse res = client.execute(hostPort, get);
    List<String> text = IOUtils.readLines(res.getEntity().getContent());
    assertThat(text.size(), is(1));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testFindMaxFileSize() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/filter?set=files&find=max:fileSize");
    HttpResponse res = client.execute(hostPort, get);
    List<String> text = IOUtils.readLines(res.getEntity().getContent());
    assertThat(text.size(), is(1));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testFindMaxFileSizeUserHistogramCSV() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&type=user&find=max:fileSize&histogramOutput=csv");
    HttpResponse res = client.execute(hostPort, get);
    List<String> text = IOUtils.readLines(res.getEntity().getContent());
    assertThat(text.size(), is(1));
    assertThat(text.get(0).split(",").length, is(2));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testFindAvgFileSizeUserHistogramCSV() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&type=user&find=max:fileSize&histogramOutput=csv");
    HttpResponse res = client.execute(hostPort, get);
    List<String> text = IOUtils.readLines(res.getEntity().getContent());
    assertThat(text.size(), is(1));
    assertThat(text.get(0).split(",").length, is(2));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testFindMinFileSizeUserHistogramCSV() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&type=user&find=min:fileSize&histogramOutput=csv");
    HttpResponse res = client.execute(hostPort, get);
    List<String> text = IOUtils.readLines(res.getEntity().getContent());
    assertThat(text.size(), is(1));
    assertThat(text.get(0).split(",").length, is(2));
    assertThat(text.get(0).split(",")[1], is("0"));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testFindMinAccessTimeHistogramCSV() throws IOException, ParseException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&type=user&find=min:accessTime&histogramOutput=csv");
    HttpResponse res = client.execute(hostPort, get);
    List<String> text = IOUtils.readLines(res.getEntity().getContent());
    assertThat(text.size(), is(1));
    assertThat(text.get(0).split(",").length, is(2));
    Date date =
        new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy").parse(text.get(0).split(",")[1]);
    boolean minDateWasBeforeRightNow = date.before(Date.from(Calendar.getInstance().toInstant()));
    assertThat(minDateWasBeforeRightNow, (is(true)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testFindMinAccessTimeHistogramRawTimestampCSV() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&type=user&find=min:accessTime&histogramOutput=csv&rawTimestamps=true");
    HttpResponse res = client.execute(hostPort, get);
    List<String> text = IOUtils.readLines(res.getEntity().getContent());
    assertThat(text.size(), is(1));
    assertThat(text.get(0).split(",").length, is(2));
    Long timestamp = Long.parseLong(text.get(0).split(",")[1]);
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(timestamp);
    Date date = calendar.getTime();
    boolean minDateWasBeforeRightNow = date.before(Date.from(Calendar.getInstance().toInstant()));
    assertThat(minDateWasBeforeRightNow, (is(true)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testPathFilter() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/filter?set=dirs&filters=path:contains:dir1");
    HttpResponse res = client.execute(hostPort, get);
    List<String> paths = IOUtils.readLines(res.getEntity().getContent());
    for (String path : paths) {
      assertThat(path, containsString("dir1"));
    }
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testHasAclFilter() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/filter?set=files&filters=hasAcl:eq:true");
    HttpResponse res = client.execute(hostPort, get);
    List<String> paths = IOUtils.readLines(res.getEntity().getContent());
    assertThat(paths.size(), is(0));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testDepthFilter() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/filter?set=all&filters=depth:gte:2&sum=count");
    HttpResponse res = client.execute(hostPort, get);
    List<String> result = IOUtils.readLines(res.getEntity().getContent());
    assertThat(result.size(), is(1));
    assertThat(result.get(0), is(String.valueOf(GSetGenerator.TOTAL_MADE.apply(null) - 11)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAccessTimeHistogram() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/histogram?set=all&type=accessTime");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAccessTimeHistogramFindMaxFileSize() throws IOException {
    HttpGet get =
        new HttpGet("http://localhost:4567/histogram?set=files&type=accessTime&find=max:fileSize");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAccessTimeHistogramTop10() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/histogram?set=all&type=accessTime&top=10");
    HttpResponse res = client.execute(hostPort, get);
    JsonObject object =
        new Gson()
            .fromJson(
                new JsonReader(new InputStreamReader(res.getEntity().getContent())),
                JsonObject.class);
    JsonArray histogramValuesArray = getJsonDataArray(object);
    assertThat(histogramValuesArray.size(), is(10));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testBlockSizeHistogramByUser() throws IOException {
    HttpGet get =
        new HttpGet("http://localhost:4567/histogram?set=files&filters=blockSize:gt:0&type=user");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testBlockSizeMaxFindHistogramByUser() throws IOException {
    HttpGet get =
        new HttpGet("http://localhost:4567/histogram?set=files&type=user&find=max:blockSize");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testBlockSizeMinFindHistogramByUser() throws IOException {
    HttpGet get =
        new HttpGet("http://localhost:4567/histogram?set=files&type=user&find=min:blockSize");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testModTimeHistogram() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/histogram?set=all&type=modTime");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testModTimeHistogramBottom10() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/histogram?set=all&type=modTime&bottom=10");
    HttpResponse res = client.execute(hostPort, get);
    JsonObject object =
        new Gson()
            .fromJson(
                new JsonReader(new InputStreamReader(res.getEntity().getContent())),
                JsonObject.class);
    JsonArray histogramValuesArray = getJsonDataArray(object);
    assertThat(histogramValuesArray.size(), is(10));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAccessTimeHistogram2WithCountAndDs() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram2?set=files&type=accessTime&sum=count,diskspaceConsumed");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testModTimeHistogram2WithCountAndDsAsCSV() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram2?set=files&type=modTime&sum=count,diskspaceConsumed&histogramOutput=csv");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testUserHistogram2WithCountAndDsAsJson() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram2?set=files&type=user&sum=count,diskspaceConsumed&histogramOutput=json");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testFileSizeHistogram() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/histogram?set=files&type=fileSize");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testFileReplicaHistogram() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/histogram?set=files&type=fileReplica");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testParentDirHistogram1() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&type=parentDir&parentDirDepth=1&histogramOutput=csv");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    System.out.println(strings);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
    assertThat(strings.size(), is(10));
  }

  @Test
  public void testParentDirHistogram2() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&type=parentDir&parentDirDepth=2&histogramOutput=csv");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    System.out.println(strings);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
    assertThat(strings.size(), is(100));
  }

  @Test
  public void testParentDirHistogram3() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&type=parentDir&parentDirDepth=3&histogramOutput=csv");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    System.out.println(strings);
    assertThat(res.getStatusLine().getStatusCode(), is(200));
    assertThat(strings.size(), is(1000));
  }

  @Test
  public void testFileReplicaHistogramSortAscending() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&type=fileReplica&sortAscending=true");
    HttpResponse res = client.execute(hostPort, get);
    JsonObject object =
        new Gson()
            .fromJson(
                new JsonReader(new InputStreamReader(res.getEntity().getContent())),
                JsonObject.class);
    JsonArray histogramValuesArray = getJsonDataArray(object);
    Iterator<JsonElement> iterator = histogramValuesArray.iterator();
    long currentComp = iterator.next().getAsLong();
    while (iterator.hasNext()) {
      long next = iterator.next().getAsLong();
      assertThat(next >= currentComp, is(true));
      currentComp = next;
    }
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testFileReplicaHistogramSortDescending() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&type=fileReplica&sortDescending=true");
    HttpResponse res = client.execute(hostPort, get);
    JsonObject object =
        new Gson()
            .fromJson(
                new JsonReader(new InputStreamReader(res.getEntity().getContent())),
                JsonObject.class);
    JsonArray histogramValuesArray = getJsonDataArray(object);
    Iterator<JsonElement> iterator = histogramValuesArray.iterator();
    long currentComp = iterator.next().getAsLong();
    while (iterator.hasNext()) {
      long next = iterator.next().getAsLong();
      assertThat(next <= currentComp, is(true));
      currentComp = next;
    }
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testMemoryConsumedHistogram() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/histogram?set=all&type=memoryConsumed");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testStorageTypeHistogram() throws IOException {
    HttpGet get = new HttpGet("http://localhost:4567/histogram?set=files&type=storageType");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    if (!String.join(" ", strings).contains("not supported")) {
      strings.clear();
      assertThat(res.getStatusLine().getStatusCode(), is(200));
    }
  }

  @Test
  public void tesGroupSumDiskConsumedHistogram() throws IOException {
    HttpGet get =
        new HttpGet("http://localhost:4567/histogram?set=files&type=group&sum=diskspaceConsumed");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAccessTimeSumDiskConsumedHistogram() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&type=accessTime&sum=diskspaceConsumed");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAccessTimeSumMemoryConsumedHistogram() throws IOException {
    HttpGet get =
        new HttpGet("http://localhost:4567/histogram?set=files&type=accessTime&sum=memoryConsumed");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAccessTimeMinutesAgoHistogram() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&filters=accessTime:minutesAgo:5,modTime:minutesAgo:5&&type=accessTime&sum=diskspaceConsumed");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAccessTimeHoursAgoHistogram() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&filters=accessTime:hoursAgo:5,modTime:hoursAgo:5&type=accessTime&sum=diskspaceConsumed");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAccessTimeDaysAgoHistogram() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&filters=accessTime:daysAgo:16,modTime:daysAgo:16&type=accessTime&sum=diskspaceConsumed");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAccessTimeMonthsAgoHistogram() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&filters=accessTime:monthsAgo:3,modTime:monthsAgo:3&type=accessTime&sum=diskspaceConsumed");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAccessTimeYearsAgoHistogram() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&filters=accessTime:yearsAgo:1,modTime:yearsAgo:1&type=accessTime&sum=diskspaceConsumed");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAccessTimeConditionsHistogram() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&filters=accessTime:yearsAgo:1,modTime:yearsAgo:1&type=accessTime&sum=count&histogramConditions=gt:15000");
    HttpResponse res = client.execute(hostPort, get);
    JsonObject object =
        new Gson()
            .fromJson(
                new JsonReader(new InputStreamReader(res.getEntity().getContent())),
                JsonObject.class);
    JsonArray histogramValuesArray = getJsonDataArray(object);
    Iterator<JsonElement> iterator = histogramValuesArray.iterator();
    long currentComp = iterator.next().getAsLong();
    while (iterator.hasNext()) {
      assertThat(currentComp >= 15000L, is(true));
    }
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAccessTimeHistogramAsCSV() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=files&filters=accessTime:yearsAgo:1,modTime:yearsAgo:1&type=accessTime&sum=count&histogramOutput=csv");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings
        .stream()
        .filter(string -> !string.isEmpty())
        .forEach(string -> assertThat(string.split(",").length, is(2)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAverageFileSize() throws IOException {
    HttpGet get =
        new HttpGet("http://localhost:4567/divide?set1=files&sum1=fileSize&set2=files&sum2=count");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    assertThat(strings.size(), is(1));
    String average = strings.get(0);
    System.out.println(average);
    long v = Long.parseLong(average);
    assertThat(v, is(not(0L)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAverageFileDiskspace() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/divide?set1=files&sum1=diskspaceConsumed&set2=files&sum2=count");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    assertThat(strings.size(), is(1));
    String average = strings.get(0);
    System.out.println(average);
    long v = Long.parseLong(average);
    assertThat(v, is(not(0L)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAverageSpacePerBlock() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/divide?set1=files&sum1=fileSize&set2=files&sum2=numBlocks");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    assertThat(strings.size(), is(1));
    String average = strings.get(0);
    System.out.println(average);
    long v = Long.parseLong(average);
    assertThat(v, is(not(0L)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAverageBlockSize() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/divide?set1=files&sum1=blockSize&set2=files&sum2=numBlocks");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    assertThat(strings.size(), is(1));
    String average = strings.get(0);
    System.out.println(average);
    long v = Long.parseLong(average);
    assertThat(v, is(not(0L)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAverageDiskspacePerBlock() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/divide?set1=files&sum1=diskspaceConsumed&set2=files&sum2=numBlocks");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    assertThat(strings.size(), is(1));
    String average = strings.get(0);
    System.out.println(average);
    long v = Long.parseLong(average);
    assertThat(v, is(not(0L)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAverageDiskspacePerReplica() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/divide?set1=files&sum1=diskspaceConsumed&set2=files&sum2=numReplicas");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    assertThat(strings.size(), is(1));
    String average = strings.get(0);
    System.out.println(average);
    long v = Long.parseLong(average);
    assertThat(v, is(not(0L)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testAverageFileSizePerDirectory() throws IOException {
    HttpGet get =
        new HttpGet("http://localhost:4567/divide?set1=files&sum1=fileSize&set2=dirs&sum2=count");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    assertThat(strings.size(), is(1));
    String average = strings.get(0);
    System.out.println(average);
    long v = Long.parseLong(average);
    assertThat(v, is(not(0L)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testHasNsQuotaFilter() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/filter?set=files&filters=isUnderNsQuota:eq:true&sum=count");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testHasDsQuotaFilter() throws IOException {
    HttpGet get =
        new HttpGet("http://localhost:4567/filter?set=files&filters=isUnderDsQuota:eq:false");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testNsQuotaHistogram() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=dirs&filters=hasQuota:eq:true&type=dirQuota&sum=nsQuota");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testDsQuotaHistogram() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=dirs&filters=hasQuota:eq:true&type=dirQuota&sum=dsQuota");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testNsQuotaUsedHistogram() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=dirs&filters=hasQuota:eq:true&type=dirQuota&sum=nsQuotaUsed");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testDsQuotaUsedHistogram() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=dirs&filters=hasQuota:eq:true&type=dirQuota&sum=dsQuotaUsed");
    HttpResponse res = client.execute(hostPort, get);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    strings.clear();
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testNsQuotaRatioUsedHistogram() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=dirs&filters=hasQuota:eq:true&type=dirQuota&sum=nsQuotaRatioUsed");
    HttpResponse res = client.execute(hostPort, get);
    JsonObject object =
        new Gson()
            .fromJson(
                new JsonReader(new InputStreamReader(res.getEntity().getContent())),
                JsonObject.class);
    JsonArray histogramValuesArray = getJsonDataArray(object);
    Iterator<JsonElement> iterator = histogramValuesArray.iterator();
    long currentComp = iterator.next().getAsLong();
    while (iterator.hasNext()) {
      assertThat(currentComp >= 0, is(true));
      assertThat(currentComp <= 100, is(true));
      currentComp = iterator.next().getAsLong();
    }
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testDsQuotaRatioUsedHistogram() throws IOException {
    HttpGet get =
        new HttpGet(
            "http://localhost:4567/histogram?set=dirs&filters=hasQuota:eq:true&type=dirQuota&sum=dsQuotaRatioUsed");
    HttpResponse res = client.execute(hostPort, get);
    JsonObject object =
        new Gson()
            .fromJson(
                new JsonReader(new InputStreamReader(res.getEntity().getContent())),
                JsonObject.class);
    JsonArray histogramValuesArray = getJsonDataArray(object);
    Iterator<JsonElement> iterator = histogramValuesArray.iterator();
    long currentComp = iterator.next().getAsLong();
    while (iterator.hasNext()) {
      assertThat(currentComp >= 0, is(true));
      assertThat(currentComp <= 100, is(true));
      currentComp = iterator.next().getAsLong();
    }
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testTransformReplicationFactor() {
    Map<String, Function<INode, Long>> transformMap =
        Transforms.getAttributeTransforms("fileReplica:gte:2", "fileReplica", "1", nna.getLoader());
    assertThat(transformMap.size(), is(CoreMatchers.not(0)));
    Function<INode, Long> fileReplicaTransform = transformMap.get("fileReplica");
    assertThat(fileReplicaTransform, is(notNullValue()));
    for (INode node : nna.getLoader().getINodeSet("files")) {
      Long transformedFileReplica = fileReplicaTransform.apply(node);
      assertThat(transformedFileReplica, is(1L));
    }
  }

  @Test
  public void testTransformDiskspaceConsumedByReplFactor() {
    Map<String, Function<INode, Long>> transformMap =
        Transforms.getAttributeTransforms("fileReplica:gte:2", "fileReplica", "1", nna.getLoader());
    assertThat(transformMap.size(), is(CoreMatchers.not(0)));
    Function<INode, Long> fileReplicaTransform = transformMap.get("diskspaceConsumed");
    assertThat(fileReplicaTransform, is(notNullValue()));
    Collection<INode> files = nna.getLoader().getINodeSet("files");
    long diskspaceConsumed =
        files
            .stream()
            .mapToLong(node -> node.asFile().getFileReplication() * node.asFile().computeFileSize())
            .sum();
    long transformedDiskspaceConsumed = files.stream().mapToLong(fileReplicaTransform::apply).sum();
    assertThat(transformedDiskspaceConsumed < diskspaceConsumed, is(true));
  }

  @Test
  public void testTransformDiskspaceConsumedByUser() {
    Map<String, Function<INode, Long>> transformMap =
        Transforms.getAttributeTransforms("user:eq:hdfs", "fileReplica", "1", nna.getLoader());
    assertThat(transformMap.size(), is(CoreMatchers.not(0)));
    Function<INode, Long> fileReplicaTransform = transformMap.get("diskspaceConsumed");
    assertThat(fileReplicaTransform, is(notNullValue()));
    Collection<INode> files = nna.getLoader().getINodeSet("files");
    long diskspaceConsumed =
        files
            .stream()
            .mapToLong(node -> node.asFile().getFileReplication() * node.asFile().computeFileSize())
            .sum();
    long transformedDiskspaceConsumed = files.stream().mapToLong(fileReplicaTransform::apply).sum();
    assertThat(transformedDiskspaceConsumed < diskspaceConsumed, is(true));
  }

  @Test
  public void testTransformDiskspaceConsumedByBeingWritten() {
    Map<String, Function<INode, Long>> transformMap =
        Transforms.getAttributeTransforms(
            "isUnderConstruction:eq:true", "fileReplica", "1", nna.getLoader());
    assertThat(transformMap.size(), is(CoreMatchers.not(0)));
    Function<INode, Long> fileReplicaTransform = transformMap.get("diskspaceConsumed");
    assertThat(fileReplicaTransform, is(notNullValue()));
    Collection<INode> files = nna.getLoader().getINodeSet("files");
    long diskspaceConsumed =
        files
            .stream()
            .mapToLong(node -> node.asFile().getFileReplication() * node.asFile().computeFileSize())
            .sum();
    long transformedDiskspaceConsumed = files.stream().mapToLong(fileReplicaTransform::apply).sum();
    assertThat(transformedDiskspaceConsumed == diskspaceConsumed, is(true));
  }

  private static JsonArray getJsonDataArray(JsonObject json) {
    JsonArray datasets = json.getAsJsonArray("datasets");
    for (JsonElement next : datasets) {
      if (next.isJsonObject() && next.getAsJsonObject().has("data")) {
        return next.getAsJsonObject().get("data").getAsJsonArray();
      }
    }
    throw new IllegalStateException("No data found in histogram.");
  }

  /* FINDER QUERY TESTS */

  @Test
  public void testFilterAndFindQuery() throws IOException {
    HashMap<INodeSet, HashMap<Find, ArrayList<FindField>>> config = getSetFilterFindConfig();
    String[] parameters = new String[5];
    parameters[0] = "http://localhost:4567/filter?";
    for (INodeSet setType : INodeSet.values()) {
      parameters[1] = setType.toString();
      HashMap<Find, ArrayList<FindField>> findFields = config.get(setType);
      for (Map.Entry<Find, ArrayList<FindField>> findField : findFields.entrySet()) {
        parameters[3] = findField.getKey().toString();
        for (FindField field : findField.getValue()) {
          parameters[4] = field.toString();
          String testingURL = buildFindQuery(parameters, false);
          checkOutput(testingURL);
        }
      }
    }
    System.out.println("Total # of completed query check for benchmarking: " + count);
    System.out.println("Total time taken in milliseconds: " + timeTaken);
  }

  @Test
  public void testHistogramTypeAndFindQuery() throws IOException {
    HashMap<INodeSet, HashMap<Histogram, HashMap<Find, ArrayList<FindField>>>> config =
        getSetHistogramTypeFindConfig();
    String[] parameters = new String[5];
    parameters[0] = "http://localhost:4567/histogram?";
    for (INodeSet setType : INodeSet.values()) {
      parameters[1] = setType.toString();
      HashMap<Histogram, HashMap<Find, ArrayList<FindField>>> histFindFields = config.get(setType);
      for (Map.Entry<Histogram, HashMap<Find, ArrayList<FindField>>> histFindField :
          histFindFields.entrySet()) {
        parameters[2] = histFindField.getKey().toString();
        HashMap<Find, ArrayList<FindField>> findFields = histFindField.getValue();
        for (Map.Entry<Find, ArrayList<FindField>> findField : findFields.entrySet()) {
          parameters[3] = findField.getKey().toString();
          for (FindField field : findField.getValue()) {
            parameters[4] = field.toString();
            String requestedURL = buildFindQuery(parameters, true);
            checkOutput(requestedURL);
          }
        }
      }
    }
    System.out.println("Total # of completed query check for benchmarking: " + count);
    System.out.println("Total time taken in milliseconds: " + timeTaken);
  }

  private static HashMap<INodeSet, HashMap<Find, ArrayList<FindField>>> getSetFilterFindConfig() {
    HashMap<INodeSet, HashMap<Find, ArrayList<FindField>>> config = new HashMap<>();

    // For 'File' set type
    HashMap<Find, ArrayList<FindField>> fileFilterFind = new HashMap<>();
    ArrayList<FindField> fileFind = new ArrayList<>();
    fileFind.addAll(Constants.FIND_FILE);
    fileFilterFind.put(Find.max, fileFind);
    fileFilterFind.put(Find.min, fileFind);
    config.put(INodeSet.files, fileFilterFind);

    // For 'Dir' set type
    HashMap<Find, ArrayList<FindField>> dirFilterFind = new HashMap<>();
    ArrayList<FindField> dirFind = new ArrayList<>();
    dirFind.addAll(Constants.FIND_DIR);
    dirFilterFind.put(Find.max, dirFind);
    dirFilterFind.put(Find.min, dirFind);
    config.put(INodeSet.dirs, dirFilterFind);

    // For 'All' set type
    HashMap<Find, ArrayList<FindField>> allFilterFind = new HashMap<>();
    ArrayList<FindField> allFind = new ArrayList<>();
    allFind.addAll(Constants.FIND_ALL);
    allFilterFind.put(Find.max, allFind);
    allFilterFind.put(Find.min, allFind);
    config.put(INodeSet.all, allFilterFind);

    return config;
  }

  private static HashMap<INodeSet, HashMap<Histogram, HashMap<Find, ArrayList<FindField>>>>
      getSetHistogramTypeFindConfig() {
    HashMap<INodeSet, HashMap<Histogram, HashMap<Find, ArrayList<FindField>>>> config =
        new HashMap<>();

    // For 'File' set type
    HashMap<Histogram, HashMap<Find, ArrayList<FindField>>> fileFindField = new HashMap<>();
    HashMap<Find, ArrayList<FindField>> findFile = new HashMap<>();
    ArrayList<FindField> fileFind = new ArrayList<>();
    fileFind.addAll(Constants.FIND_FILE);
    findFile.put(Find.max, fileFind);
    findFile.put(Find.avg, fileFind);
    findFile.put(Find.min, fileFind);
    for (Histogram typeFile : Constants.TYPE_FILE) {
      fileFindField.put(typeFile, findFile);
    }
    config.put(INodeSet.files, fileFindField);

    // For 'Dir' set type
    HashMap<Histogram, HashMap<Find, ArrayList<FindField>>> dirFindField = new HashMap<>();
    HashMap<Find, ArrayList<FindField>> findDir = new HashMap<>();
    ArrayList<FindField> dirFind = new ArrayList<>();
    dirFind.addAll(Constants.FIND_DIR);
    findDir.put(Find.max, dirFind);
    findDir.put(Find.avg, fileFind);
    findDir.put(Find.min, dirFind);
    for (Histogram typeDir : Constants.TYPE_DIR) {
      dirFindField.put(typeDir, findDir);
    }
    config.put(INodeSet.dirs, dirFindField);

    // For 'All' set type
    HashMap<Histogram, HashMap<Find, ArrayList<FindField>>> allFindField = new HashMap<>();
    HashMap<Find, ArrayList<FindField>> findAll = new HashMap<>();
    ArrayList<FindField> allFind = new ArrayList<>();
    allFind.addAll(Constants.FIND_ALL);
    findAll.put(Find.max, allFind);
    findDir.put(Find.avg, allFind);
    findAll.put(Find.min, allFind);
    for (Histogram typeAll : Constants.TYPE_ALL) {
      allFindField.put(typeAll, findAll);
    }
    config.put(INodeSet.all, allFindField);

    return config;
  }

  private static String buildFindQuery(String[] parameters, boolean isHistogram) {
    StringBuilder queryBuilder = new StringBuilder();
    String baseURL = parameters[0];
    String setType = parameters[1];
    String histogram = parameters[2];
    String findOp = parameters[3];
    String field = parameters[4];

    queryBuilder.append(baseURL).append("set=").append(setType);
    if (isHistogram && histogram != null && histogram.length() > 0) {
      queryBuilder.append("&type=").append(histogram);
    }
    if (findOp != null && findOp.length() > 0) {
      queryBuilder.append("&find=").append(findOp);
    }
    if (field != null && field.length() > 0) {
      queryBuilder.append(":").append(field);
    }
    if (isHistogram) {
      queryBuilder.append("&histogramOutput=csv");
    }
    String requestedURL = queryBuilder.toString();
    System.out.println(requestedURL);
    return requestedURL;
  }

  private static void checkOutput(String url) throws IOException {
    HttpGet get = new HttpGet(url);
    long start = System.currentTimeMillis();
    HttpResponse res = client.execute(hostPort, get);
    long end = System.currentTimeMillis();
    timeTaken += (end - start);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    int statusCode = res.getStatusLine().getStatusCode();
    if (statusCode == 500) {
      assertThat(strings, hasItem(containsString("not supported")));
    } else {
      assertThat(statusCode, is(200));
    }
    strings.clear();
    count++;
  }

  /* QUERY CHECKER TESTS */

  @Test
  public void testValidQueryChecker() throws Exception {
    HashMap<INodeSet, HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>>>
        setSumTypeFilterConfig = getValidCombinations();
    String[] parameters = new String[4];
    for (INodeSet set : INodeSet.values()) {
      String setType = set.toString();
      parameters[0] = setType;
      HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>> sumTypeFilters =
          setSumTypeFilterConfig.get(set);
      for (Map.Entry<String, HashMap<String, ArrayList<EnumSet<Filter>>>> sumTypeFilter :
          sumTypeFilters.entrySet()) {
        HashMap<String, ArrayList<EnumSet<Filter>>> typeFilters = sumTypeFilter.getValue();
        String sum = sumTypeFilter.getKey();
        parameters[1] = sum;
        for (Map.Entry<String, ArrayList<EnumSet<Filter>>> typeFilter : typeFilters.entrySet()) {
          String type = typeFilter.getKey();
          parameters[2] = type;
          String testingURL = buildQuery(parameters);
          testQuery(testingURL, true);
        }
      }
    }
  }

  @Test
  public void testInvalidQueryChecker() throws Exception {
    HashMap<INodeSet, HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>>>
        setSumTypeFilterConfig = getInvalidCombinations();
    String[] parameters = new String[4];
    for (INodeSet set : INodeSet.values()) {
      String setType = set.toString();
      parameters[0] = setType;
      HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>> sumTypeFilters =
          setSumTypeFilterConfig.get(set);
      for (Map.Entry<String, HashMap<String, ArrayList<EnumSet<Filter>>>> sumTypeFilter :
          sumTypeFilters.entrySet()) {
        HashMap<String, ArrayList<EnumSet<Filter>>> typeFilters = sumTypeFilter.getValue();
        String sum = sumTypeFilter.getKey();
        parameters[1] = sum;
        for (Map.Entry<String, ArrayList<EnumSet<Filter>>> typeFilter : typeFilters.entrySet()) {
          String type = typeFilter.getKey();
          parameters[2] = type;
          String testingURL = buildQuery(parameters);
          testQuery(testingURL, false);
        }
      }
    }
  }

  private static HashMap<INodeSet, HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>>>
      getInvalidCombinations() {
    HashMap<INodeSet, HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>>>
        invalidCombination = new HashMap<>();
    // For 'File' set Type
    HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>> fileSumTypeFilterCombo =
        new HashMap<>();
    EnumSet<Sum> diffFilesDirs = Constants.getDifference(Constants.SUM_DIR, Constants.SUM_FILE);
    // For Each value in 'SUM_FILE'
    for (Sum sum : diffFilesDirs) {
      HashMap<String, ArrayList<EnumSet<Filter>>> typeFilterCombo = new HashMap<>();
      EnumSet<Filter> onlyDir = Constants.getDifference(Constants.FILTER_DIR, Constants.FILTER_ALL);
      // For each value in 'TYPE_FILE' and 'TYPE_ALL'
      for (Histogram typeFile : Constants.TYPE_FILE) {
        ArrayList<EnumSet<Filter>> filterCombo = new ArrayList<>();
        filterCombo.add(onlyDir);

        typeFilterCombo.put(typeFile.toString(), filterCombo);
      }

      for (Histogram typeAll : Constants.TYPE_ALL) {
        ArrayList<EnumSet<Filter>> filterCombo = new ArrayList<>();
        filterCombo.add(onlyDir);

        typeFilterCombo.put(typeAll.toString(), filterCombo);
      }

      fileSumTypeFilterCombo.put(sum.toString(), typeFilterCombo);
    }

    // For 'Dir' set type
    HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>> dirSumTypeFilterCombo =
        new HashMap<>();
    EnumSet<Sum> diffDirsFiles = Constants.getDifference(Constants.SUM_FILE, Constants.SUM_DIR);
    // For Each value in 'SUM_FILE'
    for (Sum sum : diffDirsFiles) {
      HashMap<String, ArrayList<EnumSet<Filter>>> typeFilterCombo = new HashMap<>();
      EnumSet<Filter> onlyFile =
          Constants.getDifference(Constants.FILTER_FILE, Constants.FILTER_ALL);
      // For each value in 'TYPE_ALL' and 'TYPE_FILE'
      for (Histogram typeAll : Constants.TYPE_ALL) {
        ArrayList<EnumSet<Filter>> filterCombo = new ArrayList<>();
        // For each value in 'FILTER_FILE'
        filterCombo.add(onlyFile);
        typeFilterCombo.put(typeAll.toString(), filterCombo);
      }

      for (Histogram typeFile : Constants.TYPE_FILE) {
        ArrayList<EnumSet<Filter>> filterCombo = new ArrayList<>();
        // For each value in 'FILTER_FILE'
        filterCombo.add(onlyFile);
        typeFilterCombo.put(typeFile.toString(), filterCombo);
      }

      dirSumTypeFilterCombo.put(sum.toString(), typeFilterCombo);
    }

    // For 'All' set type
    HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>> allSumTypeFilterCombo =
        new HashMap<>();
    EnumSet<Sum> sumFiles = Constants.getDifference(Constants.SUM_FILE, Constants.SUM_ALL);
    // For Each value in 'SUM_FILE'
    for (Sum sum : sumFiles) {
      HashMap<String, ArrayList<EnumSet<Filter>>> typeFilterCombo = new HashMap<>();
      EnumSet<Filter> onlyFile =
          Constants.getDifference(Constants.FILTER_FILE, Constants.FILTER_ALL);
      EnumSet<Filter> onlyDir = Constants.getDifference(Constants.FILTER_DIR, Constants.FILTER_ALL);
      // For each value in 'TYPE_FILE'

      for (Histogram typeFile : Constants.TYPE_FILE) {
        ArrayList<EnumSet<Filter>> filterCombo = new ArrayList<>();
        // For each value in 'FILTER_FILE' and 'FILTER_DIR'
        filterCombo.add(onlyDir);
        filterCombo.add(onlyFile);

        typeFilterCombo.put(typeFile.toString(), filterCombo);
      }

      allSumTypeFilterCombo.put(sum.toString(), typeFilterCombo);
    }

    // Combine all of them
    invalidCombination.put(INodeSet.files, fileSumTypeFilterCombo);
    invalidCombination.put(INodeSet.dirs, dirSumTypeFilterCombo);
    invalidCombination.put(INodeSet.all, allSumTypeFilterCombo);

    return invalidCombination;
  }

  private static HashMap<INodeSet, HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>>>
      getValidCombinations() {
    HashMap<INodeSet, HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>>>
        validCombination = new HashMap<>();
    // For 'File' set Type
    HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>> fileSumTypeFilterCombo =
        new HashMap<>();
    // For Each value in 'SUM_FILE'
    for (Sum sum : Constants.SUM_FILE) {
      HashMap<String, ArrayList<EnumSet<Filter>>> typeFilterCombo = new HashMap<>();
      // For each value in 'TYPE_FILE' and 'TYPE_ALL'
      for (Histogram typeFile : Constants.TYPE_FILE) {
        ArrayList<EnumSet<Filter>> filterCombo = new ArrayList<>();
        // For each value in 'FILTER_FILE' and 'FILTER_ALL'
        filterCombo.add(Constants.FILTER_FILE);

        typeFilterCombo.put(typeFile.toString(), filterCombo);
      }

      for (Histogram typeAll : Constants.TYPE_ALL) {
        ArrayList<EnumSet<Filter>> filterCombo = new ArrayList<>();
        // For each value in 'FILTER_FILE' and 'FILTER_ALL'
        filterCombo.add(Constants.FILTER_FILE);

        typeFilterCombo.put(typeAll.toString(), filterCombo);
      }

      fileSumTypeFilterCombo.put(sum.toString(), typeFilterCombo);
    }

    // For 'Dir' set type
    HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>> dirSumTypeFilterCombo =
        new HashMap<>();
    // For Each value in 'SUM_ALL'
    for (Sum sum : Constants.SUM_DIR) {
      HashMap<String, ArrayList<EnumSet<Filter>>> typeFilterCombo = new HashMap<>();
      // For each value in 'TYPE_ALL'
      for (Histogram typeAll : Constants.TYPE_DIR) {
        ArrayList<EnumSet<Filter>> filterCombo = new ArrayList<>();
        // For each value in 'FILTER_DIR' and 'FILTER_ALL'
        filterCombo.add(Constants.FILTER_DIR);

        typeFilterCombo.put(typeAll.toString(), filterCombo);
      }

      dirSumTypeFilterCombo.put(sum.toString(), typeFilterCombo);
    }

    // For 'All' set type
    HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>> allSumTypeFilterCombo =
        new HashMap<>();
    // For Each value in 'SUM_ALL'
    for (Sum sum : Constants.SUM_ALL) {
      HashMap<String, ArrayList<EnumSet<Filter>>> typeFilterCombo = new HashMap<>();
      // For each value in 'TYPE_ALL'

      for (Histogram typeAll : Constants.TYPE_ALL) {
        ArrayList<EnumSet<Filter>> filterCombo = new ArrayList<>();
        // For each value in 'FILTER_ALL'
        filterCombo.add(Constants.FILTER_ALL);

        typeFilterCombo.put(typeAll.toString(), filterCombo);
      }

      allSumTypeFilterCombo.put(sum.toString(), typeFilterCombo);
    }

    // Combine all of them
    validCombination.put(INodeSet.files, fileSumTypeFilterCombo);
    validCombination.put(INodeSet.dirs, dirSumTypeFilterCombo);
    validCombination.put(INodeSet.all, allSumTypeFilterCombo);

    return validCombination;
  }

  private static String buildQuery(String[] parameters) {
    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append("http://localhost:4567/histogram?");
    String setType = parameters[0];
    String sum = parameters[1];
    String type = parameters[2];

    queryBuilder.append("set=").append(setType);
    // Also add filterOps
    if (type != null && type.length() > 0) {
      queryBuilder.append("&type=").append(type);
    }
    if (sum != null && sum.length() > 0) {
      queryBuilder.append("&sum=").append(sum);
    }
    String requestedURL = queryBuilder.toString();
    System.out.println(requestedURL);
    return requestedURL;
  }

  private static void testQuery(String requestedURL, boolean isValid) throws Exception {
    HttpGet get = new HttpGet(requestedURL);
    long start = System.currentTimeMillis();
    HttpResponse res = client.execute(hostPort, get);
    long end = System.currentTimeMillis();
    timeTaken += (end - start);
    List<String> strings = IOUtils.readLines(res.getEntity().getContent());
    if (isValid) {
      int statusCode = res.getStatusLine().getStatusCode();
      if (statusCode == 500) {
        assertThat(strings, hasItem(containsString("not supported")));
      } else {
        assertThat(statusCode, is(200));
      }
    } else {
      assertThat(res.getStatusLine().getStatusCode(), is(400));
    }
    strings.clear();
    count++;
  }
}
