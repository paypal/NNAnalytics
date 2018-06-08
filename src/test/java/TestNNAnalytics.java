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
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.StringContains.containsString;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import com.paypal.namenode.NNAnalyticsRestAPI;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestNNAnalytics {

  private static HttpHost hostPort;
  private static HttpClient client;
  private static NNAnalyticsRestAPI nna;

  public static void main(String[] args) throws Exception {
    beforeClass();
    while (true) {
      // Let the server run.
    }
  }

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
  public void testHasQouta() throws IOException {
    HttpGet get =
        new HttpGet("http://localhost:4567/filter?set=dirs&filters=hasQuota:eq:true&sum=count");
    HttpResponse res = client.execute(hostPort, get);
    List<String> result = IOUtils.readLines(res.getEntity().getContent());
    assertThat(result.size(), is(1));
    assertThat(result.get(0), is(not(0L)));
    assertThat(res.getStatusLine().getStatusCode(), is(200));
  }

  @Test
  public void testHasQoutaList() throws IOException {
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

  @Ignore("Operations are not ready yet")
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
      if (statusCode != 400) {
        assertThat(statusCode, is(200));
      } else {
        break;
      }
    }
    assertThat(statusCode, is(400));
  }

  @Ignore("Operations are not ready yet")
  @Test
  public void testGetNonExistantDelete() throws IOException, InterruptedException {
    HttpGet get = new HttpGet("http://localhost:4567/abortOperation?identity=FAKEID");
    HttpResponse res = client.execute(hostPort, get);
    assertThat(res.getStatusLine().getStatusCode(), is(400));
  }

  @Ignore("Operations are not ready yet")
  @Test
  public void testAbortNonExistantDelete() throws IOException, InterruptedException {
    HttpGet delete = new HttpGet("http://localhost:4567/listOperations?identity=FAKEID");
    HttpResponse res = client.execute(hostPort, delete);
    assertThat(res.getStatusLine().getStatusCode(), is(400));
  }

  @Ignore("Operations are not ready yet")
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
    assertThat(result.get(0), is(String.valueOf(GSetGenerator.TOTAL_MADE.apply(null) - 1)));
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

  private static JsonArray getJsonDataArray(JsonObject json) {
    JsonArray datasets = json.getAsJsonArray("datasets");
    for (JsonElement next : datasets) {
      if (next.isJsonObject() && next.getAsJsonObject().has("data")) {
        return next.getAsJsonObject().get("data").getAsJsonArray();
      }
    }
    throw new IllegalStateException("No data found in histogram.");
  }
}
