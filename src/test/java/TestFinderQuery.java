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
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.paypal.namenode.NNAnalyticsRestAPI;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.server.namenode.GSetGenerator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.NNAConstants;
import org.apache.hadoop.util.GSet;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestFinderQuery {

  private static HttpHost hostPort;
  private static DefaultHttpClient client;
  private static NNAnalyticsRestAPI nna;
  private static int count = 0;
  private static long timeTaken = 0;

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
    System.out.println("Total # of completed query check for benchmarking: " + count);
    System.out.println("Total time taken in milliseconds: " + timeTaken);
  }

  @Before
  public void before() {
    client = new DefaultHttpClient();
    count = 0;
    timeTaken = 0;
  }

  public static HashMap<
          NNAConstants.SET, HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>>>
      getSetFilterFindConfig() {
    HashMap<NNAConstants.SET, HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>>>
        config = new HashMap<>();

    // For 'File' set type
    HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>> fileFilterFind = new HashMap<>();
    ArrayList<NNAConstants.FIND_FIELD> fileFind = new ArrayList<>();
    fileFind.addAll(NNAConstants.FIND_FILE);
    fileFilterFind.put(NNAConstants.FIND.max, fileFind);
    fileFilterFind.put(NNAConstants.FIND.min, fileFind);
    config.put(NNAConstants.SET.files, fileFilterFind);

    // For 'Dir' set type
    HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>> dirFilterFind = new HashMap<>();
    ArrayList<NNAConstants.FIND_FIELD> dirFind = new ArrayList<>();
    dirFind.addAll(NNAConstants.FIND_DIR);
    dirFilterFind.put(NNAConstants.FIND.max, dirFind);
    dirFilterFind.put(NNAConstants.FIND.min, dirFind);
    config.put(NNAConstants.SET.dirs, dirFilterFind);

    // For 'All' set type
    HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>> allFilterFind = new HashMap<>();
    ArrayList<NNAConstants.FIND_FIELD> allFind = new ArrayList<>();
    allFind.addAll(NNAConstants.FIND_ALL);
    allFilterFind.put(NNAConstants.FIND.max, allFind);
    allFilterFind.put(NNAConstants.FIND.min, allFind);
    config.put(NNAConstants.SET.all, allFilterFind);

    return config;
  }

  public static HashMap<
          NNAConstants.SET,
          HashMap<
              NNAConstants.HISTOGRAM,
              HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>>>>
      getSetHistogramTypeFindConfig() {
    HashMap<
            NNAConstants.SET,
            HashMap<
                NNAConstants.HISTOGRAM,
                HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>>>>
        config = new HashMap<>();

    // For 'File' set type
    HashMap<NNAConstants.HISTOGRAM, HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>>>
        fileFindField = new HashMap<>();
    HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>> findFile = new HashMap<>();
    ArrayList<NNAConstants.FIND_FIELD> fileFind = new ArrayList<>();
    fileFind.addAll(NNAConstants.FIND_FILE);
    findFile.put(NNAConstants.FIND.max, fileFind);
    findFile.put(NNAConstants.FIND.min, fileFind);
    for (NNAConstants.HISTOGRAM typeFile : NNAConstants.TYPE_FILE) {
      fileFindField.put(typeFile, findFile);
    }
    config.put(NNAConstants.SET.files, fileFindField);

    // For 'Dir' set type
    HashMap<NNAConstants.HISTOGRAM, HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>>>
        dirFindField = new HashMap<>();
    HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>> findDir = new HashMap<>();
    ArrayList<NNAConstants.FIND_FIELD> dirFind = new ArrayList<>();
    dirFind.addAll(NNAConstants.FIND_DIR);
    findDir.put(NNAConstants.FIND.max, dirFind);
    findDir.put(NNAConstants.FIND.min, dirFind);
    for (NNAConstants.HISTOGRAM typeDir : NNAConstants.TYPE_DIR) {
      dirFindField.put(typeDir, findDir);
    }
    config.put(NNAConstants.SET.dirs, dirFindField);

    // For 'All' set type
    HashMap<NNAConstants.HISTOGRAM, HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>>>
        allFindField = new HashMap<>();
    HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>> findAll = new HashMap<>();
    ArrayList<NNAConstants.FIND_FIELD> allFind = new ArrayList<>();
    allFind.addAll(NNAConstants.FIND_ALL);
    findAll.put(NNAConstants.FIND.max, allFind);
    findAll.put(NNAConstants.FIND.min, allFind);
    for (NNAConstants.HISTOGRAM typeAll : NNAConstants.TYPE_ALL) {
      allFindField.put(typeAll, findAll);
    }
    config.put(NNAConstants.SET.all, allFindField);

    return config;
  }

  @Test
  public void testFilterAndFindQuery() throws IOException {
    HashMap<NNAConstants.SET, HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>>>
        config = getSetFilterFindConfig();
    String[] parameters = new String[5];
    parameters[0] = "http://localhost:4567/filter?";
    for (NNAConstants.SET setType : NNAConstants.SET.values()) {
      parameters[1] = setType.toString();
      HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>> findFields =
          config.get(setType);
      for (Map.Entry<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>> findField :
          findFields.entrySet()) {
        parameters[3] = findField.getKey().toString();
        for (NNAConstants.FIND_FIELD field : findField.getValue()) {
          parameters[4] = field.toString();
          String testingURL = buildFindQuery(parameters, false);
          checkOutput(testingURL);
        }
      }
    }
  }

  @Test
  public void testHistogramTypeAndFindQuery() throws IOException {
    HashMap<
            NNAConstants.SET,
            HashMap<
                NNAConstants.HISTOGRAM,
                HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>>>>
        config = getSetHistogramTypeFindConfig();
    String[] parameters = new String[5];
    parameters[0] = "http://localhost:4567/histogram?";
    for (NNAConstants.SET setType : NNAConstants.SET.values()) {
      parameters[1] = setType.toString();
      HashMap<
              NNAConstants.HISTOGRAM,
              HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>>>
          histFindFields = config.get(setType);
      for (Map.Entry<
              NNAConstants.HISTOGRAM,
              HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>>>
          histFindField : histFindFields.entrySet()) {
        parameters[2] = histFindField.getKey().toString();
        HashMap<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>> findFields =
            histFindField.getValue();
        for (Map.Entry<NNAConstants.FIND, ArrayList<NNAConstants.FIND_FIELD>> findField :
            findFields.entrySet()) {
          parameters[3] = findField.getKey().toString();
          for (NNAConstants.FIND_FIELD field : findField.getValue()) {
            parameters[4] = field.toString();
            String requestedURL = buildFindQuery(parameters, true);
            checkOutput(requestedURL);
          }
        }
      }
    }
  }

  public static String buildFindQuery(String[] parameters, boolean isHistogram) {
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

  public static void checkOutput(String url) throws IOException {
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
}
