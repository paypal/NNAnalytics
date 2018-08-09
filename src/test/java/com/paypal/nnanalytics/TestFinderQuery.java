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

package com.paypal.nnanalytics;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import com.paypal.namenode.WebServerMain;
import com.paypal.security.SecurityConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.Constants;
import org.apache.hadoop.hdfs.server.namenode.Constants.Find;
import org.apache.hadoop.hdfs.server.namenode.Constants.FindField;
import org.apache.hadoop.hdfs.server.namenode.Constants.Histogram;
import org.apache.hadoop.hdfs.server.namenode.Constants.INodeSet;
import org.apache.hadoop.hdfs.server.namenode.GSetGenerator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
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
  private static WebServerMain nna;
  private static int count = 0;
  private static long timeTaken = 0;

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

  public static HashMap<INodeSet, HashMap<Find, ArrayList<FindField>>> getSetFilterFindConfig() {
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

  public static HashMap<INodeSet, HashMap<Histogram, HashMap<Find, ArrayList<FindField>>>>
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
