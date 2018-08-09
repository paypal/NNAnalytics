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
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.Constants;
import org.apache.hadoop.hdfs.server.namenode.Constants.Filter;
import org.apache.hadoop.hdfs.server.namenode.Constants.Histogram;
import org.apache.hadoop.hdfs.server.namenode.Constants.INodeSet;
import org.apache.hadoop.hdfs.server.namenode.Constants.Sum;
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
public class TestQueryChecker {

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

  public static HashMap<INodeSet, HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>>>
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

  public static HashMap<INodeSet, HashMap<String, HashMap<String, ArrayList<EnumSet<Filter>>>>>
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

  public static String buildQuery(String[] parameters) {
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

  public static void testQuery(String requestedURL, boolean isValid) throws Exception {
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
