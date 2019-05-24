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

package org.apache.hadoop.hdfs.server.namenode.queries;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLoader;
import org.slf4j.Logger;

public class Histograms {

  public static final Logger LOG = NameNodeLoader.LOG;

  /**
   * Converts given histogram along with title, x and y lables to json string for Chart.js.
   *
   * @param histogram data points of histogram
   * @param title title to be used for chart
   * @param ylabel xlabel to be used for chart
   * @param xlabel ylabel to be used for chart
   * @return json string containing data points to be used as input to Chart.js for rendering on
   *     HTML canvas pages
   */
  public static String toChartJsJson(
      Map<String, Long> histogram, String title, String ylabel, String xlabel) {
    final long s1 = System.currentTimeMillis();

    final Collection<Long> dataArray = histogram.values();
    Set<String> labels = histogram.keySet();
    Map<String, Object> data = new HashMap<>();
    data.put("labels", labels);
    data.put("xlabel", xlabel);
    data.put("ylabel", ylabel);
    ArrayList<HashMap<String, Object>> datasets = new ArrayList<>();
    HashMap<String, Object> dataset = new HashMap<>();
    dataset.put("label", title);
    dataset.put("data", dataArray);
    datasets.add(dataset);
    data.put("datasets", datasets);

    long e1 = System.currentTimeMillis();
    String gson = new Gson().toJson(data);
    LOG.debug(
        "Time to convert histogram to JSON (for chart.js) of "
            + gson.length()
            + " chars took: "
            + (e1 - s1)
            + " ms.");
    return gson;
  }

  /**
   * Converts given set of values to JSON String.
   *
   * @param set set of values to be converted to json
   * @return json string
   */
  public static String toJson(Object set) {
    long s1 = System.currentTimeMillis();
    String gson = new Gson().toJson(set);
    long e1 = System.currentTimeMillis();
    LOG.debug("Time to convert object to JSON of {} chars took: {} ms.", gson.length(), (e1 - s1));
    return gson;
  }

  /**
   * Converts given histogram data points to CSV String.
   *
   * @param histogram data points of histogram
   * @param find specifies field to be included as date (accessTime or modTime)
   * @return csv string of histogram
   */
  public static String toCsv(Map<String, Long> histogram, String find, boolean rawTimestamp) {
    long s1 = System.currentTimeMillis();

    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Long> entry : histogram.entrySet()) {
      if (rawTimestamp || find == null || find.length() == 0) {
        sb.append(entry.getKey()).append(',').append(entry.getValue().toString()).append('\n');
      } else {
        String[] finds = find.split(":");
        String findField = finds[1];
        switch (findField) {
          case "accessTime":
          case "modTime":
            sb.append(entry.getKey()).append(',').append(new Date(entry.getValue())).append('\n');
            break;
          default:
            sb.append(entry.getKey()).append(',').append(entry.getValue().toString()).append('\n');
            break;
        }
      }
    }

    long e1 = System.currentTimeMillis();
    String csv = sb.toString();
    LOG.info(
        "Time to dump histogram to CSV String of {} chars took: {} ms.", csv.length(), (e1 - s1));
    return csv;
  }

  /**
   * This function converts histogram map to csv string
   *
   * @param histogram data points of histogram
   * @return csv string of the input histogram.
   */
  public static String toCsv(Map<String, List<Long>> histogram) {
    long s1 = System.currentTimeMillis();

    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, List<Long>> entry : histogram.entrySet()) {
      sb.append(entry.getKey());
      for (Long val : entry.getValue()) {
        sb.append(',');
        sb.append(val);
      }
      sb.append('\n');
    }

    long e1 = System.currentTimeMillis();
    String csv = sb.toString();
    LOG.debug(
        "Time to dump histogram2 to CSV String of {} chars took: {} ms.", csv.length(), (e1 - s1));
    return csv;
  }

  /**
   * Result is a histogram with only the top 'top' number of results. Top being those with the
   * highest long values in the positive direction.
   *
   * @param histogram data points of histogram
   * @param top the number of top(largest) elements by value to be included from given histogram
   * @return sliced histogram with top elements
   */
  public static Map<String, Long> sliceToTop(Map<String, Long> histogram, int top) {
    return slice(histogram, new BiggerValueComperator(), top);
  }

  /**
   * Result is a histogram with only the bottom 'bottom' number of results. Bottom being those with
   * the lowest long values in the negative direction.
   *
   * @param histogram data points of histogram
   * @param bottom the number of bottom(smallest) elements by value to include from given histogram
   * @return sliced histogram with bottom elements
   */
  public static Map<String, Long> sliceToBottom(Map<String, Long> histogram, int bottom) {
    return slice(histogram, new BiggerValueComperator().reversed(), bottom);
  }

  private static Map<String, Long> slice(
      Map<String, Long> histogram, Comparator<Map.Entry<String, Long>> comparator, int limit) {
    return histogram
        .entrySet()
        .parallelStream()
        .sorted(comparator)
        .limit(limit)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Result is a histogram ordered by the key order.
   *
   * @param map the output map
   * @param keys the ordered key list
   * @return histogram with 0s for empty keys and ordered by key list
   */
  public static Map<String, Long> orderByKeyOrder(Map<String, Long> map, List<String> keys) {
    LinkedHashMap<String, Long> orderedMap = new LinkedHashMap<>(map.size());
    for (String key : keys) {
      orderedMap.put(key, map.getOrDefault(key, 0L));
    }
    return orderedMap;
  }

  /**
   * Result is a histogram sorted by its values.
   *
   * @param <K> the type of the map keys
   * @param <V> the type of the map values
   * @param map map values to be sorted
   * @param ascending indicates whether ascending or descending sort order
   * @return sorted map
   */
  public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(
      Map<K, V> map, boolean ascending) {
    return map.entrySet()
        .stream()
        .sorted(
            ascending
                ? Map.Entry.comparingByValue()
                : Map.Entry.comparingByValue(Collections.reverseOrder()))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
  }

  /**
   * Result is a histogram sorted by its values.
   *
   * @param <K> the type of the map keys
   * @param <V> the type of the map values
   * @param map map values to be sorted
   * @param ascending indicates whether ascending or descending sort order
   * @return sorted map
   */
  public static <K, V extends Comparable<? super V>> Map<K, List<V>> sortByValue(
      Map<K, List<V>> map, final int sortIndex, boolean ascending) {

    return map.entrySet()
        .stream()
        .sorted(
            ascending
                ? Comparator.comparing((Map.Entry<K, List<V>> c) -> c.getValue().get(sortIndex))
                : Comparator.comparing((Map.Entry<K, List<V>> c) -> c.getValue().get(sortIndex))
                    .reversed())
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
  }
}
