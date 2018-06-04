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
import org.apache.hadoop.hdfs.server.namenode.NNLoader;
import org.slf4j.Logger;

public class Histograms {

  public static final Logger LOG = NNLoader.LOG;

  /**
   * converts given histogram along with title, x and y lables to json string for Chart.js
   *
   * @param histogram data points of histogram
   * @param title title to be used for chart
   * @param ylabel xlabel to be used for chart
   * @param xlabel ylabel to be used for chart
   * @return json string containing data points to be used as input to Chart.js for rendering on HTML canvas pages.
   */
  public static String toChartJsJson(Map<String, Long> histogram, String title, String ylabel,
      String xlabel) {
    long s1 = System.currentTimeMillis();

    Collection<Long> data_array = histogram.values();
    Set<String> labels = histogram.keySet();
    Map<String, Object> data = new HashMap<>();
    data.put("labels", labels);
    data.put("xlabel", xlabel);
    data.put("ylabel", ylabel);
    ArrayList<HashMap<String, Object>> datasets = new ArrayList<>();
    HashMap<String, Object> dataset = new HashMap<>();
    dataset.put("label", title);
    dataset.put("data", data_array);
    datasets.add(dataset);
    data.put("datasets", datasets);

    long e1 = System.currentTimeMillis();
    String gson = new Gson().toJson(data);
    LOG.info(
        "Time to convert histogram to JSON (for chart.js) of " + gson.length() + " chars took: " + (
            e1 - s1) + " ms.");
    return gson;
  }

  /**
   * converts given set of values to json string.
   * @param set set of values to be converted to json
   * @return json string
   */
  public static String toJson(Object set) {
    long s1 = System.currentTimeMillis();
    String gson = new Gson().toJson(set);
    long e1 = System.currentTimeMillis();
    LOG.info("Time to convert object to JSON of {} chars took: {} ms.", gson.length(), (e1 - s1));
    return gson;
  }

  /**
   * converts given histogram data points to csv string
   *
   * @param histogram data points of histogram
   * @param find      specifies field to be included as date (accessTime or modTime)
   * @return csv string of histogram
   */
  public static String toCSV(Map<String, Long> histogram, String find) {
    long s1 = System.currentTimeMillis();

    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Long> entry : histogram.entrySet()) {
      if (find == null || find.length() == 0) {
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
    LOG.info("Time to dump histogram to CSV String of {} chars took: {} ms.", csv.length(), (e1 - s1));
    return csv;
  }

  /**
   * This function converts histogram map to csv string
   *
   * @param histogram data points of histogram
   * @return csv string of the input histogram.
   */
  public static String toCSV(Map<String, List<Long>> histogram) {
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
    LOG.info(
        "Time to dump histogram2 to CSV String of {} chars took: {} ms.", csv.length(), (e1 - s1));
    return csv;
  }

  /**
   * Result is a histogram with only the top 'top' number of results. Top being those with the
   * highest long values in the positive direction.
   *
   * @param histogram data points of histogram
   * @param top       the number of top(largest) elements by value to be included from given histogram
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
   * @param bottom    the number of bottom(smallest) elements by value to include from given histogram
   * @return sliced histogram with bottom elements
   */
  public static Map<String, Long> sliceToBottom(Map<String, Long> histogram, int bottom) {
    return slice(histogram, new BiggerValueComperator().reversed(), bottom);
  }

  private static Map<String, Long> slice(Map<String, Long> histogram,
      Comparator<Map.Entry<String, Long>> comparator,
      int limit) {
    return histogram.entrySet().parallelStream()
        .sorted(comparator)
        .limit(limit)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Creates sorted histogram from given list of keys and values corresponding to the keys. As long as
   * the keys are in sorted order the output histogram will be as well.
   *
   * @param keys      list of keys for histogram ex: ["key1", "key2"]
   * @param histogram corresponding values for the given keys ex: [100,200]
   * @return map of input key and value as histogram ex: ["key1":100,"key2":200]
   */
  public static Map<String, Long> sortByKeys(List<String> keys,
      long[] histogram) {
    if (histogram.length == 0) {
      return Collections.emptyMap();
    }
    Map<String, Long> sortedHistogram = new LinkedHashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      String column = keys.get(i);
      sortedHistogram.put(column, histogram[i]);
    }
    int lastColumnIndex = keys.size() - 1;
    String lastColumn = keys.get(lastColumnIndex);
    sortedHistogram.put(lastColumn + "+", histogram[lastColumnIndex + 1]);
    return sortedHistogram;
  }

  /**
   * Creates a mapped histogram based on the input and the keys that go with the input. The Map
   * key's values are used an integers to index into the parameter long array.
   *
   * @param binKeyMap map of key and value index in histogram, ex: ["key1":1L,"key2":0L]
   * @param histogram list of values for the keys, ex: [100,200]
   * @return map of input key and value as histogram, ex: ["key1":200,"key2":100].
   */
  public static Map<String, Long> mapByKeys(Map<String, Long> binKeyMap,
      long[] histogram) {
    if (histogram.length == 0) {
      return Collections.emptyMap();
    }
    Map<String, Long> sortedHistogram = new LinkedHashMap<>();
    for (Map.Entry<String, Long> entry : binKeyMap.entrySet()) {
      sortedHistogram.put(entry.getKey(), histogram[entry.getValue().intValue()]);
    }
    int notMappedIndice = histogram.length - 1;
    if (notMappedIndice >= 0) {
      long notMappedSum = histogram[notMappedIndice];
      if (notMappedSum != 0L) {
        sortedHistogram.put("NO_MAPPING", notMappedSum);
      }
    }
    return sortedHistogram;
  }

    /**
     * Creates a mapped histogram based on the input and the keys that go with the input. Long array
     * values are used as keys; any "0" values in array are not mapped.
     *
     * @param histogram list of long data point values, ex: [100,0,200,300]
     * @return mapped histogram based on the input and the keys that go with the input, ex: ["0":100,"2":200,"2+":300].
     */
  public static Map<String, Long> mapToNonEmptyIndex(long[] histogram) {
    if (histogram.length == 0) {
      return Collections.emptyMap();
    }
    Map<String, Long> sortedHistogram = new LinkedHashMap<>();
    for (int i = 0; i < histogram.length; i++) {
      long currentColumnValue = histogram[i];
      if (currentColumnValue != 0L) {
        sortedHistogram.put(Integer.toString(i), histogram[i]);
      }
    }
    int lastColumnIndex = histogram.length - 1;
    String lastColumn = Integer.toString(lastColumnIndex);
    long lastColumnValue = histogram[lastColumnIndex];
    if (lastColumnValue != 0L) {
      sortedHistogram.put(lastColumn + "+", histogram[lastColumnIndex]);
    }
    return sortedHistogram;
  }

  /**
   * Result is a histogram sorted by its values.
   *
   * @param <K>       the type of the map keys
   * @param <V>       the type of the map values
   * @param map       map values to be sorted
   * @param ascending indicates whether ascending or descending sort order
   * @return sorted map
   */
  public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map,
      boolean ascending) {
    return map.entrySet()
        .stream()
        .sorted(ascending ? Map.Entry.comparingByValue()
            : Map.Entry.comparingByValue(Collections.reverseOrder()))
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue,
            (e1, e2) -> e1,
            LinkedHashMap::new
        ));
  }

  public static <K, V extends Comparable<? super V>> Map<K, List<V>> sortByValue(
      Map<K, List<V>> map,
      final int sortIndex,
      boolean ascending) {

    return map.entrySet()
        .stream()
        .sorted(ascending ? Comparator
            .comparing((Map.Entry<K, List<V>> c) -> c.getValue().get(sortIndex)) :
            Comparator.comparing((Map.Entry<K, List<V>> c) -> c.getValue().get(sortIndex))
                .reversed())
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue,
            (e1, e2) -> e1,
            LinkedHashMap::new
        ));
  }
}
