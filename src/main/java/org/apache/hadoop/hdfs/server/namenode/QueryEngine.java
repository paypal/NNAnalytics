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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.queries.FileTypeHistogram;
import org.apache.hadoop.hdfs.server.namenode.queries.Histograms;
import org.apache.hadoop.hdfs.server.namenode.queries.MemorySizeHistogram;
import org.apache.hadoop.hdfs.server.namenode.queries.SpaceSizeHistogram;
import org.apache.hadoop.hdfs.server.namenode.queries.TimeHistogram;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryEngine {

  public static final Logger LOG = LoggerFactory.getLogger(QueryEngine.class.getName());

  private VersionInterface versionLoader;

  public QueryEngine() {}

  public void setVersionLoader(VersionInterface versionLoader) {
    this.versionLoader = versionLoader;
  }

  public Collection<INode> combinedFilter(
      Collection<INode> inodes, String[] filters, String[] filterOps) {
    final ArrayList<Function<INode, Boolean>> filterArray = new ArrayList<>();

    for (int i = 0; i < filters.length; i++) {
      String filter = filters[i];
      String[] filterOp = filterOps[i].split(":");
      Function<INode, Boolean> filterFunc = getFilter(filter, filterOp);
      filterArray.add(filterFunc);
    }

    if (filterArray.size() == 0) {
      return inodes;
    }

    long start = System.currentTimeMillis();
    try {
      Stream<INode> stream = StreamSupport.stream(inodes.spliterator(), true);
      for (Function<INode, Boolean> filter : filterArray) {
        stream = stream.filter(filter::apply);
      }
      return stream.collect(Collectors.toList());
    } finally {
      long end = System.currentTimeMillis();
      LOG.info(
          "Performing filters: {} with filterOps: {} took: {} ms.",
          Arrays.asList(filters),
          Arrays.asList(filterOps),
          (end - start));
    }
  }

  public Collection<INode> findFilter(Collection<INode> inodes, String find) {

    if (find == null || find.isEmpty()) {
      return inodes;
    }

    String[] findOps = find.split(":");
    Function<INode, Long> findToLong = getFilterFunctionToLongForINode(findOps[1]);

    long start = System.currentTimeMillis();
    Optional<INode> optional;
    try {
      Stream<INode> stream = inodes.parallelStream();
      switch (findOps[0]) {
        case "max":
          optional = stream.max(Comparator.comparingLong(findToLong::apply));
          break;
        case "min":
          optional = stream.min(Comparator.comparingLong(findToLong::apply));
          break;
        default:
          throw new IllegalArgumentException("Unknown find query type: " + findOps[0]);
      }
    } finally {
      long end = System.currentTimeMillis();
      LOG.info("Performing find: {} took: {} ms.", Arrays.asList(findOps), (end - start));
    }

    return optional.<Collection<INode>>map(Collections::singleton).orElseGet(Collections::emptySet);
  }

  private Function<INode, Boolean> getFilter(String filter, String[] filterOps) {
    long start = System.currentTimeMillis();
    try {
      // Values for all other filters
      String op = filterOps[0];
      String opValue = filterOps[1];

      // Long value filters
      Function<INode, Long> longFunction = getFilterFunctionToLongForINode(filter);
      if (longFunction != null) {
        Function<Long, Boolean> longCompFunction =
            getFilterFunctionForLong(Long.parseLong(opValue), op);
        return longFunction.andThen(longCompFunction);
      }

      // String value filters
      Function<INode, String> strFunction = getFilterFunctionToStringForINode(filter);
      if (strFunction != null) {
        Function<String, Boolean> strCompFunction = getFilterFunctionForString(opValue, op);
        return strFunction.andThen(strCompFunction);
      }

      // Boolean value filters
      Function<INode, Boolean> boolFunction = getFilterFunctionToBooleanForINode(filter);
      if (boolFunction != null) {
        Function<Boolean, Boolean> boolCompFunction =
            getFilterFunctionForBoolean(Boolean.parseBoolean(opValue), op);
        return boolFunction.andThen(boolCompFunction);
      }

      throw new IllegalArgumentException(
          "Failed to determine filter: "
              + filter
              + ", with operations: "
              + Arrays.asList(filterOps)
              + ".\nCheck your filter arguments."
              + "\nPossible filters and operations available at /filters and /filterOps.");
    } finally {
      long end = System.currentTimeMillis();
      LOG.info(
          "Obtaining filter: {} with filterOps:{} took: {} ms.",
          filter,
          Arrays.asList(filterOps),
          (end - start));
    }
  }

  public Long sum(Collection<INode> inodes, String sum) {
    long startTime = System.currentTimeMillis();
    try {
      Function<Collection<INode>, Long> sumFunction = getSumFunctionForCollection(sum);
      return sumFunction.apply(inodes);
    } finally {
      long endTime = System.currentTimeMillis();
      LOG.info("Performing sum: {} took: {} ms.", sum, (endTime - startTime));
    }
  }

  public Function<INode, Long> getFilterFunctionToLongForINode(String filter) {
    switch (filter) {
      case "fileSize":
        return node -> node.asFile().computeFileSize();
      case "diskspaceConsumed":
        return node -> node.asFile().computeFileSize() * node.asFile().getFileReplication();
      case "fileReplica":
        return node -> ((long) node.asFile().getFileReplication());
      case "numBlocks":
        return node -> ((long) node.asFile().numBlocks());
      case "numReplicas":
        return node -> ((long) node.asFile().numBlocks() * node.asFile().getFileReplication());
      case "accessTime":
        return INode::getAccessTime;
      case "modTime":
        return INode::getModificationTime;
      case "memoryConsumed":
        return node -> {
          long inodeSize = 100L;
          if (node.isFile()) {
            inodeSize += node.asFile().numBlocks() * 150L;
          }
          return inodeSize;
        };
      case "depth":
        return node -> {
          String path = node.getFullPathName();
          int depth = 0;
          int slash = path.length() == 1 && path.charAt(0) == '/' ? -1 : 0;
          while (slash != -1) {
            depth++;
            slash = path.indexOf(Path.SEPARATOR, slash + 1);
          }
          return (long) depth;
        };
      case "permission":
        return node -> Long.valueOf(Integer.toOctalString(node.getFsPermissionShort()));
      default:
        return versionLoader.getFilterFunctionToLongForINode(filter);
    }
  }

  public Function<INode, String> getFilterFunctionToStringForINode(String filter) {
    switch (filter) {
      case "name":
        return INode::getLocalName;
      case "path":
        return INode::getFullPathName;
      case "user":
        return INode::getUserName;
      case "group":
        return INode::getGroupName;
      case "modDate":
        return n -> {
          SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
          try {
            Date date = new Date(n.getModificationTime());
            return sdf.format(date);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };
      case "accessDate":
        return n -> {
          SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
          try {
            Date date = new Date(n.getAccessTime());
            return sdf.format(date);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        };
      default:
        return null;
    }
  }

  public Function<INode, Boolean> getFilterFunctionToBooleanForINode(String filter) {
    switch (filter) {
      case "isUnderConstruction":
        return node -> node.asFile().isUnderConstruction();
      case "isWithSnapshot":
        return node -> node.asFile().isWithSnapshot();
      case "hasAcl":
        return node -> (node.getAclFeature() != null);
      default:
        return versionLoader.getFilterFunctionToBooleanForINode(filter);
    }
  }

  private Function<Collection<INode>, Long> getSumFunctionForCollection(String sum) {
    switch (sum) {
      case "count":
        return collection -> ((long) collection.size());
      case "fileSize":
        return collection ->
            StreamSupport.stream(collection.spliterator(), true)
                .mapToLong(node -> node.asFile().computeFileSize())
                .sum();
      case "diskspaceConsumed":
        return collection ->
            StreamSupport.stream(collection.spliterator(), true)
                .mapToLong(
                    node -> node.asFile().computeFileSize() * node.asFile().getFileReplication())
                .sum();
      case "blockSize":
        return collection ->
            StreamSupport.stream(collection.spliterator(), true)
                .mapToLong(node -> node.asFile().getPreferredBlockSize())
                .sum();
      case "numBlocks":
        return collection ->
            StreamSupport.stream(collection.spliterator(), true)
                .mapToLong(node -> node.asFile().numBlocks())
                .sum();
      case "numReplicas":
        return collection ->
            StreamSupport.stream(collection.spliterator(), true)
                .mapToLong(
                    node -> ((long) node.asFile().numBlocks() * node.asFile().getFileReplication()))
                .sum();
      case "memoryConsumed":
        return collection ->
            StreamSupport.stream(collection.spliterator(), true)
                .mapToLong(
                    node -> {
                      long inodeSize = 100L;
                      if (node.isFile()) {
                        inodeSize += node.asFile().numBlocks() * 150L;
                      }
                      return inodeSize;
                    })
                .sum();
      default:
        throw new IllegalArgumentException(
            "Could not determine sum type: " + sum + ".\nPlease check /sums for available sums.");
    }
  }

  Function<INode, Long> getSumFunctionForINode(String sum) {
    switch (sum) {
      case "count":
        return node -> 1L;
      case "fileSize":
        return node -> node.asFile().computeFileSize();
      case "diskspaceConsumed":
        return node -> node.asFile().computeFileSize() * node.asFile().getFileReplication();
      case "blockSize":
        return node -> node.asFile().getPreferredBlockSize();
      case "numBlocks":
        return node -> ((long) node.asFile().numBlocks());
      case "numReplicas":
        return node -> ((long) node.asFile().numBlocks() * node.asFile().getFileReplication());
      case "memoryConsumed":
        return node -> {
          long inodeSize = 100L;
          if (node.isFile()) {
            inodeSize += node.asFile().numBlocks() * 150L;
          }
          return inodeSize;
        };
      case "nsQuotaRatioUsed":
        return node ->
            (long)
                ((double) versionLoader.getNSQuotaUsed(node)
                    / (double) versionLoader.getNSQuota(node)
                    * 100);
      case "dsQuotaRatioUsed":
        return node ->
            (long)
                ((double) versionLoader.getDSQuotaUsed(node)
                    / (double) versionLoader.getDSQuota(node)
                    * 100);
      case "nsQuotaUsed":
        return versionLoader::getNSQuotaUsed;
      case "dsQuotaUsed":
        return versionLoader::getDSQuotaUsed;
      case "nsQuota":
        return versionLoader::getNSQuota;
      case "dsQuota":
        return versionLoader::getDSQuota;
      default:
        throw new IllegalArgumentException(
            "Could not determine sum type: " + sum + ".\nPlease check /sums for available sums.");
    }
  }

  public Function<String, Boolean> getFilterFunctionForString(String value, String op) {
    switch (op) {
      case "eq":
        return s -> s.equals(value);
      case "notEq":
        return s -> !s.equals(value);
      case "startsWith":
        return s -> s.startsWith(value);
      case "notStartsWith":
        return s -> !s.startsWith(value);
      case "endsWith":
        return s -> s.endsWith(value);
      case "notEndsWith":
        return s -> !s.endsWith(value);
      case "contains":
        return s -> s.contains(value);
      case "notContains":
        return s -> !s.contains(value);
      case "dateEq":
        return s -> {
          SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
          try {
            Date nodeDate = sdf.parse(s);
            Date valueDate = sdf.parse(value);
            return nodeDate.equals(valueDate);
          } catch (ParseException e) {
            throw new RuntimeException(e);
          }
        };
      case "dateNotEq":
        return s -> {
          SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
          try {
            Date nodeDate = sdf.parse(s);
            Date valueDate = sdf.parse(value);
            return !nodeDate.equals(valueDate);
          } catch (ParseException e) {
            throw new RuntimeException(e);
          }
        };
      case "dateLt":
        return s -> {
          SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
          try {
            Date nodeDate = sdf.parse(s);
            Date valueDate = sdf.parse(value);
            return nodeDate.before(valueDate);
          } catch (ParseException e) {
            throw new RuntimeException(e);
          }
        };
      case "dateStart":
      case "dateLte":
        return s -> {
          SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
          try {
            Date nodeDate = sdf.parse(s);
            Date valueDate = sdf.parse(value);
            return nodeDate.before(valueDate) || nodeDate.equals(valueDate);
          } catch (ParseException e) {
            throw new RuntimeException(e);
          }
        };
      case "dateGt":
        return s -> {
          SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
          try {
            Date nodeDate = sdf.parse(s);
            Date valueDate = sdf.parse(value);
            return nodeDate.after(valueDate);
          } catch (ParseException e) {
            throw new RuntimeException(e);
          }
        };
      case "dateEnd":
      case "dateGte":
        return s -> {
          SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
          try {
            Date nodeDate = sdf.parse(s);
            Date valueDate = sdf.parse(value);
            return nodeDate.after(valueDate) || nodeDate.equals(valueDate);
          } catch (ParseException e) {
            throw new RuntimeException(e);
          }
        };
      default:
        throw new IllegalArgumentException(
            "Failed to determine String filter operation.\n"
                + "Please check /filterOps and use operations meant for Strings.");
    }
  }

  public Function<Boolean, Boolean> getFilterFunctionForBoolean(Boolean value, String op) {
    switch (op) {
      case "eq":
        return b -> b == value;
      case "notEq":
        return b -> b != value;
      default:
        throw new IllegalArgumentException(
            "Failed to determine Boolean filter operation.\n"
                + "Please check /filterOps and use operations meant for Booleans.");
    }
  }

  public Function<Long, Boolean> getFilterFunctionForLong(Long value, String op) {
    switch (op) {
      case "lt":
        return l -> l < value;
      case "gt":
        return l -> l > value;
      case "eq":
        return l -> l.longValue() == value.longValue();
      case "notEq":
        return l -> l.longValue() != value.longValue();
      case "lte":
        return l -> l <= value;
      case "gte":
        return l -> l >= value;
      case "minutesAgo":
        return l -> l >= System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(value);
      case "hoursAgo":
        return l -> l >= System.currentTimeMillis() - TimeUnit.HOURS.toMillis(value);
      case "daysAgo":
        return l -> l >= System.currentTimeMillis() - TimeUnit.DAYS.toMillis(value);
      case "monthsAgo":
        return l -> l >= System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30 * value);
      case "yearsAgo":
        return l -> l >= System.currentTimeMillis() - TimeUnit.DAYS.toMillis(365 * value);
      case "olderThanMinutes":
        return l -> l <= System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(value);
      case "olderThanHours":
        return l -> l <= System.currentTimeMillis() - TimeUnit.HOURS.toMillis(value);
      case "olderThanDays":
        return l -> l <= System.currentTimeMillis() - TimeUnit.DAYS.toMillis(value);
      case "olderThanMonths":
        return l -> l <= System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30 * value);
      case "olderThanYears":
        return l -> l <= System.currentTimeMillis() - TimeUnit.DAYS.toMillis(365 * value);
      default:
        throw new IllegalArgumentException(
            "Failed to determine Long filter operation.\n"
                + "Please check /filterOps and use operations meant for Longs.");
    }
  }

  private Function<INode, Long> getTransformFunction(
      Function<INode, Long> stdFunc,
      Map<String, Function<INode, Long>> transformMap,
      String transformKey) {
    if (transformMap.containsKey(transformKey)) {
      LOG.info("Function transformed for: {}", transformKey);
      return transformMap.get(transformKey);
    }
    return stdFunc;
  }

  public Map<String, Long> diskspaceConsumedHistogram(
      Collection<INode> inodes,
      String sum,
      String find,
      Map<String, Function<INode, Long>> transformMap) {
    if (find == null || find.length() == 0) {
      return diskspaceConsumedHistogramCpu(inodes, sum, transformMap);
    }
    return diskspaceConsumedHistogramCpuWithFind(inodes, find, transformMap);
  }

  private Map<String, Long> diskspaceConsumedHistogramCpu(
      Collection<INode> inodes, String sum, Map<String, Function<INode, Long>> transformMap) {
    Function<INode, Long> binFunc =
        getTransformFunction(
            getFilterFunctionToLongForINode("diskspaceConsumed"),
            transformMap,
            "diskspaceConsumed");
    Function<INode, Long> sumFunc =
        getTransformFunction(getSumFunctionForINode(sum), transformMap, sum);
    return filteringHistogram(
        inodes,
        sum,
        sumFunc,
        binFunc,
        SpaceSizeHistogram.getBinsArray(),
        SpaceSizeHistogram.getKeys());
  }

  private Map<String, Long> diskspaceConsumedHistogramCpuWithFind(
      Collection<INode> inodes, String find, Map<String, Function<INode, Long>> transformMap) {
    Function<INode, Long> binFunc =
        getTransformFunction(
            getFilterFunctionToLongForINode("diskspaceConsumed"),
            transformMap,
            "diskspaceConsumed");
    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];
    return filteringHistogramWithFind(
        inodes,
        findField,
        findOp,
        getFilterFunctionToLongForINode(findField),
        binFunc,
        SpaceSizeHistogram.getBinsArray(),
        SpaceSizeHistogram.getKeys());
  }

  public Map<String, Long> memoryConsumedHistogram(
      Collection<INode> inodes, String sum, String find) {
    if (find == null || find.length() == 0) {
      return memoryConsumedHistogramCpu(inodes, sum);
    } else {
      return memoryConsumedHistogramCpuWithFind(inodes, find);
    }
  }

  private Map<String, Long> memoryConsumedHistogramCpu(Collection<INode> inodes, String sum) {
    Function<INode, Long> memConsumedFunction =
        node -> {
          long inodeSize = 100L;
          if (node.isFile()) {
            inodeSize += node.asFile().numBlocks() * 150L;
          }
          return inodeSize;
        };
    return filteringHistogram(
        inodes,
        sum,
        getSumFunctionForINode(sum),
        memConsumedFunction,
        MemorySizeHistogram.getBinsArray(),
        MemorySizeHistogram.getKeys());
  }

  private Map<String, Long> memoryConsumedHistogramCpuWithFind(
      Collection<INode> inodes, String find) {
    Function<INode, Long> memConsumedFunction =
        node -> {
          long inodeSize = 100L;
          if (node.isFile()) {
            inodeSize += node.asFile().numBlocks() * 150L;
          }
          return inodeSize;
        };
    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];

    return filteringHistogramWithFind(
        inodes,
        findField,
        findOp,
        getFilterFunctionToLongForINode(findField),
        memConsumedFunction,
        MemorySizeHistogram.getBinsArray(),
        MemorySizeHistogram.getKeys());
  }

  private long[][] fetchDataViaCpu(
      Collection<INode> inodes,
      String sum,
      Function<INode, Long> sumFunc,
      Function<INode, Long> nodeToLong) {
    long start = System.currentTimeMillis();

    long[] data = inodes.parallelStream().mapToLong(nodeToLong::apply).toArray();
    long[] sums = inodes.parallelStream().mapToLong(sumFunc::apply).toArray();

    long end = System.currentTimeMillis();
    LOG.info("Fetching {} data took: {} ms.", sum, (end - start));

    return new long[][] {data, sums};
  }

  private Map<String, Long> strictMappingHistogram(
      Collection<INode> inodes,
      String sum,
      Function<INode, Long> sumFunc,
      Function<INode, Long> nodeToLong) {
    long[][] datas = fetchDataViaCpu(inodes, sum, sumFunc, nodeToLong);
    long[] data = datas[0];
    long[] sums = datas[1];

    long start1 = System.currentTimeMillis();
    int maxId = (int) LongStream.of(data).parallel().max().orElse(data.length);
    long[] histogram;
    try {
      if (data.length == 0) {
        histogram = data;
        LOG.info("Empty data set; skipping.");
      } else {
        histogram = new long[maxId + 2];
        IntStream.range(0, data.length)
            .parallel()
            .forEach(
                idx -> {
                  int id = (int) data[idx];
                  int chosenBin = maxId + 1;
                  if (id < chosenBin && id != -1) {
                    // Lock in the bin.
                    chosenBin = id;
                  }
                  synchronized (histogram) {
                    histogram[chosenBin] += sums[idx];
                  }
                });
        LOG.info("Histogram returned an array of size: {}", histogram.length);
      }
    } catch (Throwable e) {
      LOG.info("Encountered exception during loading:\n {}", e);
      for (StackTraceElement stacktrace : e.getStackTrace()) {
        LOG.info(stacktrace.toString());
      }
      throw e;
    }
    long end1 = System.currentTimeMillis();
    LOG.info("Histogram took: {} ms.", (end1 - start1));
    LOG.info("Histogram result has size: {}.", histogram.length);
    if (histogram.length > 100) {
      LOG.info("It is too big to console out.");
    } else {
      LOG.info("Result is: {}", java.util.Arrays.toString(histogram));
    }
    return Histograms.mapToNonEmptyIndex(histogram);
  }

  private Map<String, Long> strictMappingHistogramWithFind(
      Collection<INode> inodes,
      String findOp,
      String find,
      Function<INode, Long> findFunc,
      Function<INode, Long> nodeToLong) {
    long[][] fetchData = fetchDataViaCpu(inodes, findOp, findFunc, nodeToLong);
    long[] data = fetchData[0];
    long[] sums = fetchData[1];

    long start1 = System.currentTimeMillis();
    long[] histogram;
    try {
      if (data.length == 0) {
        histogram = data;
        LOG.info("Empty data set; skipping.");
      } else {
        histogram = new long[data.length + 1];
        IntStream.range(0, data.length)
            .parallel()
            .forEach(
                idx -> {
                  int id = (int) data[idx];
                  int chosenBin = data.length;
                  if (id < chosenBin && id != -1) {
                    // Lock in the bin.
                    chosenBin = id;
                  }
                  synchronized (histogram) {
                    long currentVal = histogram[chosenBin];
                    long compareVal = sums[idx];
                    switch (find) {
                      case "max":
                        if (currentVal < compareVal) {
                          histogram[chosenBin] = compareVal;
                        }
                        break;
                      case "min":
                        if (compareVal == 0 || currentVal > compareVal) {
                          histogram[chosenBin] = compareVal;
                        }
                        break;
                    }
                  }
                });
        LOG.info("Histogram returned an array of size: {}", histogram.length);
      }
    } catch (Throwable e) {
      LOG.info("Encountered exception during loading:\n {}", e);
      for (StackTraceElement stacktrace : e.getStackTrace()) {
        LOG.info(stacktrace.toString());
      }
      throw e;
    }
    long end1 = System.currentTimeMillis();
    LOG.info("Histogram (with find) took: {} ms.", (end1 - start1));
    LOG.info("Histogram (with find) result has size: {} ", histogram.length);
    if (histogram.length > 100) {
      LOG.info(". It is too big to console out.");
    } else {
      LOG.info(", is: {}", java.util.Arrays.toString(histogram));
    }
    return Histograms.mapToNonEmptyIndex(histogram);
  }

  Map<String, Long> binMappingHistogram(
      Collection<INode> inodes,
      String sum,
      Function<INode, Long> sumFunc,
      Function<INode, Long> nodeToLong,
      Map<String, Long> binKeyMap) {
    long[][] datas = fetchDataViaCpu(inodes, sum, sumFunc, nodeToLong);
    long[] data = datas[0];
    long[] sums = datas[1];
    int length = Math.min(data.length, sums.length);

    long start1 = System.currentTimeMillis();
    long[] histogram;
    try {
      if (data.length == 0 || sums.length == 0) {
        histogram = data;
        LOG.info("Empty data set; skipping.");
      } else {
        histogram = new long[binKeyMap.size() + 1];
        IntStream.range(0, length)
            .parallel()
            .forEach(
                idx -> {
                  int id = (int) data[idx];
                  int chosenBin = binKeyMap.size();
                  if (id < chosenBin && id != -1) {
                    // Lock in the bin.
                    chosenBin = id;
                  }
                  synchronized (histogram) {
                    histogram[chosenBin] += sums[idx];
                  }
                });
        LOG.info("Histogram returned an array of size: {}", histogram.length);
      }
    } catch (Throwable e) {
      LOG.info("Encountered exception during loading:\n {}", e);
      for (StackTraceElement stacktrace : e.getStackTrace()) {
        LOG.info(stacktrace.toString());
      }
      throw e;
    }
    long end1 = System.currentTimeMillis();
    LOG.info("Histogram took: {} ms.", (end1 - start1));
    LOG.info("Histogram result has size: {}", histogram.length);
    if (histogram.length > 100) {
      LOG.info(". It is too big to console out.");
    } else {
      LOG.info(", is: {}", java.util.Arrays.toString(histogram));
    }
    return Histograms.mapByKeys(binKeyMap, histogram);
  }

  Map<String, Long> binMappingHistogramWithFind(
      Collection<INode> inodes,
      String findFunc,
      Function<INode, Long> findToLong,
      Function<INode, Long> nodeToLong,
      Map<String, Long> binKeyMap) {
    long[][] datas = fetchDataViaCpu(inodes, findFunc, findToLong, nodeToLong);
    long[] data = datas[0];
    long[] sums = datas[1];
    int length = Math.min(data.length, sums.length);

    long start1 = System.currentTimeMillis();
    long[] histogram;
    try {
      if (data.length == 0 || sums.length == 0) {
        histogram = data;
        LOG.info("Empty data set; skipping.");
      } else if (findFunc.equals("avg")) {
        BigInteger[] bigHistogram = new BigInteger[binKeyMap.size() + 1];
        long[] counts = new long[binKeyMap.size() + 1];
        Arrays.fill(bigHistogram, BigInteger.valueOf(-1));
        IntStream.range(0, length)
            .parallel()
            .forEach(
                idx -> {
                  int id = (int) data[idx];
                  int chosenBin = binKeyMap.size();
                  if (id < chosenBin && id != -1) {
                    // Lock in the bin.
                    chosenBin = id;
                  }
                  synchronized (bigHistogram) {
                    BigInteger currentVal = bigHistogram[chosenBin];
                    long sum = sums[idx];
                    if (currentVal.equals(BigInteger.valueOf(-1))) {
                      bigHistogram[chosenBin] = BigInteger.valueOf(sum);
                    } else {
                      bigHistogram[chosenBin] =
                          bigHistogram[chosenBin].add(BigInteger.valueOf(sum));
                    }
                    counts[chosenBin]++;
                  }
                });
        if (bigHistogram[binKeyMap.size()].equals(BigInteger.valueOf(-1))) {
          bigHistogram[binKeyMap.size()] = BigInteger.ZERO;
        }
        for (int i = 0; i < bigHistogram.length; i++) {
          if (counts[i] != 0) {
            bigHistogram[i] = bigHistogram[i].divide(BigInteger.valueOf(counts[i]));
          }
        }
        histogram = Arrays.stream(bigHistogram).mapToLong(BigInteger::longValue).toArray();
        LOG.info("Histogram returned an array of size: {}", histogram.length);
      } else {
        histogram = new long[binKeyMap.size() + 1];
        Arrays.fill(histogram, -1L);
        IntStream.range(0, length)
            .parallel()
            .forEach(
                idx -> {
                  int id = (int) data[idx];
                  int chosenBin = binKeyMap.size();
                  if (id < chosenBin && id != -1) {
                    // Lock in the bin.
                    chosenBin = id;
                  }
                  synchronized (histogram) {
                    long currentVal = histogram[chosenBin];
                    long compareVal = sums[idx];
                    switch (findFunc) {
                      case "max":
                        if (currentVal < compareVal) {
                          histogram[chosenBin] = compareVal;
                        }
                        break;
                      case "min":
                        if (currentVal == -1 || currentVal > compareVal) {
                          histogram[chosenBin] = compareVal;
                        }
                        break;
                    }
                  }
                });
        if (histogram[binKeyMap.size()] == -1) {
          histogram[binKeyMap.size()] = 0;
        }
        LOG.info("Histogram returned an array of size: {}", histogram.length);
      }
    } catch (Throwable e) {
      LOG.info("Encountered exception during loading:\n {}", e);
      for (StackTraceElement stacktrace : e.getStackTrace()) {
        LOG.info(stacktrace.toString());
      }
      throw e;
    }
    long end1 = System.currentTimeMillis();
    LOG.info("Histogram (with find) took: {} ms.", (end1 - start1));
    LOG.info("Histogram (with find) result has size: {}", histogram.length);
    if (histogram.length > 100) {
      LOG.info(". It is too big to console out.");
    } else {
      LOG.info(", is: {}", java.util.Arrays.toString(histogram));
    }
    return Histograms.mapByKeys(binKeyMap, histogram);
  }

  private Map<String, Long> filteringHistogram(
      Collection<INode> inodes,
      String sum,
      Function<INode, Long> sumFunc,
      Function<INode, Long> nodeToLong,
      final Long[] binsArray,
      List<String> keys) {
    long[][] datas = fetchDataViaCpu(inodes, sum, sumFunc, nodeToLong);
    long[] data = datas[0];
    long[] sums = datas[1];

    long start1 = System.currentTimeMillis();
    long[] histogram;
    try {
      if (data.length == 0) {
        histogram = data;
        LOG.info("Empty data set; skipping.");
      } else {
        histogram = new long[binsArray.length + 1];
        IntStream.range(0, data.length)
            .parallel()
            .forEach(
                idx -> {
                  long datum = data[idx];
                  int chosenBin = binsArray.length;
                  for (int i = 0; i < binsArray.length; i++) {
                    if (datum <= binsArray[i] && chosenBin == binsArray.length) {
                      chosenBin = i;
                    }
                  }
                  synchronized (histogram) {
                    histogram[chosenBin] += sums[idx];
                  }
                });
        LOG.info("Histogram returned an array of size: {}", histogram.length);
      }
    } catch (Throwable e) {
      LOG.info("Encountered exception during loading:\n {}", e);
      for (StackTraceElement stacktrace : e.getStackTrace()) {
        LOG.info(stacktrace.toString());
      }
      throw e;
    }
    long end1 = System.currentTimeMillis();
    LOG.info("Histogram took: {} ms.", (end1 - start1));
    LOG.info("Histogram result has size: {}", histogram.length);
    if (histogram.length > 100) {
      LOG.info(". It is too big to console out.");
    } else {
      LOG.info(", is: {}", java.util.Arrays.toString(histogram));
    }
    return Histograms.sortByKeys(keys, histogram);
  }

  private Map<String, Long> filteringHistogramWithFind(
      Collection<INode> inodes,
      String findOp,
      String find,
      Function<INode, Long> findFunc,
      Function<INode, Long> nodeToLong,
      final Long[] binsArray,
      List<String> keys) {
    long[][] fetchData = fetchDataViaCpu(inodes, findOp, findFunc, nodeToLong);
    long[] data = fetchData[0];
    long[] sums = fetchData[1];

    long start1 = System.currentTimeMillis();
    long[] histogram;
    try {
      if (data.length == 0) {
        histogram = data;
        LOG.info("Empty data set; skipping.");
      } else if (find.equals("avg")) {
        BigInteger[] bigHistogram = new BigInteger[binsArray.length + 1];
        long[] counts = new long[binsArray.length + 1];
        IntStream.range(0, data.length)
            .parallel()
            .forEach(
                idx -> {
                  long datum = data[idx];
                  int chosenBin = binsArray.length;
                  for (int i = 0; i < binsArray.length; i++) {
                    if (datum <= binsArray[i] && chosenBin == binsArray.length) {
                      chosenBin = i;
                    }
                  }
                  synchronized (bigHistogram) {
                    BigInteger currentVal = bigHistogram[chosenBin];
                    long sum = sums[idx];
                    if (currentVal == null) {
                      bigHistogram[chosenBin] = BigInteger.valueOf(sum);
                    } else {
                      bigHistogram[chosenBin] =
                          bigHistogram[chosenBin].add(BigInteger.valueOf(sum));
                    }
                    counts[chosenBin]++;
                  }
                });
        for (int i = 0; i < bigHistogram.length; i++) {
          if (counts[i] != 0) {
            bigHistogram[i] = bigHistogram[i].divide(BigInteger.valueOf(counts[i]));
          }
        }
        histogram =
            Arrays.stream(bigHistogram).mapToLong(x -> x == null ? 0L : x.longValue()).toArray();
        LOG.info("Histogram returned an array of size: {}", histogram.length);
      } else {
        histogram = new long[binsArray.length + 1];
        IntStream.range(0, data.length)
            .parallel()
            .forEach(
                idx -> {
                  long datum = data[idx];
                  int chosenBin = binsArray.length;
                  for (int i = 0; i < binsArray.length; i++) {
                    if (datum <= binsArray[i] && chosenBin == binsArray.length) {
                      chosenBin = i;
                    }
                  }
                  synchronized (histogram) {
                    long currentVal = histogram[chosenBin];
                    long compareVal = sums[idx];
                    switch (find) {
                      case "max":
                        if (currentVal < compareVal) {
                          histogram[chosenBin] = compareVal;
                        }
                        break;
                      case "min":
                        if (compareVal == 0 || currentVal > compareVal) {
                          histogram[chosenBin] = compareVal;
                        }
                        break;
                    }
                  }
                });
        LOG.info("Histogram returned an array of size: {}", histogram.length);
      }
    } catch (Throwable e) {
      LOG.info("Encountered exception during loading:\n {}", e);
      for (StackTraceElement stacktrace : e.getStackTrace()) {
        LOG.info(stacktrace.toString());
      }
      throw e;
    }
    long end1 = System.currentTimeMillis();
    LOG.info("Histogram (with find) took: {} ms.", (end1 - start1));
    LOG.info("Histogram (with find) result has size: {}", histogram.length);
    if (histogram.length > 100) {
      LOG.info(". It is too big to console out.");
    } else {
      LOG.info(", is: {}", java.util.Arrays.toString(histogram));
    }
    return Histograms.sortByKeys(keys, histogram);
  }

  public Map<String, Long> fileSizeHistogram(Collection<INode> inodes, String sum, String find) {
    if (find == null || find.length() == 0) {
      return fileSizeHistogramCpu(inodes, sum);
    }
    return fileSizeHistogramCpuWithFind(inodes, find);
  }

  public Map<String, Long> fileSizeHistogramCpu(Collection<INode> inodes, String sum) {
    return filteringHistogram(
        inodes,
        sum,
        getSumFunctionForINode(sum),
        node -> node.asFile().computeFileSize(),
        SpaceSizeHistogram.getBinsArray(),
        SpaceSizeHistogram.getKeys());
  }

  private Map<String, Long> fileSizeHistogramCpuWithFind(Collection<INode> inodes, String find) {
    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];
    return filteringHistogramWithFind(
        inodes,
        findField,
        findOp,
        getFilterFunctionToLongForINode(findField),
        node -> node.asFile().computeFileSize(),
        SpaceSizeHistogram.getBinsArray(),
        SpaceSizeHistogram.getKeys());
  }

  public Map<String, Long> fileReplicaHistogram(
      Collection<INode> inodes,
      String sum,
      String find,
      Map<String, Function<INode, Long>> transformMap) {
    if (find == null || find.length() == 0) {
      return fileReplicaHistogramCpu(inodes, sum, transformMap);
    }
    return fileReplicaHistogramCpuWithFind(inodes, find);
  }

  public Map<String, Long> fileReplicaHistogramCpu(
      Collection<INode> inodes, String sum, Map<String, Function<INode, Long>> transformMap) {
    Function<INode, Long> binFunc =
        getTransformFunction(
            getFilterFunctionToLongForINode("fileReplica"), transformMap, "fileReplica");
    Function<INode, Long> sumFunc =
        getTransformFunction(getSumFunctionForINode(sum), transformMap, sum);
    return strictMappingHistogram(inodes, sum, sumFunc, binFunc);
  }

  private Map<String, Long> fileReplicaHistogramCpuWithFind(Collection<INode> inodes, String find) {
    Function<INode, Long> binFunc = getFilterFunctionToLongForINode("fileReplica");
    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];
    Function<INode, Long> findFunc = getFilterFunctionToLongForINode(findField);
    return strictMappingHistogramWithFind(inodes, findField, findOp, findFunc, binFunc);
  }

  public Map<String, Long> storageTypeHistogram(Collection<INode> inodes, String sum, String find) {
    if (find == null || find.length() == 0) {
      return storageTypeHistogramCpu(inodes, sum);
    }
    return storageTypeHistogramCpuWithFind(inodes, find);
  }

  public Map<String, Long> storageTypeHistogramCpu(Collection<INode> inodes, String sum) {
    return versionLoader.storageTypeHistogramCpu(inodes, sum, this);
  }

  private Map<String, Long> storageTypeHistogramCpuWithFind(Collection<INode> inodes, String find) {
    return versionLoader.storageTypeHistogramCpuWithFind(inodes, find, this);
  }

  public Map<String, Long> accessTimeHistogram(
      Collection<INode> inodes, String sum, String find, String timeRange) {
    if (find == null || find.length() == 0) {
      return accessTimeHistogramCpu(inodes, sum, timeRange);
    }
    return accessTimeHistogramCpuWithFind(inodes, find, timeRange);
  }

  public Map<String, Long> accessTimeHistogramCpu(
      Collection<INode> inodes, String sum, String timeRange) {
    return filteringHistogram(
        inodes,
        sum,
        getSumFunctionForINode(sum),
        node -> System.currentTimeMillis() - node.getAccessTime(),
        TimeHistogram.getBinsArray(timeRange),
        TimeHistogram.getKeys(timeRange));
  }

  private Map<String, Long> accessTimeHistogramCpuWithFind(
      Collection<INode> inodes, String find, String timeRange) {
    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];
    return filteringHistogramWithFind(
        inodes,
        findField,
        findOp,
        getFilterFunctionToLongForINode(findField),
        node -> System.currentTimeMillis() - node.getAccessTime(),
        TimeHistogram.getBinsArray(timeRange),
        TimeHistogram.getKeys(timeRange));
  }

  public Map<String, Long> modTimeHistogram(
      Collection<INode> inodes, String sum, String find, String timeRange) {
    if (find == null || find.length() == 0) {
      return modTimeHistogramCpu(inodes, sum, timeRange);
    }
    return modTimeHistogramCpuWithFind(inodes, find, timeRange);
  }

  public Map<String, Long> modTimeHistogramCpu(
      Collection<INode> inodes, String sum, String timeRange) {
    return filteringHistogram(
        inodes,
        sum,
        getSumFunctionForINode(sum),
        node -> System.currentTimeMillis() - node.getModificationTime(),
        TimeHistogram.getBinsArray(timeRange),
        TimeHistogram.getKeys(timeRange));
  }

  private Map<String, Long> modTimeHistogramCpuWithFind(
      Collection<INode> inodes, String find, String timeRange) {
    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];
    return filteringHistogramWithFind(
        inodes,
        findField,
        findOp,
        getFilterFunctionToLongForINode(findField),
        node -> System.currentTimeMillis() - node.getModificationTime(),
        TimeHistogram.getBinsArray(timeRange),
        TimeHistogram.getKeys(timeRange));
  }

  public void dumpINodePaths(Collection<INode> inodes, Integer limit, HttpServletResponse resp)
      throws IOException {
    LOG.info("Dumping a list of {} INodes to a client.", inodes.size());
    long start = System.currentTimeMillis();
    PrintWriter writer = resp.getWriter();
    try {
      Collection<INode> subCollection;
      if (limit != null && limit < inodes.size()) {
        subCollection = inodes.stream().limit(limit).collect(Collectors.toList());
      } else {
        subCollection = inodes;
      }
      subCollection
          .stream()
          .sorted(Comparator.comparing(INode::getFullPathName))
          .forEach(
              node -> {
                writer.write(node.getFullPathName() + '\n');
                writer.flush();
              });
    } finally {
      IOUtils.closeStream(writer);
      LOG.info("Closed response.");
    }
    long end = System.currentTimeMillis();
    LOG.info("Sending the entire response took {} ms.", (end - start));
  }

  public Map<String, Long> byUserHistogram(Collection<INode> inodes, String sum, String find) {
    if (find == null || find.length() == 0) {
      return byUserHistogramCpu(inodes, sum);
    }
    return byUserHistogramCpuWithFind(inodes, find);
  }

  public Map<String, Long> byUserHistogramCpu(Collection<INode> inodes, String sum) {
    List<String> distinctUsers =
        StreamSupport.stream(inodes.spliterator(), true)
            .map(INode::getUserName)
            .distinct()
            .collect(Collectors.toList());
    Map<String, Long> userToIdMap =
        distinctUsers
            .parallelStream()
            .mapToInt(distinctUsers::indexOf)
            .boxed()
            .collect(Collectors.toMap(distinctUsers::get, value -> (long) value));

    return binMappingHistogram(
        inodes,
        sum,
        getSumFunctionForINode(sum),
        node -> userToIdMap.get(node.getUserName()),
        userToIdMap);
  }

  private Map<String, Long> byUserHistogramCpuWithFind(Collection<INode> inodes, String find) {
    List<String> distinctUsers =
        StreamSupport.stream(inodes.spliterator(), true)
            .map(INode::getUserName)
            .distinct()
            .collect(Collectors.toList());
    Map<String, Long> userToIdMap =
        distinctUsers
            .parallelStream()
            .mapToInt(distinctUsers::indexOf)
            .boxed()
            .collect(Collectors.toMap(distinctUsers::get, value -> (long) value));

    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];

    return binMappingHistogramWithFind(
        inodes,
        findOp,
        getFilterFunctionToLongForINode(findField),
        node -> userToIdMap.get(node.getUserName()),
        userToIdMap);
  }

  public Map<String, Long> byGroupHistogram(Collection<INode> inodes, String sum, String find) {
    if (find == null || find.length() == 0) {
      return byGroupHistogramCpu(inodes, sum);
    }
    return byGroupHistogramCpuWithFind(inodes, find);
  }

  public Map<String, Long> byGroupHistogramCpu(Collection<INode> inodes, String sum) {
    List<String> distinctGroups =
        StreamSupport.stream(inodes.spliterator(), true)
            .map(INode::getGroupName)
            .distinct()
            .collect(Collectors.toList());
    Map<String, Long> groupToIdMap =
        distinctGroups
            .parallelStream()
            .mapToInt(distinctGroups::indexOf)
            .boxed()
            .collect(Collectors.toMap(distinctGroups::get, value -> (long) value));

    return binMappingHistogram(
        inodes,
        sum,
        getSumFunctionForINode(sum),
        node -> groupToIdMap.get(node.getGroupName()),
        groupToIdMap);
  }

  private Map<String, Long> byGroupHistogramCpuWithFind(Collection<INode> inodes, String find) {
    List<String> distinctGroups =
        StreamSupport.stream(inodes.spliterator(), true)
            .map(INode::getGroupName)
            .distinct()
            .collect(Collectors.toList());
    Map<String, Long> groupToIdMap =
        distinctGroups
            .parallelStream()
            .mapToInt(distinctGroups::indexOf)
            .boxed()
            .collect(Collectors.toMap(distinctGroups::get, value -> (long) value));

    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];

    return binMappingHistogramWithFind(
        inodes,
        findOp,
        getFilterFunctionToLongForINode(findField),
        node -> groupToIdMap.get(node.getGroupName()),
        groupToIdMap);
  }

  public Map<String, Long> parentDirHistogram(
      Collection<INode> inodes, Integer parentDirDepth, String sum, String find) {
    if (find == null || find.length() == 0) {
      return parentDirHistogramCpu(inodes, parentDirDepth, sum);
    }
    return parentDirHistogramCpuWithFind(inodes, parentDirDepth, find);
  }

  public Map<String, Long> parentDirHistogramCpu(
      Collection<INode> inodes, Integer parentDirDepth, String sum) {
    int dirDepth =
        (parentDirDepth == null || parentDirDepth <= 0) ? Integer.MAX_VALUE : parentDirDepth;
    List<String> distinctDirectories =
        inodes
            .parallelStream()
            .map(
                node -> {
                  try {
                    INodeDirectory parent = node.getParent();
                    int topParentDepth = new Path(parent.getFullPathName()).depth();
                    if (topParentDepth < dirDepth) {
                      return "NO_MAPPING";
                    }
                    for (int parentTravs = topParentDepth; parentTravs > dirDepth; parentTravs--) {
                      parent = parent.getParent();
                    }
                    return parent.getFullPathName();
                  } catch (Exception e) {
                    return "NO_MAPPING";
                  }
                })
            .distinct()
            .collect(Collectors.toList());

    final AtomicLong id = new AtomicLong(0L);
    final Map<String, Long> dirToIdMap =
        distinctDirectories
            .parallelStream()
            .collect(Collectors.toMap(dir -> dir, dir -> id.getAndIncrement()));
    if (!dirToIdMap.containsKey("NO_MAPPING")) {
      dirToIdMap.put("NO_MAPPING", id.getAndIncrement());
    }
    final long noMappingId = dirToIdMap.get("NO_MAPPING");

    Map<String, Long> result =
        binMappingHistogram(
            inodes,
            sum,
            getSumFunctionForINode(sum),
            node -> {
              try {
                INodeDirectory parent = node.getParent();
                int topParentDepth = new Path(parent.getFullPathName()).depth();
                if (topParentDepth < dirDepth) {
                  return noMappingId;
                }
                for (int parentTravs = topParentDepth; parentTravs > dirDepth; parentTravs--) {
                  parent = parent.getParent();
                }
                Long index = dirToIdMap.get(parent.getFullPathName());
                return index != null ? index : noMappingId;
              } catch (Throwable e) {
                return noMappingId;
              }
            },
            dirToIdMap);
    result.remove("NO_MAPPING");
    return result;
  }

  private Map<String, Long> parentDirHistogramCpuWithFind(
      Collection<INode> inodes, Integer parentDirDepth, String find) {
    int dirDepth = (parentDirDepth != null) ? parentDirDepth : 0;
    List<String> distinctDirectories =
        inodes
            .parallelStream()
            .map(
                node -> {
                  try {
                    INodeDirectory parent = node.getParent();
                    int topParentDepth = new Path(parent.getFullPathName()).depth();
                    if (topParentDepth < dirDepth) {
                      return "NO_MAPPING";
                    }
                    for (int parentTravs = topParentDepth; parentTravs > dirDepth; parentTravs--) {
                      parent = parent.getParent();
                    }
                    return parent.getFullPathName();
                  } catch (Exception e) {
                    return "NO_MAPPING";
                  }
                })
            .distinct()
            .collect(Collectors.toList());

    final AtomicLong id = new AtomicLong(0L);
    Map<String, Long> dirToIdMap =
        distinctDirectories
            .parallelStream()
            .collect(Collectors.toMap(dir -> dir, dir -> id.getAndIncrement()));
    if (!dirToIdMap.containsKey("NO_MAPPING")) {
      dirToIdMap.put("NO_MAPPING", id.getAndIncrement());
    }
    final long noMappingId = dirToIdMap.get("NO_MAPPING");
    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];

    Map<String, Long> result =
        binMappingHistogramWithFind(
            inodes,
            findOp,
            getFilterFunctionToLongForINode(findField),
            node -> {
              try {
                INodeDirectory parent = node.getParent();
                int topParentDepth = new Path(parent.getFullPathName()).depth();
                if (topParentDepth < dirDepth) {
                  return noMappingId;
                }
                for (int parentTravs = topParentDepth; parentTravs > dirDepth; parentTravs--) {
                  parent = parent.getParent();
                }
                Long index = dirToIdMap.get(parent.getFullPathName());
                return index != null ? index : noMappingId;
              } catch (Throwable e) {
                return noMappingId;
              }
            },
            dirToIdMap);
    result.remove("NO_MAPPING");
    return result;
  }

  public Map<String, Long> fileTypeHistogram(Collection<INode> inodes, String sum, String find) {
    if (find == null || find.length() == 0) {
      return fileTypeHistogramCpu(inodes, sum);
    }
    return fileTypeHistogramCpu(inodes, sum);
  }

  public Map<String, Long> fileTypeHistogramCpu(Collection<INode> inodes, String sum) {
    List<String> fileTypes = FileTypeHistogram.keys;

    Map<String, Long> typeToIdMap =
        fileTypes
            .parallelStream()
            .mapToInt(fileTypes::indexOf)
            .boxed()
            .collect(Collectors.toMap(fileTypes::get, value -> (long) value));

    Map<String, Long> histogram =
        binMappingHistogram(
            inodes,
            sum,
            getSumFunctionForINode(sum),
            node -> typeToIdMap.get(FileTypeHistogram.determineType(node.getLocalName())),
            typeToIdMap);

    return removeKeysOnConditional(histogram, "gt:0");
  }

  public Map<String, Long> dirQuotaHistogram(Collection<INode> inodes, String sum) {
    return dirQuotaHistogramCpu(inodes, sum);
  }

  public Map<String, Long> dirQuotaHistogramCpu(Collection<INode> inodes, String sum) {
    List<String> distinctDirectories =
        inodes.parallelStream().map(INode::getFullPathName).distinct().collect(Collectors.toList());

    final AtomicLong id = new AtomicLong(0L);
    Map<String, Long> dirToIdMap =
        distinctDirectories
            .parallelStream()
            .collect(Collectors.toMap(dir -> dir, dir -> id.getAndIncrement()));

    Map<String, Long> histogram =
        binMappingHistogram(
            inodes,
            sum,
            getSumFunctionForINode(sum),
            node -> dirToIdMap.get(node.getFullPathName()),
            dirToIdMap);

    return removeKeysOnConditional(histogram, "gte:0");
  }

  /**
   * Creates histogram with only entries that satisfy the conditional String. Conditional String ex:
   * 'gte:1000' should create a histogram where all entries have values greater than or equal to
   * 1000L. NOTE: Modifies the parameter histogram.
   *
   * @param histogram data points of histogram
   * @param histogramConditionsStr conditional string to filter out the given histogram
   * @return filtered histogram as per the given conditional string
   */
  public Map<String, Long> removeKeysOnConditional(
      Map<String, Long> histogram, String histogramConditionsStr) {
    long s1 = System.currentTimeMillis();
    int originalHistSize = histogram.size();

    List<Function<Long, Boolean>> comparisons = createComparisons(histogramConditionsStr);
    Set<String> keys = new HashSet<>();
    for (Map.Entry<String, Long> entry : histogram.entrySet()) {
      boolean columnCheck = check(comparisons, entry.getValue());
      if (!columnCheck) {
        keys.add(entry.getKey());
      }
    }
    keys.forEach(histogram::remove);

    long e1 = System.currentTimeMillis();
    LOG.info(
        "Removing {} keys from histogram of size {} using conditional String:'{}', took: {} ms.",
        keys.size(),
        originalHistSize,
        histogramConditionsStr,
        (e1 - s1));

    return histogram;
  }

  /**
   * Result is a histogram with only entries that satisfy the conditional String. Conditional String
   * ex: 'gte:1000' should create a histogram where all entries have values greater than or equal to
   * 1000L.
   *
   * <p>NOTE: Modifies the parameter histogram.
   *
   * @param histogram data points of histogram
   * @param histogramConditionsStr conditional string to filter out the given histogram
   * @return filtered histogram as per the given conditional string
   */
  public Map<String, List<Long>> removeKeysOnConditional2(
      Map<String, List<Long>> histogram, String histogramConditionsStr) {
    long s1 = System.currentTimeMillis();
    int originalHistSize = histogram.size();

    List<Function<List<Long>, Boolean>> comparisons =
        createIndexedComparisons(histogramConditionsStr);
    Set<String> keys = new HashSet<>();
    for (Map.Entry<String, List<Long>> entry : histogram.entrySet()) {
      boolean columnCheck = check(comparisons, entry.getValue());
      if (!columnCheck) {
        keys.add(entry.getKey());
      }
    }
    keys.forEach(histogram::remove);

    long e1 = System.currentTimeMillis();
    LOG.info(
        "Removing {} keys from histogram2 of size {} using conditional String:'{}', took: {} ms.",
        keys.size(),
        originalHistSize,
        histogramConditionsStr,
        (e1 - s1));

    return histogram;
  }

  public List<Function<Long, Boolean>> createComparisons(String conditionsStr) {
    String[] conditionsArray = conditionsStr.split(";");
    String[][] conditionTuplets = new String[conditionsArray.length][2];

    List<Function<Long, Boolean>> comparisons = new ArrayList<>(conditionTuplets.length);
    for (int i = 0; i < conditionTuplets.length; i++) {
      String tuplet = conditionsArray[i];
      conditionTuplets[i] = tuplet.split(":");
    }

    // Create comparisons.
    for (String[] condition : conditionTuplets) {
      Function<Long, Boolean> longFunction =
          getFilterFunctionForLong(Long.parseLong(condition[1]), condition[0]);
      comparisons.add(longFunction);
    }

    return comparisons;
  }

  public boolean check(List<Function<Long, Boolean>> comparisons, long value) {
    boolean check = true;
    for (Function<Long, Boolean> comparison : comparisons) {
      boolean compareResult = comparison.apply(value);
      if (!compareResult) {
        check = false;
        break;
      }
    }
    return check;
  }

  public boolean check(List<Function<List<Long>, Boolean>> comparisons, List<Long> value) {
    boolean check = true;
    for (Function<List<Long>, Boolean> comparison : comparisons) {
      boolean compareResult = comparison.apply(value);
      if (!compareResult) {
        check = false;
        break;
      }
    }
    return check;
  }

  public List<Function<List<Long>, Boolean>> createIndexedComparisons(String conditionsStr) {
    String[] conditionsArray = conditionsStr.split(",");
    String[][] conditionTuplets = new String[conditionsArray.length][3];

    List<Function<List<Long>, Boolean>> comparisons = new ArrayList<>(conditionTuplets.length);
    for (int i = 0; i < conditionTuplets.length; i++) {
      String triplet = conditionsArray[i];
      conditionTuplets[i] = triplet.split(":");
    }

    // Create comparisons.
    for (String[] condition : conditionTuplets) {
      Function<Long, Boolean> longFunction =
          getFilterFunctionForLong(Long.parseLong(condition[2]), condition[1]);
      Function<List<Long>, Boolean> indexedLongFunction =
          (list) -> longFunction.apply(list.get(Integer.parseInt(condition[0])));
      comparisons.add(indexedLongFunction);
    }

    return comparisons;
  }
}
