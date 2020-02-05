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
import java.lang.reflect.Field;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.analytics.ApplicationConfiguration;
import org.apache.hadoop.hdfs.server.namenode.analytics.Helper;
import org.apache.hadoop.hdfs.server.namenode.analytics.HistogramInvoker;
import org.apache.hadoop.hdfs.server.namenode.queries.FileTypeHistogram;
import org.apache.hadoop.hdfs.server.namenode.queries.MemorySizeHistogram;
import org.apache.hadoop.hdfs.server.namenode.queries.SpaceSizeHistogram;
import org.apache.hadoop.hdfs.server.namenode.queries.StorageTypeHistogram;
import org.apache.hadoop.hdfs.server.namenode.queries.TimeHistogram;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.CollectionsView;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.GSetSeperatorWrapper;
import org.apache.hadoop.util.ReflectionUtils;

public abstract class AbstractQueryEngine implements QueryEngine {

  protected Collection<INode> all;
  protected Map<INode, INodeWithAdditionalFields> files;
  protected Map<INode, INodeWithAdditionalFields> dirs;

  private VersionInterface versionLoader;

  @Override // QueryEngine
  public void setVersionContext(VersionInterface versionLoader) {
    this.versionLoader = versionLoader;
  }

  @SuppressWarnings("unchecked") /* We do unchecked casting to extract GSets */
  @Override // QueryEngine
  public void handleGSet(
      GSet<INode, INodeWithAdditionalFields> preloaded,
      ApplicationConfiguration nnaConf,
      FSNamesystem namesystem)
      throws Exception {
    if (preloaded != null) {
      filterINodes(preloaded, nnaConf);
      return;
    }

    namesystem.writeLock();
    try {
      FSDirectory fsDirectory = namesystem.getFSDirectory();
      INodeMap inodeMap = fsDirectory.getINodeMap();
      Field mapField = inodeMap.getClass().getDeclaredField("map");
      mapField.setAccessible(true);
      GSet<INode, INodeWithAdditionalFields> gset =
          (GSet<INode, INodeWithAdditionalFields>) mapField.get(inodeMap);

      filterINodes(gset, nnaConf);

      GSet<INode, INodeWithAdditionalFields> newGSet = new GSetSeperatorWrapper(files, dirs);
      mapField.set(inodeMap, newGSet);
    } finally {
      namesystem.writeUnlock();
    }
  }

  @SuppressWarnings("unchecked") /* We do unchecked casting to extract GSets */
  private void filterINodes(
      GSet<INode, INodeWithAdditionalFields> gset, ApplicationConfiguration nnaConf)
      throws Exception {
    String inodeCollectionClass = nnaConf.getINodeCollectionImplementation();
    LOG.info("Filtering inodes with implementation: {}", inodeCollectionClass);
    Class<INodeFilterer> filtererClass = (Class<INodeFilterer>) Class.forName(inodeCollectionClass);
    INodeFilterer filterer = ReflectionUtils.newInstance(filtererClass, null);
    final long start = System.currentTimeMillis();
    files = filterer.filterFiles(gset);
    dirs = filterer.filterDirs(gset);
    all = CollectionsView.combine(files.keySet(), dirs.keySet());
    final long end = System.currentTimeMillis();
    LOG.info("Performing AbstractQE filtering of files and dirs took: {} ms.", (end - start));
  }

  @Override // QueryEngine
  public Collection<INode> getINodeSet(String set) {
    long start = System.currentTimeMillis();
    Collection<INode> inodes;
    switch (set) {
      case "all":
        inodes = all;
        break;
      case "files":
        inodes = files.keySet();
        break;
      case "dirs":
        inodes = dirs.keySet();
        break;
      default:
        throw new IllegalArgumentException(
            "You did not specify a set to use. Please check /sets for available sets.");
    }
    long end = System.currentTimeMillis();
    LOG.debug(
        "Fetching set of: {} had result size: {} and took: {} ms.",
        set,
        inodes.size(),
        (end - start));
    return inodes;
  }

  /**
   * Get a Function to convert INode to a Long value.
   *
   * @param filter the filter to look for
   * @return the function representing the filter transform
   */
  @Override // QueryEngine
  public Function<INode, Long> getFilterFunctionToLongForINode(String filter) {
    switch (filter) {
      case "id":
        return INode::getId;
      case "fileSize":
        return node -> node.asFile().computeFileSize();
      case "diskspaceConsumed":
        return node -> node.asFile().computeFileSize() * node.asFile().getFileReplication();
      case "fileReplica":
        return node -> ((long) node.asFile().getFileReplication());
      case "blockSize":
        return node -> ((long) node.asFile().getPreferredBlockSize());
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
          long inodeSize = 150L;
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

  /**
   * Get a Function to convert INode to a String value for grouping.
   *
   * @param grouping the grouping function to look for
   * @return the function representing the filter transform
   */
  @Override // QueryEngine
  public Function<INode, String> getGroupingFunctionToStringForINode(
      String grouping, Integer parentDirDepth, String timeRange) {
    switch (grouping) {
      case "name":
        return INode::getLocalName;
      case "path":
      case "dirQuota":
        return INode::getFullPathName;
      case "user":
        return INode::getUserName;
      case "group":
        return INode::getGroupName;
      case "accessTime":
        return n -> {
          Function<INode, Long> timeDiffFunc =
              node -> System.currentTimeMillis() - node.getAccessTime();
          Function<Long, String> bucketingFunc = TimeHistogram.computeBucketFunction(timeRange);
          return getCustomLongToStringGroupingFunction(timeDiffFunc, bucketingFunc).apply(n);
        };
      case "modTime":
        return n -> {
          Function<INode, Long> timeDiffFunc =
              node -> System.currentTimeMillis() - node.getModificationTime();
          Function<Long, String> bucketingFunc = TimeHistogram.computeBucketFunction(timeRange);
          return getCustomLongToStringGroupingFunction(timeDiffFunc, bucketingFunc).apply(n);
        };
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
      case "fileSize":
        return getCustomLongToStringGroupingFunction(
            getFilterFunctionToLongForINode("fileSize"),
            SpaceSizeHistogram.determineBucketFunction);
      case "diskspaceConsumed":
        return getCustomLongToStringGroupingFunction(
            getFilterFunctionToLongForINode("diskspaceConsumed"),
            SpaceSizeHistogram.determineBucketFunction);
      case "memoryConsumed":
        return getCustomLongToStringGroupingFunction(
            getFilterFunctionToLongForINode("memoryConsumed"),
            MemorySizeHistogram.determineBucketFunction);
      case "fileType":
        return n -> FileTypeHistogram.determineType(n.getLocalName());
      case "parentDir":
        return Helper.getDirectoryAtDepthFunction((parentDirDepth != null) ? parentDirDepth : 0);
      case "storageType":
        return n -> {
          List<Long> storageIds = StorageTypeHistogram.bins;
          List<String> storageKeys = StorageTypeHistogram.keys;
          int index = storageIds.indexOf((long) n.getStoragePolicyID());
          if (index >= 0) {
            return storageKeys.get(index);
          }
          return "NO_MAPPING";
        };
      case "fileReplica":
        return n -> String.valueOf(n.asFile().getFileReplication());
      default:
        return null;
    }
  }

  /**
   * Get a Function to convert INode to a String value.
   *
   * @param filter the filter to look for
   * @return the function representing the filter transform
   */
  @Override // QueryEngine
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
      case "fileType":
        return n -> FileTypeHistogram.determineType(n.getLocalName());
      default:
        return null;
    }
  }

  /**
   * Get a Function to convert INode to a Boolean value.
   *
   * @param filter the filter to look for
   * @return the function representing the filter transform
   */
  @Override // QueryEngine
  public Function<INode, Boolean> getFilterFunctionToBooleanForINode(String filter) {
    switch (filter) {
      case "isUnderConstruction":
        return node -> node.asFile().isUnderConstruction();
      case "isWithSnapshot":
        return node -> {
          if (node.isFile()) {
            return node.asFile().isWithSnapshot();
          }
          if (node.isDirectory()) {
            return node.asDirectory().isWithSnapshot();
          }
          return false;
        };
      case "hasAcl":
        return node -> (node.getAclFeature() != null);
      case "isUnderNsQuota":
        return node -> {
          if (node.isRoot()) {
            return false;
          }
          for (INodeDirectory p = node.getParent(); p != null; p = node.getParent()) {
            node = p;
            if (!node.isRoot() && versionLoader.getNsQuota(node) >= 0) {
              return true;
            }
          }
          return false;
        };
      case "isUnderDsQuota":
        return node -> {
          if (node.isRoot()) {
            return false;
          }
          for (INodeDirectory p = node.getParent(); p != null; p = node.getParent()) {
            node = p;
            if (!node.isRoot() && versionLoader.getDsQuota(node) >= 0) {
              return true;
            }
          }
          return false;
        };
      default:
        return versionLoader.getFilterFunctionToBooleanForINode(filter);
    }
  }

  /**
   * Get a Function that converts an INode to a single Long for summation. Used mostly by Histogram
   * functions.
   *
   * @param sum the sum to look for
   * @return the function representing the sum transform
   */
  @Override // QueryEngine
  public Function<INode, Long> getSumFunctionForINode(
      String sum, Map<String, Function<INode, Long>> transformMap) {
    Function<INode, Long> toLongFunction;
    switch (sum) {
      case "count":
        toLongFunction = node -> 1L;
        break;
      case "dirNumChildren":
        toLongFunction =
            node -> versionLoader.getFilterFunctionToLongForINode("dirNumChildren").apply(node);
        break;
      case "fileSize":
        toLongFunction = node -> node.asFile().computeFileSize();
        break;
      case "diskspaceConsumed":
        toLongFunction =
            node -> node.asFile().computeFileSize() * node.asFile().getFileReplication();
        break;
      case "blockSize":
        toLongFunction = node -> node.asFile().getPreferredBlockSize();
        break;
      case "numBlocks":
        toLongFunction = node -> (long) node.asFile().numBlocks();
        break;
      case "numReplicas":
        toLongFunction =
            node -> (long) node.asFile().numBlocks() * node.asFile().getFileReplication();
        break;
      case "memoryConsumed":
        toLongFunction =
            node -> {
              long inodeSize = 150L;
              if (node.isFile()) {
                inodeSize += node.asFile().numBlocks() * 150L;
              }
              return inodeSize;
            };
        break;
      case "nsQuotaRatioUsed":
        toLongFunction =
            node ->
                (long)
                    ((double) versionLoader.getNsQuotaUsed(node)
                        / (double) versionLoader.getNsQuota(node)
                        * 100);
        break;
      case "dsQuotaRatioUsed":
        toLongFunction =
            node ->
                (long)
                    ((double) versionLoader.getDsQuotaUsed(node)
                        / (double) versionLoader.getDsQuota(node)
                        * 100);
        break;
      case "nsQuotaUsed":
        toLongFunction = versionLoader::getNsQuotaUsed;
        break;
      case "dsQuotaUsed":
        toLongFunction = versionLoader::getDsQuotaUsed;
        break;
      case "nsQuota":
        toLongFunction = versionLoader::getNsQuota;
        break;
      case "dsQuota":
        toLongFunction = versionLoader::getDsQuota;
        break;
      default:
        throw new IllegalArgumentException(
            "Could not determine sum type: " + sum + ".\nPlease check /sums for available sums.");
    }
    return getTransformFunction(toLongFunction, transformMap, sum);
  }

  /**
   * Perform the find operation on a /filter endpoint call.
   *
   * @param inodes set of inodes to work on
   * @param find the find operation to perform
   * @return the result of the find operation
   */
  @Override // QueryEngine
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

  /**
   * Performs a summation against a collection of INodes.
   *
   * @param inodes the inodes to sum on
   * @param sum the type of summation to perform
   * @return the resulting sum as a long
   */
  @Override // QueryEngine
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

  /**
   * Get a Function that performs a summation on an entire INode collection to a single Long.
   *
   * @param sum the sum to look for
   * @return the function representing the summation against the INode collection
   */
  private Function<Collection<INode>, Long> getSumFunctionForCollection(String sum) {
    switch (sum) {
      case "count":
        return collection -> ((long) collection.size());
      case "fileSize":
        return collection ->
            collection.parallelStream().mapToLong(node -> node.asFile().computeFileSize()).sum();
      case "diskspaceConsumed":
        return collection ->
            collection
                .parallelStream()
                .mapToLong(
                    node -> node.asFile().computeFileSize() * node.asFile().getFileReplication())
                .sum();
      case "blockSize":
        return collection ->
            collection
                .parallelStream()
                .mapToLong(node -> node.asFile().getPreferredBlockSize())
                .sum();
      case "numBlocks":
        return collection ->
            collection.parallelStream().mapToLong(node -> node.asFile().numBlocks()).sum();
      case "numReplicas":
        return collection ->
            collection
                .parallelStream()
                .mapToLong(
                    node -> ((long) node.asFile().numBlocks() * node.asFile().getFileReplication()))
                .sum();
      case "memoryConsumed":
        return collection ->
            collection
                .parallelStream()
                .mapToLong(
                    node -> {
                      long inodeSize = 150L;
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

  /**
   * Get a Function that converts a String into a Boolean expression.
   *
   * @param value the value to compute against
   * @param op the operation to perform
   * @return the function representing a String to Boolean transformation
   */
  @Override // QueryEngine
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

  /**
   * Get a Function that converts a Boolean into a Boolean expression.
   *
   * @param value the value to compute against
   * @param op the operation to perform
   * @return the function representing a Boolean to Boolean transformation
   */
  @Override // QueryEngine
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

  /**
   * Get a Function that converts a Long into a Boolean expression.
   *
   * @param value the value to compute against
   * @param op the operation to perform
   * @return the function representing a Long to Boolean transformation
   */
  @Override // QueryEngine
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

  /**
   * Dump collection of INodes to parameter HTTP response.
   *
   * @param inodes the collection to dump
   * @param limit some limit of inodes to show
   * @param resp the HTTP response
   * @throws IOException error in dumping collection
   */
  @Override // QueryEngine
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

  /**
   * Creates histogram with only entries that satisfy the conditional String. Conditional String ex:
   * 'gte:1000' should create a histogram where all entries have values greater than or equal to
   * 1000L. NOTE: Modifies the parameter histogram.
   *
   * @param histogram data points of histogram
   * @param histogramConditionsStr conditional string to filter out the given histogram
   * @return filtered histogram as per the given conditional string
   */
  @Override // QueryEngine
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
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Removing {} keys from histogram of size {} using conditional String:'{}', took: {} ms.",
          keys.size(),
          originalHistSize,
          histogramConditionsStr,
          (e1 - s1));
    }

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
  @Override // QueryEngine
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
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Removing {} keys from histogram2 of size {} using conditional String:'{}', took: {} ms.",
          keys.size(),
          originalHistSize,
          histogramConditionsStr,
          (e1 - s1));
    }

    return histogram;
  }

  private List<Function<List<Long>, Boolean>> createIndexedComparisons(String conditionsStr) {
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

  /**
   * Check the parameter `value` against a series of checks.
   *
   * @param comparisons the Long to Boolean comparison functions
   * @param value the Long value to compare against
   * @return true if checks passed; false otherwise
   */
  @Override // QueryEngine
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

  private boolean check(List<Function<List<Long>, Boolean>> comparisons, List<Long> value) {
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

  @Override // QueryEngine
  public Function<INode, String> getCustomLongToStringGroupingFunction(
      Function<INode, Long> toLongFunction, Function<Long, String> longToStringFunction) {
    return node -> longToStringFunction.apply(toLongFunction.apply(node));
  }

  /**
   * Produces a 2-level gropued Histogram for generic summation.
   *
   * @param inodes inodes
   * @param namingFunction1 function to string for top level grouping
   * @param namingFunction2 function to string for second level grouping
   * @param dataFunction function to long
   * @return histogram of sums
   */
  @Override // QueryEngine
  public Map<String, Map<String, Long>> genericTwoLevelHistogram(
      Stream<INode> inodes,
      Function<INode, String> namingFunction1,
      Function<INode, String> namingFunction2,
      Function<INode, Long> dataFunction) {
    return inodes.collect(
        Collectors.groupingBy(
            namingFunction1,
            Collectors.groupingBy(
                namingFunction2,
                Collectors.mapping(dataFunction, Collectors.summingLong(i -> i)))));
  }

  /**
   * Produces Histogram for generic summation.
   *
   * @param inodes inodes
   * @param namingFunction function to string
   * @param dataFunction function to long
   * @return histogram of sums
   */
  @Override // QueryEngine
  public Map<String, Long> genericSummingHistogram(
      Stream<INode> inodes,
      Function<INode, String> namingFunction,
      Function<INode, Long> dataFunction) {
    return inodes.collect(
        Collectors.groupingBy(
            namingFunction, Collectors.mapping(dataFunction, Collectors.summingLong(i -> i))));
  }

  /**
   * Produces Histogram for generic summarization. Primarily for use by SuggestionEngine where
   * obtaining sum and count is vital.
   *
   * @param inodes inodes
   * @param namingFunction function to string
   * @param dataFunction function to long
   * @return histogram of sums
   */
  @Override // QueryEngine
  public Map<String, LongSummaryStatistics> genericSummarizingHistogram(
      Stream<INode> inodes,
      Function<INode, String> namingFunction,
      Function<INode, Long> dataFunction) {
    return inodes.collect(
        Collectors.groupingBy(
            namingFunction, Collectors.mapping(dataFunction, Collectors.summarizingLong(i -> i))));
  }

  /**
   * Produces either Histogram for generic summarization or finding of max/min/avg.
   *
   * @param inodes inodes
   * @param namingFunction function to string
   * @param dataFunction function to long
   * @return histogram of sums
   */
  @Override // QueryEngine
  public Map<String, Long> genericSumOrFindHistogram(
      Stream<INode> inodes,
      Function<INode, String> namingFunction,
      ToLongFunction<INode> dataFunction,
      String find) {
    String findOp;
    String findField;
    if (find != null && find.length() != 0) {
      String[] finds = find.split(":");
      findOp = finds[0];
      findField = finds[1];
      switch (findOp) {
        case "max":
          return genericMaxxingHistogram(
              inodes,
              namingFunction,
              Helper.convertToLongFunction(getFilterFunctionToLongForINode(findField)));
        case "min":
          return genericMinningHistogram(
              inodes,
              namingFunction,
              Helper.convertToLongFunction(getFilterFunctionToLongForINode(findField)));
        case "avg":
          return genericAvgingHistogram(
              inodes,
              namingFunction,
              Helper.convertToLongFunction(getFilterFunctionToLongForINode(findField)));
        default:
          throw new IllegalArgumentException(
              "findOp: " + findOp + ", is not a valid find. Check /finds for valid options.");
      }
    } else {
      return genericSummingHistogram(inodes, namingFunction, dataFunction::applyAsLong);
    }
  }

  private Map<String, Long> genericMinningHistogram(
      Stream<INode> inodes,
      Function<INode, String> namingFunction,
      ToLongFunction<INode> dataFunction) {
    Map<String, Optional<INode>> collect =
        inodes.collect(
            Collectors.groupingBy(
                namingFunction, Collectors.minBy(Comparator.comparingLong(dataFunction))));
    HashMap<String, Long> histogram = new HashMap<>(collect.size());
    for (Entry<String, Optional<INode>> entry : collect.entrySet()) {
      if (entry.getValue().isPresent()) {
        histogram.put(entry.getKey(), dataFunction.applyAsLong(entry.getValue().get()));
      }
    }
    return histogram;
  }

  private Map<String, Long> genericMaxxingHistogram(
      Stream<INode> inodes,
      Function<INode, String> namingFunction,
      ToLongFunction<INode> dataFunction) {
    Map<String, Optional<INode>> collect =
        inodes.collect(
            Collectors.groupingBy(
                namingFunction, Collectors.maxBy(Comparator.comparingLong(dataFunction))));
    HashMap<String, Long> histogram = new HashMap<>(collect.size());
    for (Entry<String, Optional<INode>> entry : collect.entrySet()) {
      if (entry.getValue().isPresent()) {
        histogram.put(entry.getKey(), dataFunction.applyAsLong(entry.getValue().get()));
      }
    }
    return histogram;
  }

  private Map<String, Long> genericAvgingHistogram(
      Stream<INode> inodes,
      Function<INode, String> namingFunction,
      ToLongFunction<INode> dataFunction) {
    Map<String, Double> collect =
        inodes.collect(
            Collectors.groupingBy(
                namingFunction,
                Collectors.averagingDouble(n -> (double) dataFunction.applyAsLong(n))));
    HashMap<String, Long> histogram = new HashMap<>(collect.size());
    for (Entry<String, Double> entry : collect.entrySet()) {
      histogram.put(entry.getKey(), entry.getValue().longValue());
    }
    return histogram;
  }

  /**
   * Splits a conditional String and ANDs them together.
   *
   * @param conditionsStr conditional string seperated by semicolons; conditions by colons
   * @return a list of functions that represent the conditional
   */
  @Override // QueryEngine
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

  /**
   * Creates a histogram representation of INodes where the X-axis represents diskspace consumed.
   *
   * @param inodes the filtered inodes to operate with
   * @param sum the Y-axis type
   * @param find optional; a find operation to perform; overrides sum
   * @param transformMap a transform to overlay during histogram processing
   * @return a map representing bins as Strings and the sum/finds as Longs
   */
  @Override // QueryEngine
  public Map<String, Long> diskspaceConsumedHistogram(
      Stream<INode> inodes,
      String sum,
      String find,
      Map<String, Function<INode, Long>> transformMap) {
    return new HistogramInvoker(this, "diskspaceConsumed", sum, find, inodes)
        .invoke()
        .getHistogram();
  }

  /**
   * Creates a histogram representation of INodes where the X-axis represents memory consumed.
   *
   * @param inodes the filtered inodes to operate with
   * @param sum the Y-axis type
   * @param find optional; a find operation to perform; overrides sum
   * @return a map representing bins as Strings and the sum/finds as Longs
   */
  @Override // QueryEngine
  public Map<String, Long> memoryConsumedHistogram(Stream<INode> inodes, String sum, String find) {
    return new HistogramInvoker(this, "memoryConsumed", sum, find, inodes).invoke().getHistogram();
  }

  /**
   * Creates a histogram representation of INodes where the X-axis represents file size.
   *
   * @param inodes the filtered inodes to operate with
   * @param sum the Y-axis type
   * @param find optional; a find operation to perform; overrides sum
   * @return a map representing bins as Strings and the sum/finds as Longs
   */
  @Override // QueryEngine
  public Map<String, Long> fileSizeHistogram(Stream<INode> inodes, String sum, String find) {
    return new HistogramInvoker(this, "fileSize", sum, find, inodes).invoke().getHistogram();
  }

  /**
   * Creates a histogram representation of INodes where the X-axis represents replication factors.
   *
   * @param inodes the filtered inodes to operate with
   * @param sum the Y-axis type
   * @param find optional; a find operation to perform; overrides sum
   * @return a map representing bins as Strings and the sum/finds as Longs
   */
  @Override // QueryEngine
  public Map<String, Long> fileReplicaHistogram(
      Stream<INode> inodes,
      String sum,
      String find,
      Map<String, Function<INode, Long>> transformMap) {
    return new HistogramInvoker(this, "fileReplica", sum, find, inodes).invoke().getHistogram();
  }

  /**
   * Creates a histogram representation of INodes where the X-axis represents storage policies.
   *
   * @param inodes the filtered inodes to operate with
   * @param sum the Y-axis type
   * @param find optional; a find operation to perform; overrides sum
   * @return a map representing bins as Strings and the sum/finds as Longs
   */
  @Override // QueryEngine
  public Map<String, Long> storageTypeHistogram(Stream<INode> inodes, String sum, String find) {
    return new HistogramInvoker(this, "storageGroup", sum, find, inodes).invoke().getHistogram();
  }

  /**
   * Creates a histogram representation of INodes where the X-axis represents access time ranges.
   *
   * @param inodes the filtered inodes to operate with
   * @param sum the Y-axis type
   * @param find optional; a find operation to perform; overrides sum
   * @return a map representing bins as Strings and the sum/finds as Longs
   */
  @Override // QueryEngine
  public Map<String, Long> accessTimeHistogram(
      Stream<INode> inodes, String sum, String find, String timeRange) {
    return new HistogramInvoker(this, "accessTime", sum, find, inodes).invoke().getHistogram();
  }

  /**
   * Creates a histogram representation of INodes where the X-axis represents modification time
   * ranges.
   *
   * @param inodes the filtered inodes to operate with
   * @param sum the Y-axis type
   * @param find optional; a find operation to perform; overrides sum
   * @return a map representing bins as Strings and the sum/finds as Longs
   */
  @Override // QueryEngine
  public Map<String, Long> modTimeHistogram(
      Stream<INode> inodes, String sum, String find, String timeRange) {
    return new HistogramInvoker(this, "modTime", sum, find, inodes).invoke().getHistogram();
  }

  /**
   * Creates a histogram representation of INodes where the X-axis represents user names.
   *
   * @param inodes the filtered inodes to operate with
   * @param sum the Y-axis type
   * @param find optional; a find operation to perform; overrides sum
   * @return a map representing bins as Strings and the sum/finds as Longs
   */
  @Override // QueryEngine
  public Map<String, Long> byUserHistogram(Stream<INode> inodes, String sum, String find) {
    return new HistogramInvoker(this, "user", sum, find, inodes).invoke().getHistogram();
  }

  /**
   * Creates a histogram representation of INodes where the X-axis represents group names.
   *
   * @param inodes the filtered inodes to operate with
   * @param sum the Y-axis type
   * @param find optional; a find operation to perform; overrides sum
   * @return a map representing bins as Strings and the sum/finds as Longs
   */
  @Override // QueryEngine
  public Map<String, Long> byGroupHistogram(Stream<INode> inodes, String sum, String find) {
    return new HistogramInvoker(this, "group", sum, find, inodes).invoke().getHistogram();
  }

  /**
   * Creates a histogram representation of INodes where the X-axis represents parent directories.
   *
   * @param inodes the filtered inodes to operate with
   * @param parentDirDepth the depth of the parents to group on
   * @param sum the Y-axis type
   * @param find optional; a find operation to perform; overrides sum
   * @return a map representing bins as Strings and the sum/finds as Longs
   */
  @Override // QueryEngine
  public Map<String, Long> parentDirHistogram(
      Stream<INode> inodes, Integer parentDirDepth, String sum, String find) {
    return new HistogramInvoker(this, "parentDir", sum, find, inodes).invoke().getHistogram();
  }

  /**
   * Creates a histogram representation of INodes where the X-axis represents a file type extension.
   *
   * @param inodes the filtered inodes to operate with
   * @param sum the Y-axis type
   * @param find optional; a find operation to perform; overrides sum
   * @return a map representing bins as Strings and the sum/finds as Longs
   */
  @Override // QueryEngine
  public Map<String, Long> fileTypeHistogram(Stream<INode> inodes, String sum, String find) {
    return new HistogramInvoker(this, "fileType", sum, find, inodes).invoke().getHistogram();
  }

  /**
   * Creates a histogram representation of INodes where the X-axis represents a directory quota.
   * WARNING: This should only ever be called with a "small" set of directory INodes!
   *
   * @param inodes the filtered inodes to operate with
   * @param sum the Y-axis type
   * @return a map representing bins as Strings and the sum/finds as Longs
   */
  @Override // QueryEngine
  public Map<String, Long> dirQuotaHistogram(Stream<INode> inodes, String sum) {
    return new HistogramInvoker(this, "dirQuota", sum, null, inodes).invoke().getHistogram();
  }

  private Function<INode, Long> getTransformFunction(
      Function<INode, Long> stdFunc,
      Map<String, Function<INode, Long>> transformMap,
      String transformKey) {
    if (transformMap != null && transformMap.containsKey(transformKey)) {
      LOG.info("Function transformed for: {}", transformKey);
      return transformMap.get(transformKey);
    }
    return stdFunc;
  }

  @Override // QueryEngine
  public void clear() {
    if (all != null) {
      all.clear();
    }
    if (files != null) {
      files.clear();
    }
    if (dirs != null) {
      dirs.clear();
    }
  }
}
