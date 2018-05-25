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

import com.google.common.collect.Sets;
import com.paypal.namenode.HSQLDriver;
import com.paypal.security.SecurityConfiguration;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.SQLException;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.queries.FileTypeHistogram;
import org.apache.hadoop.hdfs.server.namenode.queries.Histograms;
import org.apache.hadoop.hdfs.server.namenode.queries.MemorySizeHistogram;
import org.apache.hadoop.hdfs.server.namenode.queries.SpaceSizeHistogram;
import org.apache.hadoop.hdfs.server.namenode.queries.TimeHistogram;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressView;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.TokenExtractor;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.GSetCollectionWrapper;
import org.apache.hadoop.util.GSetParallelWrapper;
import org.apache.hadoop.util.GSetSeperatorWrapper;
import org.apache.hadoop.util.MapSerializer;
import org.apache.hadoop.util.VirtualINodeTree;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NNLoader {

  public static final Logger LOG =
      LoggerFactory.getLogger(NNLoader.class.getName());

  private final DB cache = DBMaker
      .fileDB("/usr/local/nn-analytics/db/nna_cache")
      .fileMmapEnable()
      .transactionEnable()
      .closeOnJvmShutdown()
      .cleanerHackEnable()
      .make();

  private final Map<String, Long> cachedValues =
      Collections.synchronizedMap(
          cache.hashMap("cachedValues", Serializer.STRING, Serializer.LONG).createOrOpen());
  private final Map<String, Map<String, Long>> cachedMaps =
      Collections.synchronizedMap(
          cache.hashMap("cachedMaps", Serializer.STRING, new MapSerializer()).createOrOpen());
  private final Map<String, Long> cachedLogins =
      Collections.synchronizedMap(
          cache.hashMap("cachedLogins", Serializer.STRING, Serializer.LONG).createOrOpen());
  private final Set<String> cachedUsers =
      Collections.synchronizedSet(cache.hashSet("cachedUsers", Serializer.STRING).createOrOpen());
  private final Set<String> cachedDirs =
      Collections.synchronizedSet(cache.hashSet("cachedDirs", Serializer.STRING).createOrOpen());
  private final Map<String, Map<String, Long>> cachedUserNsQuotas =
      Collections.synchronizedMap(
          cache.hashMap("cachedUserNsQuotas", Serializer.STRING, new MapSerializer())
              .createOrOpen());
  private final Map<String, Map<String, Long>> cachedUserDsQuotas =
      Collections.synchronizedMap(
          cache.hashMap("cachedUserDsQuotas", Serializer.STRING, new MapSerializer())
              .createOrOpen());

  private AtomicBoolean inited = new AtomicBoolean(false);
  private AtomicBoolean historical = new AtomicBoolean(false);
  private AtomicBoolean suggestive = new AtomicBoolean(false);
  private Configuration conf = null;
  private FSNamesystem namesystem = null;
  private HSQLDriver hsqlDriver = null;
  private Set<INode> all = null;
  private Map<INode, INode> files = null;
  private Map<INode, INode> dirs = null;
  private TokenExtractor tokenExtractor = null;
  private VersionInterface versionLoader = null;
  
  public NNLoader() {
    versionLoader = new VersionContext();
  }

  public boolean isInit() {
    return inited.get();
  }

  public boolean isHistorical() {
    return historical.get();
  }

  public boolean isSuggestive() {
    return suggestive.get();
  }

  public long getCurrentTxID() {
    if (namesystem == null) {
      return -1L;
    }
    return namesystem.getFSImage().lastAppliedTxId;
  }

  public String getAuthority() {
    if (conf == null) {
      return "test";
    }
    String authority = new Path(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY)).toUri()
        .getAuthority();
    if (authority == null) {
      return "test";
    }
    return authority;
  }

  public void sendLoadingStatus(HttpServletResponse resp) throws IOException {
    String COUNT = "count";
    String ELAPSED_TIME = "elapsedTime";
    String FILE = "file";
    String NAME = "name";
    String DESC = "desc";
    String PERCENT_COMPLETE = "percentComplete";
    String PHASES = "phases";
    String SIZE = "size";
    String STATUS = "status";
    String STEPS = "steps";
    String TOTAL = "total";

    StartupProgressView view = NameNode.getStartupProgress().createView();
    JsonGenerator json = new JsonFactory().createJsonGenerator(resp.getWriter())
        .useDefaultPrettyPrinter();

    try {
      json.writeStartObject();
      json.writeNumberField(ELAPSED_TIME, view.getElapsedTime());
      json.writeNumberField(PERCENT_COMPLETE, view.getPercentComplete());
      json.writeArrayFieldStart(PHASES);

      for (Phase phase : view.getPhases()) {
        json.writeStartObject();
        json.writeStringField(NAME, phase.getName());
        json.writeStringField(DESC, phase.getDescription());
        json.writeStringField(STATUS, view.getStatus(phase).toString());
        json.writeNumberField(PERCENT_COMPLETE, view.getPercentComplete(phase));
        json.writeNumberField(ELAPSED_TIME, view.getElapsedTime(phase));
        writeStringFieldIfNotNull(json, FILE, view.getFile(phase));
        writeNumberFieldIfDefined(json, SIZE, view.getSize(phase));
        json.writeArrayFieldStart(STEPS);

        for (Step step : view.getSteps(phase)) {
          json.writeStartObject();
          StepType stepType = step.getType();
          if (stepType != null) {
            json.writeStringField(NAME, stepType.getName());
            json.writeStringField(DESC, stepType.getDescription());
          }
          json.writeNumberField(COUNT, view.getCount(phase, step));
          writeStringFieldIfNotNull(json, FILE, step.getFile());
          writeNumberFieldIfDefined(json, SIZE, step.getSize());
          json.writeNumberField(TOTAL, view.getTotal(phase, step));
          json.writeNumberField(PERCENT_COMPLETE, view.getPercentComplete(phase, step));
          json.writeNumberField(ELAPSED_TIME, view.getElapsedTime(phase, step));
          json.writeEndObject();
        }

        json.writeEndArray();
        json.writeEndObject();
      }

      json.writeEndArray();
      json.writeEndObject();
    } finally {
      IOUtils.closeStream(json);
    }
  }

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
    LOG.info("Fetching set of: " + set + " had result size: " + inodes.size() + " and took: " + (end
        - start) + " ms.");
    return inodes;
  }

  public void dumpINodeInDetail(String path,
      HttpServletResponse resp)
      throws IOException {
    versionLoader.dumpINodeInDetail(path, resp);
  }

  public void dumpConfig(HttpServletResponse resp) throws IOException {
    PrintWriter writer = resp.getWriter();
    try {
      conf.writeXml(writer);
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  public String getConfigValue(String key) throws IOException {
    return conf.get(key);
  }

  public void dumpLog(Integer charsLimitVar, HttpServletResponse resp)
      throws IOException {
    int charLimit = (charsLimitVar != null) ? charsLimitVar : 4000;
    LOG.info("Dumping last " + charLimit + " chars of logging to a client.");
    long start = System.currentTimeMillis();
    PrintWriter writer = resp.getWriter();
    String logPath = System.getProperty("hadoop.log.dir", "/var/log/nn-analytics");
    String logFile = System.getProperty("hadoop.log.file", "nn-analytics.log");
    RandomAccessFile reader = new RandomAccessFile(logPath + "/" + logFile, "r");
    long startOffsetCalc = reader.length() - charLimit;
    long startOffset = (startOffsetCalc < 0) ? 0 : startOffsetCalc;
    reader.seek(startOffset);
    try {
      for (int charsRead = 0; charsRead < charLimit; charsRead++) {
        int charac = reader.read();
        writer.write(charac);
        writer.flush();
      }
    } finally {
      IOUtils.closeStream(reader);
      IOUtils.closeStream(writer);
      LOG.info("Closed response.");
    }
    long end = System.currentTimeMillis();
    LOG.info("Dumping the log response took " + (end - start) + "ms.");
  }

  public Collection<INode> combinedFilter(Collection<INode> inodes,
      String[] filters,
      String[] filterOps) {
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
      LOG.info("Performing filters: " + Arrays.asList(filters) + ", with filterOps: " +
          Arrays.asList(filterOps) + " took: " + (end - start) + " ms.");
    }
  }

  public Collection<INode> findFilter(Collection<INode> inodes,
      String find) {

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
      LOG.info("Performing find: " + Arrays.asList(findOps) + ", took: " + (end - start) + " ms.");
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
        Function<Long, Boolean> longCompFunction = getFilterFunctionForLong(Long.parseLong(opValue),
            op);
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
        Function<Boolean, Boolean> boolCompFunction = getFilterFunctionForBoolean(
            Boolean.parseBoolean(opValue), op);
        return boolFunction.andThen(boolCompFunction);
      }

      throw new IllegalArgumentException(
          "Failed to determine filter: " + filter + ", with operations: " +
              Arrays.asList(filterOps) + ".\nCheck your filter arguments." +
              "\nPossible filters and operations available at /filters and /filterOps.");
    } finally {
      long end = System.currentTimeMillis();
      LOG.info("Obtaining filter: " + filter + ", with filterOps: " + Arrays.asList(filterOps) +
          " took: " + (end - start) + " ms.");
    }
  }

  public Long sum(Collection<INode> inodes, String sum) {
    long startTime = System.currentTimeMillis();
    try {
      Function<Collection<INode>, Long> sumFunction = getSumFunctionForCollection(sum);
      return sumFunction.apply(inodes);
    } finally {
      long endTime = System.currentTimeMillis();
      LOG.info("Performing sum: " + sum + " took: " + (endTime - startTime) + " ms.");
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
        return collection -> StreamSupport.stream(collection.spliterator(), true)
            .mapToLong(node -> node.asFile().computeFileSize())
            .sum();
      case "diskspaceConsumed":
        return collection -> StreamSupport.stream(collection.spliterator(), true)
            .mapToLong(node -> node.asFile().computeFileSize() * node.asFile().getFileReplication())
            .sum();
      case "blockSize":
        return collection -> StreamSupport.stream(collection.spliterator(), true)
            .mapToLong(node -> node.asFile().getPreferredBlockSize())
            .sum();
      case "numBlocks":
        return collection -> StreamSupport.stream(collection.spliterator(), true)
            .mapToLong(node -> node.asFile().numBlocks())
            .sum();
      case "numReplicas":
        return collection -> StreamSupport.stream(collection.spliterator(), true)
            .mapToLong(
                node -> ((long) node.asFile().numBlocks() * node.asFile().getFileReplication()))
            .sum();
      case "memoryConsumed":
        return collection -> StreamSupport.stream(collection.spliterator(), true)
            .mapToLong(node -> {
              long inodeSize = 100L;
              if (node.isFile()) {
                inodeSize += node.asFile().numBlocks() * 150L;
              }
              return inodeSize;
            }).sum();
      default:
        throw new IllegalArgumentException("Could not determine sum type: " + sum +
            ".\nPlease check /sums for available sums.");
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
        return node -> (long) ((double) versionLoader.getNSQuotaUsed(node) 
            / (double) versionLoader.getNSQuota(node) * 100);
      case "dsQuotaRatioUsed":
        return node -> (long) ((double) versionLoader.getDSQuotaUsed(node) 
            / (double) versionLoader.getDSQuota(node) * 100);
      case "nsQuotaUsed":
        return versionLoader::getNSQuotaUsed;
      case "dsQuotaUsed":
        return versionLoader::getDSQuotaUsed;
      case "nsQuota":
        return versionLoader::getNSQuota;
      case "dsQuota":
        return versionLoader::getDSQuota;
      default:
        throw new IllegalArgumentException("Could not determine sum type: " + sum +
            ".\nPlease check /sums for available sums.");
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
        throw new IllegalArgumentException("Failed to determine String filter operation.\n" +
            "Please check /filterOps and use operations meant for Strings.");
    }
  }

  public Function<Boolean, Boolean> getFilterFunctionForBoolean(Boolean value, String op) {
    switch (op) {
      case "eq":
        return b -> b == value;
      case "notEq":
        return b -> b != value;
      default:
        throw new IllegalArgumentException("Failed to determine Boolean filter operation.\n" +
            "Please check /filterOps and use operations meant for Booleans.");
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
        throw new IllegalArgumentException("Failed to determine Long filter operation.\n" +
            "Please check /filterOps and use operations meant for Longs.");
    }
  }

  private Function<INode, Long> getTransformFunction(Function<INode, Long> stdFunc,
      Map<String, Function<INode, Long>> transformMap,
      String transformKey) {
    if (transformMap.containsKey(transformKey)) {
      LOG.info("Function transformed for: " + transformKey);
      return transformMap.get(transformKey);
    }
    return stdFunc;
  }

  public Map<String, Long> diskspaceConsumedHistogram(Collection<INode> inodes,
      String sum,
      String find,
      Map<String, Function<INode, Long>> transformMap) {
    if (!isInit()) {
      return Collections.emptyMap();
    }
    if (find == null || find.length() == 0) {
      return diskspaceConsumedHistogramCpu(inodes, sum, transformMap);
    }
    return diskspaceConsumedHistogramCpuWithFind(inodes, find, transformMap);
  }

  private Map<String, Long> diskspaceConsumedHistogramCpu(Collection<INode> inodes,
      String sum,
      Map<String, Function<INode, Long>> transformMap) {
    Function<INode, Long> binFunc = getTransformFunction(
        getFilterFunctionToLongForINode("diskspaceConsumed"), transformMap, "diskspaceConsumed");
    Function<INode, Long> sumFunc = getTransformFunction(
        getSumFunctionForINode(sum), transformMap, sum);
    return filteringHistogram(inodes, sum, sumFunc, binFunc, SpaceSizeHistogram.getBinsArray(),
        SpaceSizeHistogram.getKeys());
  }

  private Map<String, Long> diskspaceConsumedHistogramCpuWithFind(Collection<INode> inodes,
      String find,
      Map<String, Function<INode, Long>> transformMap) {
    Function<INode, Long> binFunc = getTransformFunction(
        getFilterFunctionToLongForINode("diskspaceConsumed"), transformMap, "diskspaceConsumed");
    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];
    return filteringHistogramWithFind(inodes, findField, findOp,
        getFilterFunctionToLongForINode(findField), binFunc,
        SpaceSizeHistogram.getBinsArray(), SpaceSizeHistogram.getKeys());
  }

  public Map<String, Long> memoryConsumedHistogram(Collection<INode> inodes, String sum,
      String find) {
    if (!isInit()) {
      return Collections.emptyMap();
    }
    if (find == null || find.length() == 0) {
      return memoryConsumedHistogramCpu(inodes, sum);
    } else {
      return memoryConsumedHistogramCpuWithFind(inodes, find);
    }
  }

  private Map<String, Long> memoryConsumedHistogramCpu(Collection<INode> inodes, String sum) {
    Function<INode, Long> memConsumedFunction = node -> {
      long inodeSize = 100L;
      if (node.isFile()) {
        inodeSize += node.asFile().numBlocks() * 150L;
      }
      return inodeSize;
    };
    return filteringHistogram(inodes, sum, getSumFunctionForINode(sum), memConsumedFunction,
        MemorySizeHistogram.getBinsArray(), MemorySizeHistogram.getKeys());
  }

  private Map<String, Long> memoryConsumedHistogramCpuWithFind(Collection<INode> inodes,
      String find) {
    Function<INode, Long> memConsumedFunction = node -> {
      long inodeSize = 100L;
      if (node.isFile()) {
        inodeSize += node.asFile().numBlocks() * 150L;
      }
      return inodeSize;
    };
    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];

    return filteringHistogramWithFind(inodes, findField, findOp,
        getFilterFunctionToLongForINode(findField), memConsumedFunction,
        MemorySizeHistogram.getBinsArray(), MemorySizeHistogram.getKeys());
  }

  private long[][] fetchDataViaCpu(Collection<INode> inodes,
      String sum,
      Function<INode, Long> sumFunc,
      Function<INode, Long> nodeToLong) {
    long start = System.currentTimeMillis();

    long[] data = inodes.parallelStream().mapToLong(nodeToLong::apply).toArray();
    long[] sums = inodes.parallelStream().mapToLong(sumFunc::apply).toArray();

    long end = System.currentTimeMillis();
    LOG.info("Fetching " + sum + " data took: " + (end - start) + " ms.");

    return new long[][]{data, sums};
  }

  private Map<String, Long> strictMappingHistogram(Collection<INode> inodes,
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
        IntStream.range(0, data.length).parallel().forEach(idx ->
            {
              int id = (int) data[idx];
              int chosenBin = maxId + 1;
              if (id < chosenBin && id != -1) {
                // Lock in the bin.
                chosenBin = id;
              }
              synchronized (histogram) {
                histogram[chosenBin] += sums[idx];
              }
            }
        );
        LOG.info("Histogram returned an array of size: " + histogram.length);
      }
    } catch (Throwable e) {
      LOG.info("Encountered exception during loading:\n" + e);
      for (StackTraceElement stacktrace : e.getStackTrace()) {
        LOG.info(stacktrace.toString());
      }
      throw e;
    }
    long end1 = System.currentTimeMillis();
    LOG.info("Histogram took: " + (end1 - start1) + " ms.");
    LOG.info("Histogram result has size: " + histogram.length + ".");
    if (histogram.length > 100) {
      LOG.info("It is too big to console out.");
    } else {
      LOG.info("Result is: " + java.util.Arrays.toString(histogram));
    }
    return Histograms.mapToNonEmptyIndex(histogram);
  }

  private Map<String, Long> strictMappingHistogramWithFind(Collection<INode> inodes,
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
        IntStream.range(0, data.length).parallel().forEach(idx ->
            {
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
            }
        );
        LOG.info("Histogram returned an array of size: " + histogram.length);
      }
    } catch (Throwable e) {
      LOG.info("Encountered exception during loading:\n" + e);
      for (StackTraceElement stacktrace : e.getStackTrace()) {
        LOG.info(stacktrace.toString());
      }
      throw e;
    }
    long end1 = System.currentTimeMillis();
    LOG.info("Histogram (with find) took: " + (end1 - start1) + " ms.");
    LOG.info("Histogram (with find) result has size: " + histogram.length);
    if (histogram.length > 100) {
      LOG.info(". It is too big to console out.");
    } else {
      LOG.info(", is: " + java.util.Arrays.toString(histogram));
    }
    return Histograms.mapToNonEmptyIndex(histogram);
  }

  Map<String, Long> binMappingHistogram(Collection<INode> inodes,
      String sum,
      Function<INode, Long> sumFunc,
      Function<INode, Long> nodeToLong,
      Map<String, Long> binKeyMap) {
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
        histogram = new long[binKeyMap.size() + 1];
        IntStream.range(0, data.length).parallel().forEach(idx ->
            {
              int id = (int) data[idx];
              int chosenBin = binKeyMap.size();
              if (id < chosenBin && id != -1) {
                // Lock in the bin.
                chosenBin = id;
              }
              synchronized (histogram) {
                histogram[chosenBin] += sums[idx];
              }
            }
        );
        LOG.info("Histogram returned an array of size: " + histogram.length);
      }
    } catch (Throwable e) {
      LOG.info("Encountered exception during loading:\n" + e);
      for (StackTraceElement stacktrace : e.getStackTrace()) {
        LOG.info(stacktrace.toString());
      }
      throw e;
    }
    long end1 = System.currentTimeMillis();
    LOG.info("Histogram took: " + (end1 - start1) + " ms.");
    LOG.info("Histogram result has size: " + histogram.length);
    if (histogram.length > 100) {
      LOG.info(". It is too big to console out.");
    } else {
      LOG.info(", is: " + java.util.Arrays.toString(histogram));
    }
    return Histograms.mapByKeys(binKeyMap, histogram);
  }

  Map<String, Long> binMappingHistogramWithFind(Collection<INode> inodes,
      String findFunc,
      Function<INode, Long> findToLong,
      Function<INode, Long> nodeToLong,
      Map<String, Long> binKeyMap) {
    long[][] datas = fetchDataViaCpu(inodes, findFunc, findToLong, nodeToLong);
    long[] data = datas[0];
    long[] sums = datas[1];

    long start1 = System.currentTimeMillis();
    long[] histogram;
    try {
      if (data.length == 0) {
        histogram = data;
        LOG.info("Empty data set; skipping.");
      } else if (findFunc.equals("avg")) {
        BigInteger[] bigHistogram = new BigInteger[binKeyMap.size() + 1];
        long[] counts = new long[binKeyMap.size() + 1];
        Arrays.fill(bigHistogram, BigInteger.valueOf(-1));
        IntStream.range(0, data.length).parallel().forEach(idx ->
            {
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
                  bigHistogram[chosenBin] = bigHistogram[chosenBin].add(BigInteger.valueOf(sum));
                }
                counts[chosenBin]++;
              }
            }
        );
        if (bigHistogram[binKeyMap.size()].equals(BigInteger.valueOf(-1))) {
          bigHistogram[binKeyMap.size()] = BigInteger.ZERO;
        }
        for (int i = 0; i < bigHistogram.length; i++) {
          if (counts[i] != 0) {
            bigHistogram[i] = bigHistogram[i].divide(BigInteger.valueOf(counts[i]));
          }
        }
        histogram = Arrays.stream(bigHistogram).mapToLong(BigInteger::longValue).toArray();
        LOG.info("Histogram returned an array of size: " + histogram.length);
      } else {
        histogram = new long[binKeyMap.size() + 1];
        Arrays.fill(histogram, -1L);
        IntStream.range(0, data.length).parallel().forEach(idx ->
            {
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
            }
        );
        if (histogram[binKeyMap.size()] == -1) {
          histogram[binKeyMap.size()] = 0;
        }
        LOG.info("Histogram returned an array of size: " + histogram.length);
      }
    } catch (Throwable e) {
      LOG.info("Encountered exception during loading:\n" + e);
      for (StackTraceElement stacktrace : e.getStackTrace()) {
        LOG.info(stacktrace.toString());
      }
      throw e;
    }
    long end1 = System.currentTimeMillis();
    LOG.info("Histogram (with find) took: " + (end1 - start1) + " ms.");
    LOG.info("Histogram (with find) result has size: " + histogram.length);
    if (histogram.length > 100) {
      LOG.info(". It is too big to console out.");
    } else {
      LOG.info(", is: " + java.util.Arrays.toString(histogram));
    }
    return Histograms.mapByKeys(binKeyMap, histogram);
  }

  private Map<String, Long> filteringHistogram(Collection<INode> inodes,
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
        IntStream.range(0, data.length).parallel().forEach(idx ->
            {
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
            }
        );
        LOG.info("Histogram returned an array of size: " + histogram.length);
      }
    } catch (Throwable e) {
      LOG.info("Encountered exception during loading:\n" + e);
      for (StackTraceElement stacktrace : e.getStackTrace()) {
        LOG.info(stacktrace.toString());
      }
      throw e;
    }
    long end1 = System.currentTimeMillis();
    LOG.info("Histogram took: " + (end1 - start1) + " ms.");
    LOG.info("Histogram result has size: " + histogram.length);
    if (histogram.length > 100) {
      LOG.info(". It is too big to console out.");
    } else {
      LOG.info(", is: " + java.util.Arrays.toString(histogram));
    }
    return Histograms.sortByKeys(keys, histogram);
  }

  private Map<String, Long> filteringHistogramWithFind(Collection<INode> inodes,
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
        IntStream.range(0, data.length).parallel().forEach(idx ->
            {
              int id = (int) data[idx];
              int chosenBin = binsArray.length;
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
                  bigHistogram[chosenBin] = bigHistogram[chosenBin].add(BigInteger.valueOf(sum));
                }
                counts[chosenBin]++;
              }
            }
        );
        for (int i = 0; i < bigHistogram.length; i++) {
          if (counts[i] != 0) {
            bigHistogram[i] = bigHistogram[i].divide(BigInteger.valueOf(counts[i]));
          }
        }
        histogram = Arrays.stream(bigHistogram).mapToLong(BigInteger::longValue).toArray();
        LOG.info("Histogram returned an array of size: " + histogram.length);
      } else {
        histogram = new long[binsArray.length + 1];
        IntStream.range(0, data.length).parallel().forEach(idx ->
            {
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
            }
        );
        LOG.info("Histogram returned an array of size: " + histogram.length);
      }
    } catch (Throwable e) {
      LOG.info("Encountered exception during loading:\n" + e);
      for (StackTraceElement stacktrace : e.getStackTrace()) {
        LOG.info(stacktrace.toString());
      }
      throw e;
    }
    long end1 = System.currentTimeMillis();
    LOG.info("Histogram (with find) took: " + (end1 - start1) + " ms.");
    LOG.info("Histogram (with find) result has size: " + histogram.length);
    if (histogram.length > 100) {
      LOG.info(". It is too big to console out.");
    } else {
      LOG.info(", is: " + java.util.Arrays.toString(histogram));
    }
    return Histograms.sortByKeys(keys, histogram);
  }

  public Map<String, Long> fileSizeHistogram(Collection<INode> inodes, String sum, String find) {
    if (!isInit()) {
      return Collections.emptyMap();
    }
    if (find == null || find.length() == 0) {
      return fileSizeHistogramCpu(inodes, sum);
    }
    return fileSizeHistogramCpuWithFind(inodes, find);
  }

  private Map<String, Long> fileSizeHistogramCpu(Collection<INode> inodes, String sum) {
    return filteringHistogram(inodes, sum, getSumFunctionForINode(sum),
        node -> node.asFile().computeFileSize(),
        SpaceSizeHistogram.getBinsArray(), SpaceSizeHistogram.getKeys());
  }

  private Map<String, Long> fileSizeHistogramCpuWithFind(Collection<INode> inodes, String find) {
    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];
    return filteringHistogramWithFind(inodes, findField, findOp,
        getFilterFunctionToLongForINode(findField), node -> node.asFile().computeFileSize(),
        SpaceSizeHistogram.getBinsArray(), SpaceSizeHistogram.getKeys());
  }

  public Map<String, Long> fileReplicaHistogram(Collection<INode> inodes, String sum, String find,
      Map<String, Function<INode, Long>> transformMap) {
    if (!isInit()) {
      return Collections.emptyMap();
    }
    if (find == null || find.length() == 0) {
      return fileReplicaHistogramCpu(inodes, sum, transformMap);
    }
    return fileReplicaHistogramCpuWithFind(inodes, find);
  }

  private Map<String, Long> fileReplicaHistogramCpu(Collection<INode> inodes, String sum,
      Map<String, Function<INode, Long>> transformMap) {
    Function<INode, Long> binFunc = getTransformFunction(
        getFilterFunctionToLongForINode("fileReplica"), transformMap, "fileReplica");
    Function<INode, Long> sumFunc = getTransformFunction(getSumFunctionForINode(sum), transformMap,
        sum);
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
    if (!isInit()) {
      return Collections.emptyMap();
    }
    if (find == null || find.length() == 0) {
      return storageTypeHistogramCpu(inodes, sum);
    }
    return storageTypeHistogramCpuWithFind(inodes, find);
  }

  private Map<String, Long> storageTypeHistogramCpu(Collection<INode> inodes, String sum) {
    return versionLoader.storageTypeHistogramCpu(inodes, sum, this);
  }

  private Map<String, Long> storageTypeHistogramCpuWithFind(Collection<INode> inodes, String find) {
    return versionLoader.storageTypeHistogramCpuWithFind(inodes, find, this);
  }

  public Map<String, Long> accessTimeHistogram(Collection<INode> inodes, String sum, String find,
      String timeRange) {
    if (!isInit()) {
      return Collections.emptyMap();
    }
    if (find == null || find.length() == 0) {
      return accessTimeHistogramCpu(inodes, sum, timeRange);
    }
    return accessTimeHistogramCpuWithFind(inodes, find, timeRange);
  }

  private Map<String, Long> accessTimeHistogramCpu(Collection<INode> inodes, String sum,
      String timeRange) {
    return filteringHistogram(inodes, sum, getSumFunctionForINode(sum),
        node -> System.currentTimeMillis() - node.getAccessTime(),
        TimeHistogram.getBinsArray(timeRange), TimeHistogram.getKeys(timeRange));
  }

  private Map<String, Long> accessTimeHistogramCpuWithFind(Collection<INode> inodes, String find,
      String timeRange) {
    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];
    return filteringHistogramWithFind(inodes, findField, findOp,
        getFilterFunctionToLongForINode(findField),
        node -> System.currentTimeMillis() - node.getAccessTime(),
        TimeHistogram.getBinsArray(timeRange), TimeHistogram.getKeys(timeRange));
  }

  public Map<String, Long> modTimeHistogram(Collection<INode> inodes, String sum, String find,
      String timeRange) {
    if (!isInit()) {
      return Collections.emptyMap();
    }
    if (find == null || find.length() == 0) {
      return modTimeHistogramCpu(inodes, sum, timeRange);
    }
    return modTimeHistogramCpuWithFind(inodes, find, timeRange);
  }

  private Map<String, Long> modTimeHistogramCpu(Collection<INode> inodes, String sum,
      String timeRange) {
    return filteringHistogram(inodes, sum, getSumFunctionForINode(sum),
        node -> System.currentTimeMillis() - node.getModificationTime(),
        TimeHistogram.getBinsArray(timeRange), TimeHistogram.getKeys(timeRange));
  }

  private Map<String, Long> modTimeHistogramCpuWithFind(Collection<INode> inodes, String find,
      String timeRange) {
    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];
    return filteringHistogramWithFind(inodes, findField, findOp,
        getFilterFunctionToLongForINode(findField),
        node -> System.currentTimeMillis() - node.getModificationTime(),
        TimeHistogram.getBinsArray(timeRange), TimeHistogram.getKeys(timeRange));
  }

  public void dumpINodePaths(Collection<INode> inodes,
      Integer limit,
      HttpServletResponse resp)
      throws IOException {
    LOG.info("Dumping a list of " + inodes.size() + " INodes to a client.");
    long start = System.currentTimeMillis();
    PrintWriter writer = resp.getWriter();
    try {
      Collection<INode> subCollection;
      if (limit != null && limit < inodes.size()) {
        subCollection = inodes.stream().limit(limit).collect(Collectors.toList());
      } else {
        subCollection = inodes;
      }
      subCollection.stream().sorted(Comparator.comparing(INode::getFullPathName)).forEach(node -> {
        writer.write(node.getFullPathName() + '\n');
        writer.flush();
      });
    } finally {
      IOUtils.closeStream(writer);
      LOG.info("Closed response.");
    }
    long end = System.currentTimeMillis();
    LOG.info("Sending the entire response took " + (end - start) + "ms.");
  }

  public Map<String, Long> byUserHistogram(Collection<INode> inodes, String sum, String find) {
    if (!isInit()) {
      return Collections.emptyMap();
    }
    if (find == null || find.length() == 0) {
      return byUserHistogramCpu(inodes, sum);
    }
    return byUserHistogramCpuWithFind(inodes, find);
  }

  private Map<String, Long> byUserHistogramCpu(Collection<INode> inodes, String sum) {
    List<String> distinctUsers =
        StreamSupport.stream(inodes.spliterator(), true).map(INode::getUserName).distinct()
            .collect(Collectors.toList());
    Map<String, Long> userToIdMap =
        distinctUsers.parallelStream().mapToInt(distinctUsers::indexOf).boxed()
            .collect(Collectors.toMap(distinctUsers::get, value -> (long) value));

    return binMappingHistogram(inodes, sum, getSumFunctionForINode(sum),
        node -> userToIdMap.get(node.getUserName()), userToIdMap);
  }

  private Map<String, Long> byUserHistogramCpuWithFind(Collection<INode> inodes, String find) {
    List<String> distinctUsers =
        StreamSupport.stream(inodes.spliterator(), true).map(INode::getUserName).distinct()
            .collect(Collectors.toList());
    Map<String, Long> userToIdMap =
        distinctUsers.parallelStream().mapToInt(distinctUsers::indexOf).boxed()
            .collect(Collectors.toMap(distinctUsers::get, value -> (long) value));

    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];

    return binMappingHistogramWithFind(inodes, findOp, getFilterFunctionToLongForINode(findField),
        node -> userToIdMap.get(node.getUserName()), userToIdMap);
  }

  public Map<String, Long> byGroupHistogram(Collection<INode> inodes, String sum, String find) {
    if (!isInit()) {
      return Collections.emptyMap();
    }
    if (find == null || find.length() == 0) {
      return byGroupHistogramCpu(inodes, sum);
    }
    return byGroupHistogramCpuWithFind(inodes, find);
  }

  private Map<String, Long> byGroupHistogramCpu(Collection<INode> inodes, String sum) {
    List<String> distinctGroups =
        StreamSupport.stream(inodes.spliterator(), true).map(INode::getGroupName).distinct()
            .collect(Collectors.toList());
    Map<String, Long> groupToIdMap =
        distinctGroups.parallelStream().mapToInt(distinctGroups::indexOf).boxed()
            .collect(Collectors.toMap(distinctGroups::get, value -> (long) value));

    return binMappingHistogram(inodes, sum, getSumFunctionForINode(sum),
        node -> groupToIdMap.get(node.getGroupName()), groupToIdMap);
  }

  private Map<String, Long> byGroupHistogramCpuWithFind(Collection<INode> inodes, String find) {
    List<String> distinctGroups =
        StreamSupport.stream(inodes.spliterator(), true).map(INode::getGroupName).distinct()
            .collect(Collectors.toList());
    Map<String, Long> groupToIdMap =
        distinctGroups.parallelStream().mapToInt(distinctGroups::indexOf).boxed()
            .collect(Collectors.toMap(distinctGroups::get, value -> (long) value));

    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];

    return binMappingHistogramWithFind(inodes, findOp, getFilterFunctionToLongForINode(findField),
        node -> groupToIdMap.get(node.getGroupName()), groupToIdMap);
  }

  public Map<String, Long> parentDirHistogram(Collection<INode> inodes, Integer parentDirDepth,
      String sum, String find) {
    if (!isInit()) {
      return Collections.emptyMap();
    }
    if (find == null || find.length() == 0) {
      return parentDirHistogramCpu(inodes, parentDirDepth, sum);
    }
    return parentDirHistogramCpuWithFind(inodes, parentDirDepth, find);
  }

  private Map<String, Long> parentDirHistogramCpu(Collection<INode> inodes, Integer parentDirDepth,
      String sum) {
    int dirDepth =
        (parentDirDepth == null || parentDirDepth <= 0) ? Integer.MAX_VALUE : parentDirDepth;
    List<String> distinctDirectories =
        StreamSupport.stream(inodes.spliterator(), true).map(node -> {
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
        }).distinct().collect(Collectors.toList());

    final AtomicLong id = new AtomicLong(0L);
    Map<String, Long> dirToIdMap =
        distinctDirectories.parallelStream()
            .collect(Collectors.toMap(dir -> dir, dir -> id.getAndIncrement()));
    if (!dirToIdMap.containsKey("NO_MAPPING")) {
      dirToIdMap.put("NO_MAPPING", 0L);
    }

    Map<String, Long> result = binMappingHistogram(inodes, sum, getSumFunctionForINode(sum),
        node -> {
          try {
            INodeDirectory parent = node.getParent();
            int topParentDepth = new Path(parent.getFullPathName()).depth();
            if (topParentDepth < dirDepth) {
              return dirToIdMap.get("NO_MAPPING");
            }
            for (int parentTravs = topParentDepth; parentTravs > dirDepth; parentTravs--) {
              parent = parent.getParent();
            }
            return dirToIdMap.get(parent.getFullPathName());
          } catch (Exception e) {
            return dirToIdMap.get("NO_MAPPING");
          }
        }, dirToIdMap);
    result.remove("NO_MAPPING");
    return result;
  }

  private Map<String, Long> parentDirHistogramCpuWithFind(Collection<INode> inodes,
      Integer parentDirDepth, String find) {
    int dirDepth = (parentDirDepth != null) ? parentDirDepth : 0;
    List<String> distinctDirectories =
        StreamSupport.stream(inodes.spliterator(), true).map(node -> {
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
        }).distinct().collect(Collectors.toList());

    final AtomicLong id = new AtomicLong(0L);
    Map<String, Long> dirToIdMap =
        distinctDirectories.parallelStream()
            .collect(Collectors.toMap(dir -> dir, dir -> id.getAndIncrement()));
    String[] finds = find.split(":");
    String findOp = finds[0];
    String findField = finds[1];

    Map<String, Long> result = binMappingHistogramWithFind(inodes, findOp,
        getFilterFunctionToLongForINode(findField), node -> {
          try {
            INodeDirectory parent = node.getParent();
            int topParentDepth = new Path(parent.getFullPathName()).depth();
            if (topParentDepth < dirDepth) {
              return dirToIdMap.get("NO_MAPPING");
            }
            for (int parentTravs = topParentDepth; parentTravs > dirDepth; parentTravs--) {
              parent = parent.getParent();
            }
            return dirToIdMap.get(parent.getFullPathName());
          } catch (Exception e) {
            return dirToIdMap.get("NO_MAPPING");
          }
        }, dirToIdMap);
    result.remove("NO_MAPPING");
    return result;
  }

  public Map<String, Long> fileTypeHistogram(Collection<INode> inodes, String sum, String find) {
    if (!isInit()) {
      return Collections.emptyMap();
    }
    if (find == null || find.length() == 0) {
      return fileTypeHistogramCpu(inodes, sum);
    }
    return fileTypeHistogramCpu(inodes, sum);
  }

  private Map<String, Long> fileTypeHistogramCpu(Collection<INode> inodes, String sum) {
    List<String> fileTypes = FileTypeHistogram.keys;

    Map<String, Long> typeToIdMap =
        fileTypes.parallelStream().mapToInt(fileTypes::indexOf).boxed()
            .collect(Collectors.toMap(fileTypes::get, value -> (long) value));

    Map<String, Long> histogram =
        binMappingHistogram(inodes, sum, getSumFunctionForINode(sum),
            node -> typeToIdMap.get(FileTypeHistogram.determineType(node.getLocalName())),
            typeToIdMap);

    return removeKeysOnConditional(histogram, "gt:0");
  }

  public Map<String, Long> dirQuotaHistogram(Collection<INode> inodes, String sum) {
    if (!isInit()) {
      return Collections.emptyMap();
    }
    return dirQuotaHistogramCpu(inodes, sum);
  }

  private Map<String, Long> dirQuotaHistogramCpu(Collection<INode> inodes, String sum) {
    List<String> distinctDirectories =
        inodes.parallelStream().map(INode::getFullPathName).distinct().collect(Collectors.toList());

    final AtomicLong id = new AtomicLong(0L);
    Map<String, Long> dirToIdMap =
        distinctDirectories.parallelStream()
            .collect(Collectors.toMap(dir -> dir, dir -> id.getAndIncrement()));
    
    Map<String, Long> histogram =
        binMappingHistogram(inodes, sum, getSumFunctionForINode(sum),
            node -> dirToIdMap.get(node.getFullPathName()), dirToIdMap);

    return removeKeysOnConditional(histogram, "gte:0");
  }

  public void saveNamespace() throws IOException {
    if (!isInit()) {
      throw new IllegalStateException("Namesystem is not initalized. Cannot saveNamespace.");
    }
    if (namesystem != null) {
      versionLoader.saveNamespace();
    } else {
      throw new IOException("Namesystem does not exist.");
    }
  }

  public void saveLegacyNamespace(String dir) throws IOException {
    if (!isInit()) {
      throw new IllegalStateException("Namesystem is not initalized. Cannot saveNamespace.");
    }
    if (namesystem != null) {
      versionLoader.saveLegacyOIVImage(dir);
    } else {
      throw new IOException("Namesystem does not exist.");
    }
  }

  @SuppressWarnings("unchecked") /* We do unchecked casting to extract GSets */
  public void load(GSet<INode, INodeWithAdditionalFields> preloadedInodes)
      throws InterruptedException, NoSuchFieldException, IllegalAccessException {
    /*
     * Configuration standard is: /etc/hadoop/conf.
     * Goal is to let configuration tell us where the FsImage and EditLogs are for loading.
     */

    conf = new Configuration();
    conf.addResource("hdfs-default.xml");
    conf.addResource("hdfs-site.xml");
    long start = System.currentTimeMillis();

    GSetParallelWrapper<INode, INodeWithAdditionalFields> gsetMap;
    if (preloadedInodes == null) {
      LOG.info("Setting: " + DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY + " to: " + false);
      conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, false);

      LOG.info("Setting: " + DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY + " to: " + (-1));
      conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, -1);

      LOG.info("Setting: " + DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_KEY + " to: " + false);
      conf.setBoolean(DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_KEY, false);

      LOG.info("Setting: " + DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY
          + " to: /usr/local/nn-analytics/dfs/name");
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, "/usr/local/nn-analytics/dfs/name");

      String nameserviceId = DFSUtil.getOnlyNameServiceIdOrNull(conf);
      nameserviceId =
          (nameserviceId == null) ? conf.get(DFSConfigKeys.DFS_NAMESERVICE_ID) : nameserviceId;
      if (nameserviceId == null || nameserviceId.isEmpty()) {
        /* Hack for 2.4.0 support. attempt to override with internal nameservices. */
        nameserviceId = conf.get("dfs.internal.nameservices");

        LOG.info("Setting: " + DFSConfigKeys.DFS_NAMESERVICE_ID + " to: " + nameserviceId);
        conf.set(DFSConfigKeys.DFS_NAMESERVICE_ID, nameserviceId);
      }

      UserGroupInformation.setConfiguration(conf);
      reloadKeytab();

      LOG.info("Loading with configuration: " + conf.toString());
      LOG.info(
          "FileSystem seen as: " + conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
      LOG.info("Loading image from: " + conf.get(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY));
      long start1 = System.currentTimeMillis();
      try {
        namesystem = FSNamesystem.loadFromDisk(conf);
        namesystem.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
      } catch (IOException e) {
        LOG.info("Failed to load namesystem: " + e);
        return;
      }
      long end1 = System.currentTimeMillis();
      LOG.info("FSImage loaded in: " + (end1 - start1) + " ms.");
      LOG.info("Loaded in " + namesystem.getFilesTotal() + " INodes.");

      namesystem.writeLock();
      tokenExtractor = new TokenExtractor(namesystem.dtSecretManager, namesystem);
      FSDirectory fsDirectory = namesystem.getFSDirectory();
      INodeMap iNodeMap = fsDirectory.getINodeMap();
      Field mapField = iNodeMap.getClass().getDeclaredField("map");
      mapField.setAccessible(true);
      gsetMap = new GSetParallelWrapper(
          (GSet<INode, INodeWithAdditionalFields>) mapField.get(iNodeMap));
    } else {
      gsetMap = new GSetParallelWrapper(preloadedInodes);
      tokenExtractor = new TokenExtractor(null, null);
    }

    long s1 = System.currentTimeMillis();
    all = new GSetCollectionWrapper(gsetMap);
    files = StreamSupport.stream(gsetMap.spliterator(), true).filter(INode::isFile)
        .collect(Collectors.toConcurrentMap(node -> node, node -> node));
    dirs = StreamSupport.stream(gsetMap.spliterator(), true).filter(INode::isDirectory)
        .collect(Collectors.toConcurrentMap(node -> node, node -> node));
    long e1 = System.currentTimeMillis();
    LOG.info("Filtering " + files.size() + " files and " + dirs.size() + " dirs took: " + (e1 - s1)
        + " ms.");

    if (preloadedInodes == null) {
      // Start tailing and updating security credentials threads.
      try {
        FSDirectory fsDirectory = namesystem.getFSDirectory();
        INodeMap iNodeMap = fsDirectory.getINodeMap();
        Field mapField = iNodeMap.getClass().getDeclaredField("map");
        mapField.setAccessible(true);
        GSet<INode, INodeWithAdditionalFields> newGSet = new GSetSeperatorWrapper(gsetMap, files,
            dirs);
        mapField.set(iNodeMap, newGSet);
        namesystem.writeUnlock();

        namesystem.startStandbyServices(conf);
        versionLoader.setNamesystem(namesystem);
      } catch (Throwable e) {
        LOG.info("ERROR: Failed to start EditLogTailer: " + e);
      }
    }

    long end = System.currentTimeMillis();
    LOG.info("NNLoader bootstrap'd in: " + (end - start) + " ms.");
    inited.set(true);
  }

  private void writeNumberFieldIfDefined(JsonGenerator json, String key, Long value)
      throws IOException {
    if (value != Long.MIN_VALUE) {
      json.writeNumberField(key, value);
    }
  }

  private void writeStringFieldIfNotNull(JsonGenerator json, String key, String value)
      throws IOException {
    if (value != null) {
      json.writeStringField(key, value);
    }
  }

  public FileSystem getFileSystem() throws IOException {
    return FileSystem.get(conf);
  }

  Configuration getConfiguration() {
    return conf;
  }

  private void reloadKeytab() {
    if (UserGroupInformation.isSecurityEnabled()) {
      try {
        SecurityUtil.login(conf, DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY,
            DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY,
            InetAddress.getLocalHost().getCanonicalHostName());
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void clear() {
    if (namesystem != null) {
      try {
        namesystem.shutdown();
      } finally {
        namesystem = null;
      }
    }
    if (all != null) {
      all.clear();
    }
    if (files != null) {
      files.clear();
    }
    if (dirs != null) {
      dirs.clear();
    }
    inited.set(false);
  }

  /**
   * Result is a histogram with only entries that satisfy the conditional String. Conditional String
   * ex: 'gte:1000' should create a histogram where all entries have values greater than or equal to
   * 1000L.
   *
   * NOTE: Modifies the parameter histogram.
   */
  public Map<String, Long> removeKeysOnConditional(Map<String, Long> histogram,
      String histogramConditionsStr) {
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
    LOG.info("Removing " + keys.size() + " keys from histogram of size " + originalHistSize +
        " using conditional String: '" + histogramConditionsStr + "', took: " + (e1 - s1) + " ms.");

    return histogram;
  }

  /**
   * Result is a histogram with only entries that satisfy the conditional String. Conditional String
   * ex: 'gte:1000' should create a histogram where all entries have values greater than or equal to
   * 1000L.
   *
   * NOTE: Modifies the parameter histogram.
   */
  public Map<String, List<Long>> removeKeysOnConditional2(Map<String, List<Long>> histogram,
      String histogramConditionsStr) {
    long s1 = System.currentTimeMillis();
    int originalHistSize = histogram.size();

    List<Function<List<Long>, Boolean>> comparisons = createIndexedComparisons(
        histogramConditionsStr);
    Set<String> keys = new HashSet<>();
    for (Map.Entry<String, List<Long>> entry : histogram.entrySet()) {
      boolean columnCheck = check(comparisons, entry.getValue());
      if (!columnCheck) {
        keys.add(entry.getKey());
      }
    }
    keys.forEach(histogram::remove);

    long e1 = System.currentTimeMillis();
    LOG.info("Removing " + keys.size() + " keys from histogram2 of size " + originalHistSize +
        " using conditional String: '" + histogramConditionsStr + "', took: " + (e1 - s1) + " ms.");

    return histogram;
  }

  public boolean check(List<Function<Long, Boolean>> comparisons,
      long value) {
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

  public boolean check(List<Function<List<Long>, Boolean>> comparisons,
      List<Long> value) {
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

    //Create comparisons.
    for (String[] condition : conditionTuplets) {
      Function<Long, Boolean> longFunction = getFilterFunctionForLong(Long.parseLong(condition[2]),
          condition[1]);
      Function<List<Long>, Boolean> indexedLongFunction = (list) -> longFunction
          .apply(list.get(Integer.parseInt(condition[0])));
      comparisons.add(indexedLongFunction);
    }

    return comparisons;
  }

  public List<Function<Long, Boolean>> createComparisons(String conditionsStr) {
    String[] conditionsArray = conditionsStr.split(";");
    String[][] conditionTuplets = new String[conditionsArray.length][2];

    List<Function<Long, Boolean>> comparisons = new ArrayList<>(conditionTuplets.length);
    for (int i = 0; i < conditionTuplets.length; i++) {
      String tuplet = conditionsArray[i];
      conditionTuplets[i] = tuplet.split(":");
    }

    //Create comparisons.
    for (String[] condition : conditionTuplets) {
      Function<Long, Boolean> longFunction = getFilterFunctionForLong(Long.parseLong(condition[1]),
          condition[0]);
      comparisons.add(longFunction);
    }

    return comparisons;
  }

  public void namesystemWriteLock(Boolean useLock) {
    if (useLock != null && useLock && namesystem != null) {
      namesystem.writeLock();
    }
  }

  public void namesystemWriteUnlock(Boolean useLock) {
    if (useLock != null && useLock && namesystem != null) {
      namesystem.writeUnlock();
    }
  }

  public String getSuggestionsAsJson(String user) {
    if (!isInit()) {
      return Histograms.toJson(Collections.emptyMap());
    }
    if (user == null || user.isEmpty()) {
      return Histograms.toJson(cachedValues);
    } else {
      Map<String, Long> userMap = new HashMap<>(cachedValues);
      userMap.put("diskspace",
          cachedMaps.getOrDefault("diskspaceUsers", Collections.emptyMap()).getOrDefault(user, 0L));
      userMap.put("diskspace24h",
          cachedMaps.getOrDefault("diskspace24hUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("numFiles",
          cachedMaps.getOrDefault("numFilesUsers", Collections.emptyMap()).getOrDefault(user, 0L));
      userMap.put("numFiles24h", cachedMaps.getOrDefault("numFiles24hUsers", Collections.emptyMap())
          .getOrDefault(user, 0L));
      userMap.put("numDirs",
          cachedMaps.getOrDefault("numDirsUsers", Collections.emptyMap()).getOrDefault(user, 0L));
      userMap.put("emptyFiles", cachedMaps.getOrDefault("emptyFilesUsers", Collections.emptyMap())
          .getOrDefault(user, 0L));
      userMap.put("emptyFiles24h",
          cachedMaps.getOrDefault("emptyFiles24hUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("emptyFiles1yr",
          cachedMaps.getOrDefault("emptyFiles1yrUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("emptyFilesMem",
          cachedMaps.getOrDefault("emptyFilesMemUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("emptyFiles24hMem",
          cachedMaps.getOrDefault("emptyFiles24hMemUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("emptyDirs",
          cachedMaps.getOrDefault("emptyDirsUsers", Collections.emptyMap()).getOrDefault(user, 0L));
      userMap.put("emptyDirs24h",
          cachedMaps.getOrDefault("emptyDirs24hUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("emptyDirs1yr",
          cachedMaps.getOrDefault("emptyDirs1yrUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("emptyDirsMem",
          cachedMaps.getOrDefault("emptyDirsMemUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("emptyDirs24hMem",
          cachedMaps.getOrDefault("emptyDirs24hMemUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("tinyFiles",
          cachedMaps.getOrDefault("tinyFilesUsers", Collections.emptyMap()).getOrDefault(user, 0L));
      userMap.put("tinyFiles24h",
          cachedMaps.getOrDefault("tinyFiles24hUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("tinyFiles1yr",
          cachedMaps.getOrDefault("tinyFiles1yrUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("tinyFilesMem",
          cachedMaps.getOrDefault("tinyFilesMemUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("tinyFiles24hMem",
          cachedMaps.getOrDefault("tinyFiles24hMemUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("tinyFilesDs", cachedMaps.getOrDefault("tinyFilesDsUsers", Collections.emptyMap())
          .getOrDefault(user, 0L));
      userMap.put("tinyFiles24hDs",
          cachedMaps.getOrDefault("tinyFiles24hDsUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("smallFiles", cachedMaps.getOrDefault("smallFilesUsers", Collections.emptyMap())
          .getOrDefault(user, 0L));
      userMap.put("smallFiles24h",
          cachedMaps.getOrDefault("smallFiles24hUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("smallFiles1yr",
          cachedMaps.getOrDefault("smallFiles1yrUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("smallFilesMem",
          cachedMaps.getOrDefault("smallFilesMemUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("smallFiles24hMem",
          cachedMaps.getOrDefault("smallFiles24hMemUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("smallFilesDs",
          cachedMaps.getOrDefault("smallFilesDsUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("smallFiles24hDs",
          cachedMaps.getOrDefault("smallFiles24hDsUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("mediumFiles", cachedMaps.getOrDefault("mediumFilesUsers", Collections.emptyMap())
          .getOrDefault(user, 0L));
      userMap.put("largeFiles", cachedMaps.getOrDefault("largeFilesUsers", Collections.emptyMap())
          .getOrDefault(user, 0L));
      userMap.put("lastLogin", cachedLogins.getOrDefault(user, 0L));
      userMap.put("oldFiles1yr", cachedMaps.getOrDefault("oldFiles1yrUsers", Collections.emptyMap())
          .getOrDefault(user, 0L));
      userMap.put("oldFiles1yrDs",
          cachedMaps.getOrDefault("oldFiles1yrDsUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("oldFiles2yr", cachedMaps.getOrDefault("oldFiles2yrUsers", Collections.emptyMap())
          .getOrDefault(user, 0L));
      userMap.put("oldFiles2yrDs",
          cachedMaps.getOrDefault("oldFiles2yrDsUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("nsQuotaCount",
          cachedMaps.getOrDefault("nsQuotaCountsUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("dsQuotaCount",
          cachedMaps.getOrDefault("dsQuotaCountsUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("nsQuotaThreshCount",
          cachedMaps.getOrDefault("nsQuotaThreshCountsUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      userMap.put("dsQuotaThreshCount",
          cachedMaps.getOrDefault("dsQuotaThreshCountsUsers", Collections.emptyMap())
              .getOrDefault(user, 0L));
      return Histograms.toJson(userMap);
    }
  }

  public String getDirectoriesAsJson(String directory,
      String sum) {
    Map<String, Long> dirMap;
    switch (sum) {
      case "count":
        dirMap = cachedMaps.getOrDefault("dirCount", Collections.emptyMap());
        break;
      case "diskspaceConsumed":
        dirMap = cachedMaps.getOrDefault("dirDs", Collections.emptyMap());
        break;
      default:
        throw new IllegalArgumentException("Invalid sum type: " + sum);
    }
    if (directory != null && !directory.isEmpty()) {
      dirMap = Collections.singletonMap(directory, dirMap.get(directory));
    }
    return Histograms.toJson(dirMap);
  }

  public String getUsersAsJson(String suggestion) {
    if (!isInit()) {
      return Histograms.toJson(Collections.emptySet());
    }
    if (suggestion == null || suggestion.isEmpty()) {
      return Histograms.toJson(cachedUsers);
    } else {
      Map<String, Long> userSuggestions = cachedMaps.get(suggestion);
      if (userSuggestions == null) {
        throw new IllegalArgumentException(suggestion + " is not a valid suggestion query.");
      }
      return Histograms.toJson(userSuggestions);
    }
  }

  public String getIssuesAsJson(Integer limit,
                                boolean ascending) {
    if (!isInit()) {
      return Histograms.toJson(Collections.emptySet());
    }
    Map<String, Map<String, Long>> issuesMap = new LinkedHashMap<>();
    Map<String, Long> topEmptyFileUsers =
        Histograms.sortByValue(cachedMaps.getOrDefault("emptyFilesUsers", Collections.emptyMap()),
            ascending);
    Map<String, Long> topEmptyDirUsers =
        Histograms.sortByValue(cachedMaps.getOrDefault("emptyDirsUsers", Collections.emptyMap()),
            ascending);
    Map<String, Long> topTinyFilesUsers =
        Histograms.sortByValue(cachedMaps.getOrDefault("tinyFilesUsers", Collections.emptyMap()),
            ascending);
    Map<String, Long> topSmallFilesUsers =
        Histograms.sortByValue(cachedMaps.getOrDefault("smallFilesUsers", Collections.emptyMap()),
            ascending);
    Map<String, Long> topEmptyFile24hUsers =
        Histograms
            .sortByValue(cachedMaps.getOrDefault("emptyFiles24hUsers", Collections.emptyMap()),
                ascending);
    Map<String, Long> topEmptyDir24hUsers =
        Histograms.sortByValue(cachedMaps.getOrDefault("emptyDirs24hUsers", Collections.emptyMap()),
            ascending);
    Map<String, Long> topTinyFiles24hUsers =
        Histograms.sortByValue(cachedMaps.getOrDefault("tinyFiles24hUsers", Collections.emptyMap()),
            ascending);
    Map<String, Long> topSmallFiles24hUsers =
        Histograms
            .sortByValue(cachedMaps.getOrDefault("smallFiles24hUsers", Collections.emptyMap()),
                ascending);
    Map<String, Long> topOldFiles1yrUsers =
        Histograms.sortByValue(cachedMaps.getOrDefault("oldFiles1yrUsers", Collections.emptyMap()),
            ascending);
    Map<String, Long> topDirCount =
        Histograms
            .sortByValue(cachedMaps.getOrDefault("dirCount", Collections.emptyMap()), ascending);
    Map<String, Long> topDirDiskspace =
        Histograms.sortByValue(cachedMaps.getOrDefault("dirDs", Collections.emptyMap()), ascending);
    Map<String, Long> topDirCount24h =
        Histograms
            .sortByValue(cachedMaps.getOrDefault("dirCount24h", Collections.emptyMap()), ascending);
    Map<String, Long> topDirDiskspace24h =
        Histograms
            .sortByValue(cachedMaps.getOrDefault("dirDs24h", Collections.emptyMap()), ascending);
    Function<Map<String, Long>, Map<String, Long>> sliceFunc = (histogramMap) -> (ascending ?
        Histograms.sliceToBottom(histogramMap, limit) : Histograms.sliceToTop(histogramMap, limit));
    issuesMap.put("emptyFiles", sliceFunc.apply(topEmptyFileUsers));
    issuesMap.put("emptyDirs", sliceFunc.apply(topEmptyDirUsers));
    issuesMap.put("tinyFiles", sliceFunc.apply(topTinyFilesUsers));
    issuesMap.put("smallFiles", sliceFunc.apply(topSmallFilesUsers));
    issuesMap.put("emptyFiles24h", sliceFunc.apply(topEmptyFile24hUsers));
    issuesMap.put("emptyDirs24h", sliceFunc.apply(topEmptyDir24hUsers));
    issuesMap.put("tinyFiles24h", sliceFunc.apply(topTinyFiles24hUsers));
    issuesMap.put("smallFiles24h", sliceFunc.apply(topSmallFiles24hUsers));
    issuesMap.put("oldFiles1yr", sliceFunc.apply(topOldFiles1yrUsers));
    issuesMap.put("dirCount", sliceFunc.apply(topDirCount));
    issuesMap.put("dirDiskspace", sliceFunc.apply(topDirDiskspace));
    issuesMap.put("dirCount24h", sliceFunc.apply(topDirCount24h));
    issuesMap.put("dirDiskspace24h", sliceFunc.apply(topDirDiskspace24h));
    return Histograms.toJson(issuesMap);
  }

  private void reloadSuggestions() {
    long s1 = System.currentTimeMillis();
    Collection<INode> files = getINodeSet("files");
    Collection<INode> dirs = getINodeSet("dirs");

    long numFiles = files.size();
    long numDirs = dirs.size();
    long capacity = 0L;

    try {
      FileSystem fs = getFileSystem();
      capacity = fs.getStatus().getCapacity();
    } catch (IOException e) {
      e.printStackTrace();
    }

    Map<String, Long> modTimeCount = modTimeHistogram(files, "count", null, "monthly");
    Map<String, Long> modTimeDiskspace = modTimeHistogram(files, "diskspaceConsumed", null,
        "monthly");

    Set<String> fileUsers =
        files.parallelStream().map(INode::getUserName).distinct().collect(Collectors.toSet());
    Set<String> dirUsers =
        dirs.parallelStream().map(INode::getUserName).distinct().collect(Collectors.toSet());
    Set<String> users = Sets.union(fileUsers, dirUsers);

    long diskspace = sum(files, "diskspaceConsumed");
    Collection<INode> files24h = combinedFilter(files, new String[]{"modTime"},
        new String[]{"hoursAgo:24"});
    long numFiles24h = files24h.size();
    long diskspace24h = sum(files24h, "diskspaceConsumed");
    Map<String, Long> numFiles24hUsers = byUserHistogramCpu(files24h, "count");
    Map<String, Long> diskspace24hUsers = byUserHistogramCpu(files24h, "diskspaceConsumed");
    Map<String, Long> diskspaceUsers = byUserHistogramCpu(files, "diskspaceConsumed");

    Collection<INode> oldFiles1yr = combinedFilter(files, new String[]{"accessTime"},
        new String[]{"olderThanYears:1"});
    Map<String, Long> oldFiles1yrCountUsers = byUserHistogramCpu(oldFiles1yr, "count");
    Map<String, Long> oldFiles1yrDsUsers = byUserHistogramCpu(oldFiles1yr, "diskspaceConsumed");
    Collection<INode> oldFiles2yr = combinedFilter(files, new String[]{"accessTime"},
        new String[]{"olderThanYears:2"});
    Map<String, Long> oldFiles2yrCountUsers = byUserHistogramCpu(oldFiles2yr, "count");
    Map<String, Long> oldFiles2yrDsUsers = byUserHistogramCpu(oldFiles2yr, "diskspaceConsumed");

    Collection<INode> emptyFiles = combinedFilter(files, new String[]{"fileSize"},
        new String[]{"eq:0"});
    Collection<INode> emptyDirs = combinedFilter(dirs, new String[]{"dirNumChildren"},
        new String[]{"eq:0"});
    Collection<INode> tinyFiles = combinedFilter(files, new String[]{"fileSize", "fileSize"},
        new String[]{"lte:1024", "gt:0"});
    Collection<INode> smallFiles = combinedFilter(files, new String[]{"fileSize", "fileSize"},
        new String[]{"lte:1048576", "gt:1024"});
    Collection<INode> mediumFiles = combinedFilter(files, new String[]{"fileSize", "fileSize"},
        new String[]{"lte:134217728", "gt:1048576"});

    Collection<INode> emptyFiles24h = combinedFilter(emptyFiles, new String[]{"modTime"},
        new String[]{"hoursAgo:24"});
    Collection<INode> emptyDirs24h = combinedFilter(emptyDirs, new String[]{"modTime"},
        new String[]{"hoursAgo:24"});
    Collection<INode> tinyFiles24h = combinedFilter(tinyFiles, new String[]{"modTime"},
        new String[]{"hoursAgo:24"});
    Collection<INode> smallFiles24h = combinedFilter(smallFiles, new String[]{"modTime"},
        new String[]{"hoursAgo:24"});

    Collection<INode> emptyFiles1yr = combinedFilter(emptyFiles, new String[]{"accessTime"},
        new String[]{"olderThanYears:1"});
    Collection<INode> emptyDirs1yr = combinedFilter(emptyDirs, new String[]{"modTime"},
        new String[]{"olderThanYears:1"});
    Collection<INode> tinyFiles1yr = combinedFilter(tinyFiles, new String[]{"accessTime"},
        new String[]{"olderThanYears:1"});
    Collection<INode> smallFiles1yr = combinedFilter(smallFiles, new String[]{"accessTime"},
        new String[]{"olderThanYears:1"});

    long emptyFilesCount = emptyFiles.size();
    long emptyDirsCount = emptyDirs.size();
    long emptyFilesMem = sum(emptyFiles, "memoryConsumed");
    long emptyDirsMem = sum(emptyDirs, "memoryConsumed");
    long tinyFilesCount = tinyFiles.size();
    long smallFilesCount = smallFiles.size();
    long mediumFilesCount = mediumFiles.size();
    long largeFilesCount =
        numFiles - emptyFilesCount - tinyFilesCount - smallFilesCount - mediumFilesCount;
    long tinyFilesMem = sum(tinyFiles, "memoryConsumed");
    long smallFilesMem = sum(smallFiles, "memoryConsumed");
    long tinyFilesDs = sum(tinyFiles, "diskspaceConsumed");
    long smallFilesDs = sum(smallFiles, "diskspaceConsumed");

    long emptyFiles24hCount = emptyFiles24h.size();
    long emptyDirs24hCount = emptyDirs24h.size();
    long emptyFiles24hMem = sum(emptyFiles24h, "memoryConsumed");
    long emptyDirs24hMem = sum(emptyDirs24h, "memoryConsumed");
    long tinyFiles24hCount = tinyFiles24h.size();
    long smallFiles24hCount = smallFiles24h.size();
    long tinyFiles24hMem = sum(tinyFiles24h, "memoryConsumed");
    long smallFiles24hMem = sum(smallFiles24h, "memoryConsumed");
    long tinyFiles24hDs = sum(tinyFiles24h, "diskspaceConsumed");
    long smallFiles24hDs = sum(smallFiles24h, "diskspaceConsumed");

    long emptyFiles1yrCount = emptyFiles1yr.size();
    long emptyDirs1yrCount = emptyDirs1yr.size();
    long tinyFiles1yrCount = tinyFiles1yr.size();
    long smallFiles1yrCount = smallFiles1yr.size();

    long oldFiles1yrCount = oldFiles1yr.size();
    long oldFiles2yrCount = oldFiles2yr.size();
    long oldFiles1yrDs = sum(oldFiles1yr, "diskspaceConsumed");
    long oldFiles2yrDs = sum(oldFiles2yr, "diskspaceConsumed");

    Map<String, Long> filesUsers = byUserHistogramCpu(files, "count");
    Map<String, Long> dirsUsers = byUserHistogramCpu(dirs, "count");

    Map<String, Long> emptyFilesUsers = byUserHistogramCpu(emptyFiles, "count");
    Map<String, Long> emptyDirsUsers = byUserHistogramCpu(emptyDirs, "count");
    Map<String, Long> tinyFilesUsers = byUserHistogramCpu(tinyFiles, "count");
    Map<String, Long> smallFilesUsers = byUserHistogramCpu(smallFiles, "count");
    Map<String, Long> mediumFilesUsers = byUserHistogramCpu(mediumFiles, "count");
    Map<String, Long> largeFilesUsers = new HashMap<>(users.size());
    users.forEach(u -> {
      long largeFiles = filesUsers.getOrDefault(u, 0L) - emptyFilesUsers.getOrDefault(u, 0L) -
          tinyFilesUsers.getOrDefault(u, 0L) - smallFilesUsers.getOrDefault(u, 0L) -
          mediumFilesUsers.getOrDefault(u, 0L);
      largeFilesUsers.put(u, largeFiles);
    });

    Map<String, Long> emptyFiles24hUsers = byUserHistogramCpu(emptyFiles24h, "count");
    Map<String, Long> emptyDirs24hUsers = byUserHistogramCpu(emptyDirs24h, "count");
    Map<String, Long> tinyFiles24hUsers = byUserHistogramCpu(tinyFiles24h, "count");
    Map<String, Long> smallFiles24hUsers = byUserHistogramCpu(smallFiles24h, "count");

    Map<String, Long> emptyFiles1yrUsers = byUserHistogramCpu(emptyFiles1yr, "count");
    Map<String, Long> emptyDirs1yrUsers = byUserHistogramCpu(emptyDirs1yr, "count");
    Map<String, Long> tinyFiles1yrUsers = byUserHistogramCpu(tinyFiles1yr, "count");
    Map<String, Long> smallFiles1yrUsers = byUserHistogramCpu(smallFiles1yr, "count");

    Map<String, Long> emptyFilesMemUsers = byUserHistogramCpu(emptyFiles, "memoryConsumed");
    Map<String, Long> emptyDirsMemUsers = byUserHistogramCpu(emptyDirs, "memoryConsumed");
    Map<String, Long> tinyFilesMemUsers = byUserHistogramCpu(tinyFiles, "memoryConsumed");
    Map<String, Long> smallFilesMemUsers = byUserHistogramCpu(smallFiles, "memoryConsumed");
    Map<String, Long> tinyFilesDsUsers = byUserHistogramCpu(tinyFiles, "diskspaceConsumed");
    Map<String, Long> smallFilesDsUsers = byUserHistogramCpu(smallFiles, "diskspaceConsumed");
    Map<String, Long> emptyFiles24hMemUsers = byUserHistogramCpu(emptyFiles24h, "memoryConsumed");
    Map<String, Long> emptyDirs24hMemUsers = byUserHistogramCpu(emptyDirs24h, "memoryConsumed");
    Map<String, Long> tinyFiles24hMemUsers = byUserHistogramCpu(tinyFiles24h, "memoryConsumed");
    Map<String, Long> smallFiles24hMemUsers = byUserHistogramCpu(smallFiles24h, "memoryConsumed");
    Map<String, Long> tinyFiles24hDsUsers = byUserHistogramCpu(tinyFiles24h, "diskspaceConsumed");
    Map<String, Long> smallFiles24hDsUsers = byUserHistogramCpu(smallFiles24h, "diskspaceConsumed");

    Map<String, Long> dirCount = parentDirHistogramCpu(files, 3, "count");
    dirCount = Histograms.sliceToTop(dirCount, 1000);
    Map<String, Long> dirDs = parentDirHistogramCpu(files, 3, "diskspaceConsumed");
    dirDs = Histograms.sliceToTop(dirDs, 1000);

    VirtualINodeTree tree = new VirtualINodeTree();
    cachedDirs.forEach(tree::addElement);
    List<String> commonRoots = tree.getCommonRootsAsStrings();

    for (String commonRoot : commonRoots) {
      Collection<INode> commonINodes =
          combinedFilter(files, new String[]{"path"}, new String[]{"startsWith:" + commonRoot});

      for (String cachedDir : cachedDirs) {
        if (!cachedDir.startsWith(commonRoot)) {
          continue;
        }
        Collection<INode> inodes;
        if (cachedDir.equals(commonRoot)) {
          inodes = commonINodes;
        } else {
          inodes = combinedFilter(commonINodes, new String[]{"path"},
              new String[]{"startsWith:" + cachedDir});
        }
        long count = inodes.size();
        long diskspaceConsumed = sum(inodes, "diskspaceConsumed");
        dirCount.put(cachedDir, count);
        dirDs.put(cachedDir, diskspaceConsumed);
      }
    }

    Map<String, Long> dirCount24h = parentDirHistogramCpu(files24h, 3, "count");
    dirCount24h = Histograms.sliceToTop(dirCount24h, 1000);
    Map<String, Long> dirDs24h = parentDirHistogramCpu(files24h, 3, "diskspaceConsumed");
    dirDs24h = Histograms.sliceToTop(dirDs24h, 1000);
    for (String dir : cachedDirs) {
      Collection<INode> inodes = combinedFilter(files24h, new String[]{"path"},
          new String[]{"startsWith:" + dir});
      long count = inodes.size();
      long diskspaceConsumed = sum(inodes, "diskspaceConsumed");
      dirCount24h.put(dir, count);
      dirDs24h.put(dir, diskspaceConsumed);
    }

    long nsQuotaCount = 0;
    long dsQuotaCount = 0;
    long nsQuotaThreshCount = 0;
    long dsQuotaThreshCount = 0;
    Map<String, Long> nsQuotaThreshCountsUsers = new HashMap<>();
    Map<String, Long> dsQuotaThreshCountsUsers = new HashMap<>();
    Map<String, Long> nsQuotaCountsUsers = new HashMap<>();
    Map<String, Long> dsQuotaCountsUsers = new HashMap<>();
    for (String user : users) {
      Collection<INode> quotaDirs = combinedFilter(dirs, new String[] {"user","hasQuota"},
          new String[] {"eq:" + user,"eq:true"});
      Map<String, Long> nsQuotaRatio = dirQuotaHistogramCpu(quotaDirs, "nsQuotaRatioUsed");
      Map<String, Long> dsQuotaRatio = dirQuotaHistogramCpu(quotaDirs, "dsQuotaRatioUsed");
      long nsThreshExceeded = nsQuotaRatio.values().parallelStream().filter(v -> v > 85L).count();
      long dsThreshExceeded = dsQuotaRatio.values().parallelStream().filter(v -> v > 85L).count();
      cachedUserNsQuotas.put(user, nsQuotaRatio);
      cachedUserDsQuotas.put(user, dsQuotaRatio);
      nsQuotaThreshCountsUsers.put(user, nsThreshExceeded);
      dsQuotaThreshCountsUsers.put(user, dsThreshExceeded);
      nsQuotaCount += nsQuotaRatio.size();
      dsQuotaCount += dsQuotaRatio.size();
      nsQuotaThreshCount += nsThreshExceeded;
      dsQuotaThreshCount += dsThreshExceeded;
      nsQuotaCountsUsers.put(user, (long) nsQuotaRatio.size());
      dsQuotaCountsUsers.put(user, (long) dsQuotaRatio.size());
    }

    long e1 = System.currentTimeMillis();
    long timeTaken = (e1 - s1);

    long s2 = System.currentTimeMillis();

    cachedLogins.putAll(tokenExtractor.getTokenLastLogins());
    cachedUsers.clear();
    cachedUsers.addAll(users);
    cachedValues.put("timeTaken", timeTaken);
    cachedValues.put("reportTime", e1);
    cachedValues.put("capacity", capacity);
    cachedValues.put("diskspace", diskspace);
    cachedValues.put("diskspace24h", diskspace24h);
    cachedValues.put("numFiles", numFiles);
    cachedValues.put("numFiles24h", numFiles24h);
    cachedValues.put("numDirs", numDirs);
    cachedValues.put("totalFiles", numFiles);
    cachedValues.put("totalDirs", numDirs);
    cachedValues.put("emptyFiles", emptyFilesCount);
    cachedValues.put("emptyDirs", emptyDirsCount);
    cachedValues.put("tinyFiles", tinyFilesCount);
    cachedValues.put("smallFiles", smallFilesCount);
    cachedValues.put("emptyFiles24h", emptyFiles24hCount);
    cachedValues.put("emptyDirs24h", emptyDirs24hCount);
    cachedValues.put("tinyFiles24h", tinyFiles24hCount);
    cachedValues.put("smallFiles24h", smallFiles24hCount);
    cachedValues.put("emptyFiles1yr", emptyFiles1yrCount);
    cachedValues.put("emptyDirs1yr", emptyDirs1yrCount);
    cachedValues.put("tinyFiles1yr", tinyFiles1yrCount);
    cachedValues.put("smallFiles1yr", smallFiles1yrCount);
    cachedValues.put("mediumFiles", mediumFilesCount);
    cachedValues.put("largeFiles", largeFilesCount);
    cachedValues.put("emptyFilesMem", emptyFilesMem);
    cachedValues.put("emptyDirsMem", emptyDirsMem);
    cachedValues.put("tinyFilesMem", tinyFilesMem);
    cachedValues.put("tinyFilesDs", tinyFilesDs);
    cachedValues.put("smallFilesMem", smallFilesMem);
    cachedValues.put("smallFilesDs", smallFilesDs);
    cachedValues.put("emptyFiles24hMem", emptyFiles24hMem);
    cachedValues.put("emptyDirs24hMem", emptyDirs24hMem);
    cachedValues.put("tinyFiles24hMem", tinyFiles24hMem);
    cachedValues.put("smallFiles24hMem", smallFiles24hMem);
    cachedValues.put("tinyFiles24hDs", tinyFiles24hDs);
    cachedValues.put("smallFiles24hDs", smallFiles24hDs);
    cachedValues.put("oldFiles1yr", oldFiles1yrCount);
    cachedValues.put("oldFiles1yrDs", oldFiles1yrDs);
    cachedValues.put("oldFiles2yr", oldFiles2yrCount);
    cachedValues.put("oldFiles2yrDs", oldFiles2yrDs);
    cachedValues.put("nsQuotaCount", nsQuotaCount);
    cachedValues.put("dsQuotaCount", dsQuotaCount);
    cachedValues.put("nsQuotaThreshCount", nsQuotaThreshCount);
    cachedValues.put("dsQuotaThreshCount", dsQuotaThreshCount);
    cachedMaps.put("diskspaceUsers", diskspaceUsers);
    cachedMaps.put("numFilesUsers", filesUsers);
    cachedMaps.put("numDirsUsers", dirsUsers);
    cachedMaps.put("emptyFilesUsers", emptyFilesUsers);
    cachedMaps.put("emptyDirsUsers", emptyDirsUsers);
    cachedMaps.put("emptyFilesMemUsers", emptyFilesMemUsers);
    cachedMaps.put("emptyDirsMemUsers", emptyDirsMemUsers);
    cachedMaps.put("tinyFilesUsers", tinyFilesUsers);
    cachedMaps.put("smallFilesUsers", smallFilesUsers);
    cachedMaps.put("tinyFilesMemUsers", tinyFilesMemUsers);
    cachedMaps.put("smallFilesMemUsers", smallFilesMemUsers);
    cachedMaps.put("tinyFilesDsUsers", tinyFilesDsUsers);
    cachedMaps.put("smallFilesDsUsers", smallFilesDsUsers);
    cachedMaps.put("diskspace24hUsers", diskspace24hUsers);
    cachedMaps.put("numFiles24hUsers", numFiles24hUsers);
    cachedMaps.put("emptyFiles24hUsers", emptyFiles24hUsers);
    cachedMaps.put("emptyDirs24hUsers", emptyDirs24hUsers);
    cachedMaps.put("emptyFiles24hMemUsers", emptyFiles24hMemUsers);
    cachedMaps.put("emptyDirs24hMemUsers", emptyDirs24hMemUsers);
    cachedMaps.put("tinyFiles24hUsers", tinyFiles24hUsers);
    cachedMaps.put("smallFiles24hUsers", smallFiles24hUsers);
    cachedMaps.put("tinyFiles24hMemUsers", tinyFiles24hMemUsers);
    cachedMaps.put("smallFiles24hMemUsers", smallFiles24hMemUsers);
    cachedMaps.put("tinyFiles24hDsUsers", tinyFiles24hDsUsers);
    cachedMaps.put("smallFiles24hDsUsers", smallFiles24hDsUsers);
    cachedMaps.put("emptyFiles1yrUsers", emptyFiles1yrUsers);
    cachedMaps.put("emptyDirs1yrUsers", emptyDirs1yrUsers);
    cachedMaps.put("tinyFiles1yrUsers", tinyFiles1yrUsers);
    cachedMaps.put("smallFiles1yrUsers", smallFiles1yrUsers);
    cachedMaps.put("mediumFilesUsers", mediumFilesUsers);
    cachedMaps.put("largeFilesUsers", largeFilesUsers);
    cachedMaps.put("oldFiles1yrUsers", oldFiles1yrCountUsers);
    cachedMaps.put("oldFiles1yrDsUsers", oldFiles1yrDsUsers);
    cachedMaps.put("oldFiles2yrUsers", oldFiles2yrCountUsers);
    cachedMaps.put("oldFiles2yrDsUsers", oldFiles2yrDsUsers);
    cachedMaps.put("dirCount", dirCount);
    cachedMaps.put("dirDs", dirDs);
    cachedMaps.put("dirCount24h", dirCount24h);
    cachedMaps.put("dirDs24h", dirDs24h);
    cachedMaps.put("modTimeCount", modTimeCount);
    cachedMaps.put("modTimeDiskspace", modTimeDiskspace);
    cachedMaps.put("nsQuotaCountsUsers", nsQuotaCountsUsers);
    cachedMaps.put("dsQuotaCountsUsers", dsQuotaCountsUsers);
    cachedMaps.put("nsQuotaThreshCountsUsers", nsQuotaThreshCountsUsers);
    cachedMaps.put("dsQuotaThreshCountsUsers", dsQuotaThreshCountsUsers);

    long e2 = System.currentTimeMillis();
    LOG.info("Sync-switch of suggestions took: " + (e2 - s2) + " ms.");
    LOG.info("Reloading suggestions matrices took: " + timeTaken + " ms.");
    suggestive.set(true);

    if (hsqlDriver != null && isInit() && isHistorical()) {
      long s3 = System.currentTimeMillis();
      try {
        hsqlDriver.logHistoryPerUser(cachedValues, cachedMaps, cachedUsers);
      } catch (SQLException e) {
        LOG.info("Failed to write historical data due to: " + e);
      }
      long e3 = System.currentTimeMillis();
      LOG.info("Writing to embedded SQL DB took: " + (e3 - s3) + " ms.");
    } else {
      LOG.info("No historical data written as it is disabled.");
    }

    long s4 = System.currentTimeMillis();
    try {
      cache.commit();
    } catch (Exception e) {
      LOG.info("Failed to write cache data due to: " + e);
    }
    long e4 = System.currentTimeMillis();
    LOG.info("Writing to embedded MapDB took: " + (e4 - s4) + " ms.");
  }

  public void initReloadThreads(ExecutorService internalService) {
    Future<Void> reload = internalService.submit(() -> {
      while (true) {
        try {
          reloadSuggestions();
        } catch (Throwable e) {
          LOG.info("Suggestion reload failed: " + e);
          for (StackTraceElement element : e.getStackTrace()) {
            LOG.info(element.toString());
          }
        }
        // Reload suggestions every 15 minutes.
        try {
          Thread.sleep(15 * 60 * 1000L);
        } catch (InterruptedException ignored) {
        }
      }
    });
    Future<Void> keytab = internalService.submit(() -> {
      while (true) {
        // Reload Keytab every 10 minutes.
        try {
          Thread.sleep(10 * 60 * 1000L);
        } catch (InterruptedException ignored) {
        }
        reloadKeytab();
      }
    });
    if (reload.isDone()) {
      LOG.error("Suggestion reload service exited; suggestions will not update.");
    }
    if (keytab.isDone()) {
      LOG.error("Keytab reload service exited; keytab will expire.");
    }
  }

  public void initHistoryRecorder(HSQLDriver hsqlDriver,
      SecurityConfiguration conf,
      boolean isEnabled) throws SQLException {
    if (isEnabled && hsqlDriver != null) {
      this.hsqlDriver = hsqlDriver;
      hsqlDriver.startDatabase(conf);
      hsqlDriver.createTable();
      hsqlDriver.checkLoginTable(cachedLogins);
      historical.set(true);
    }
  }

  public String getTokens() {
    return Histograms.toJson(Histograms.sortByValue(cachedLogins, true));
  }

  public void addDirectoryToAnalysis(String directory) throws IOException {
    if (directory == null || directory.isEmpty()) {
      throw new IllegalArgumentException("Directory parameter 'dir' not defined.");
    }
    if (directory.endsWith("/")) {
      directory = directory.substring(0, directory.length() - 1);
    }
    boolean existed = cachedDirs.add(directory);
    if (existed) {
      throw new IOException(directory + " already set for analysis.");
    }
  }

  public void removeDirectoryFromAnalysis(String directory) throws IOException {
    if (directory == null || directory.isEmpty()) {
      throw new IllegalArgumentException("Directory parameter 'dir' not defined.");
    }
    if (directory.endsWith("/")) {
      directory = directory.substring(0, directory.length() - 1);
    }
    boolean removed = cachedDirs.remove(directory);
    if (!removed) {
      throw new IOException(directory + " was not scheduled for analysis.");
    }
  }

  public Set<String> getDirectoriesForAnalysis() {
    return cachedDirs;
  }

  public String getQuotaAsJson(String user, String sum) {
    if (sum == null || sum.length() == 0) {
      throw new IllegalArgumentException(
          "Please define a sum of either diskspaceConsumed or count for Quotas.");
    }
    if (user != null && user.length() > 0) {
      switch (sum) {
        case "dsQuotaRatioUsed":
          return Histograms.toJson(Histograms.sortByValue(cachedUserDsQuotas.get(user), false));
        case "nsQuotaRatioUsed":
          return Histograms.toJson(Histograms.sortByValue(cachedUserNsQuotas.get(user), false));
        default:
          throw new IllegalArgumentException(
              "Please choose between diskspaceConsumed or count for Quotas.");
      }
    } else {
      switch (sum) {
        case "dsQuotaRatioUsed":
          return Histograms.toJson(cachedUserDsQuotas);
        case "nsQuotaRatioUsed":
          return Histograms.toJson(cachedUserNsQuotas);
        default:
          throw new IllegalArgumentException(
              "Please choose between diskspaceConsumed or count for Quotas.");
      }
    }
  }

  public String getFileAgeAsJson(String sum) {
    if (sum == null || sum.length() == 0) {
      throw new IllegalArgumentException(
          "Please define a sum of either diskspaceConsumed or count for File ages.");
    }
    switch (sum) {
      case "diskspaceConsumed":
        return Histograms
            .toJson(cachedMaps.getOrDefault("modTimeDiskspace", Collections.emptyMap()));
      case "count":
        return Histograms.toJson(cachedMaps.getOrDefault("modTimeCount", Collections.emptyMap()));
      default:
        throw new IllegalArgumentException(
            "Please choose between diskspaceConsumed or count for File ages.");
    }
  }
}
