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
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.analytics.ApplicationConfiguration;
import org.apache.hadoop.hdfs.server.namenode.analytics.HsqlDriver;
import org.apache.hadoop.hdfs.server.namenode.analytics.WebServerMain;
import org.apache.hadoop.hdfs.server.namenode.analytics.security.KeytabReloader;
import org.apache.hadoop.hdfs.server.namenode.cache.SuggestionsEngine;
import org.apache.hadoop.hdfs.server.namenode.cache.SuggestionsReloader;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressView;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.TokenExtractor;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.ReflectionUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NameNodeLoader {

  public static final Logger LOG = LoggerFactory.getLogger(NameNodeLoader.class.getName());

  private final VersionInterface versionLoader;
  private final SuggestionsEngine suggestionsEngine;

  private QueryEngine queryEngine = null;
  private AtomicBoolean inited = new AtomicBoolean(false);
  private AtomicBoolean historical = new AtomicBoolean(false);
  private Configuration conf = null;
  private FSNamesystem namesystem = null;
  private HsqlDriver hsqlDriver = null;
  private TokenExtractor tokenExtractor = null;
  private KeytabReloader keytabReloader = null;
  private SuggestionsReloader suggestionsReloader = null;

  /** Constructor. */
  public NameNodeLoader() {
    versionLoader = new VersionContext();
    suggestionsEngine = new SuggestionsEngine();
  }

  public TokenExtractor getTokenExtractor() {
    return tokenExtractor;
  }

  public HsqlDriver getEmbeddedHistoryDatabaseDriver() {
    return hsqlDriver;
  }

  public SuggestionsEngine getSuggestionsEngine() {
    return suggestionsEngine;
  }

  public QueryEngine getQueryEngine() {
    return queryEngine;
  }

  public boolean isInit() {
    return inited.get();
  }

  public boolean isHistorical() {
    return historical.get();
  }

  /**
   * Gets the last applied txid on NNA.
   *
   * @return long representing last applied txid
   */
  public long getCurrentTxId() {
    if (namesystem == null) {
      return -1L;
    }
    return namesystem.getFSImage().lastAppliedTxId;
  }

  /**
   * Get the authority of the FileSystem URI.
   *
   * @return string representing the filesystem authority
   */
  public String getAuthority() {
    if (conf == null) {
      return "test";
    }
    String authority =
        new Path(conf.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY)).toUri().getAuthority();
    if (authority == null) {
      return "test";
    }
    return authority;
  }

  /**
   * Sends the loading status as JSON to the parameter HTTP response. Copied from NameNode.
   *
   * @param resp the HTTP response
   * @throws IOException error in fetching loading status
   */
  public void sendLoadingStatus(HttpServletResponse resp) throws IOException {
    String count = "count";
    String elapsedTime = "elapsedTime";
    String file = "file";
    String name = "name";
    String desc = "desc";
    String percentComplete = "percentComplete";
    String phases = "phases";
    String size = "size";
    String status = "status";
    String steps = "steps";
    String total = "total";

    StartupProgressView view = NameNode.getStartupProgress().createView();
    JsonGenerator json =
        new JsonFactory().createJsonGenerator(resp.getWriter()).useDefaultPrettyPrinter();

    try {
      json.writeStartObject();
      json.writeNumberField(elapsedTime, view.getElapsedTime());
      json.writeNumberField(percentComplete, view.getPercentComplete());
      json.writeArrayFieldStart(phases);

      for (Phase phase : view.getPhases()) {
        json.writeStartObject();
        json.writeStringField(name, phase.getName());
        json.writeStringField(desc, phase.getDescription());
        json.writeStringField(status, view.getStatus(phase).toString());
        json.writeNumberField(percentComplete, view.getPercentComplete(phase));
        json.writeNumberField(elapsedTime, view.getElapsedTime(phase));
        writeStringFieldIfNotNull(json, file, view.getFile(phase));
        writeNumberFieldIfDefined(json, size, view.getSize(phase));
        json.writeArrayFieldStart(steps);

        for (Step step : view.getSteps(phase)) {
          json.writeStartObject();
          StepType stepType = step.getType();
          if (stepType != null) {
            json.writeStringField(name, stepType.getName());
            json.writeStringField(desc, stepType.getDescription());
          }
          json.writeNumberField(count, view.getCount(phase, step));
          writeStringFieldIfNotNull(json, file, step.getFile());
          writeNumberFieldIfDefined(json, size, step.getSize());
          json.writeNumberField(total, view.getTotal(phase, step));
          json.writeNumberField(percentComplete, view.getPercentComplete(phase, step));
          json.writeNumberField(elapsedTime, view.getElapsedTime(phase, step));
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

  /**
   * Delegates to {@link VersionContext} for dumping INode information.
   *
   * @param path the inode path to dump info on
   * @param resp the HTTP response
   * @throws IOException error in dumping inode info
   */
  public void dumpINodeInDetail(String path, HttpServletResponse resp) throws IOException {
    if (path == null || path.isEmpty() || !path.startsWith("/")) {
      throw new IllegalArgumentException(
          "Please specify non-empty and absolute `path` parameter for /dump.");
    }
    versionLoader.dumpINodeInDetail(path, resp);
  }

  /**
   * Prints out the HDFS configuration to the parameter HTTP response.
   *
   * @param resp the HTTP response
   * @throws IOException error in dumping hdfs configs
   */
  public void dumpConfig(HttpServletResponse resp) throws IOException {
    PrintWriter writer = resp.getWriter();
    try {
      conf.writeXml(writer);
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  public String getConfigValue(String key) {
    return conf.get(key);
  }

  /**
   * Reads NNA log information and forwards to parameter HTTP response.
   *
   * @param charsLimitVar limit of number of characters to read
   * @param resp the HTTP response
   * @throws IOException error in reading log file
   */
  public void dumpLog(Integer charsLimitVar, HttpServletResponse resp) throws IOException {
    int charLimit = (charsLimitVar != null) ? charsLimitVar : 4000;
    LOG.info("Dumping last {} chars of logging to a client.", charLimit);
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
    LOG.info("Dumping the log response took {} ms.", (end - start));
  }

  /**
   * Saves the current in-memory file system to a binary file snapshot locally. Does not communicate
   * with active HDFS cluster.
   *
   * @throws IOException error in saving namespace
   */
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

  /**
   * Saves the current in-memory file system to a binary file snapshot locally as a legacy image.
   * Does not communicate with active HDFS cluster.
   *
   * @param dir custom directory to write legacy image to
   * @throws IOException error in saving legacy namespace
   */
  public void saveLegacyNamespace(String dir) throws IOException {
    if (!isInit()) {
      throw new IllegalStateException("Namesystem is not initalized. Cannot saveNamespace.");
    }
    if (namesystem != null) {
      versionLoader.saveLegacyOivImage(dir);
    } else {
      throw new IOException("Namesystem does not exist.");
    }
  }

  /**
   * Loads the INodes into NNA from parameter or from local FsImage.
   *
   * @param preloadedInodes set of pre-generated inodes; if null loads from fsimage
   * @param preloadedHadoopConf pre-loaded hdfs configuration; if null will get from classloader
   * @param nnaConf the NNA application configuration
   * @throws IOException error in loading inodes
   * @throws NoSuchFieldException error in fetching inodes from fsimage
   * @throws IllegalAccessException error in fetching inodes from fsimage
   */
  @SuppressWarnings("unchecked") /* We do unchecked casting to extract GSets */
  public void load(
      GSet<INode, INodeWithAdditionalFields> preloadedInodes,
      Configuration preloadedHadoopConf,
      ApplicationConfiguration nnaConf)
      throws Exception {
    /*
     * Configuration standard is: /etc/hadoop/conf.
     * Goal is to let configuration tell us where the FsImage and EditLogs are for loading.
     */

    queryEngine = initializeQueryEngine(nnaConf);
    suggestionsEngine.start(nnaConf);
    if (conf == null) {
      if (preloadedHadoopConf != null) {
        conf = preloadedHadoopConf;
      } else {
        conf = new Configuration();
        conf.addResource("hdfs-default.xml");
        conf.addResource("hdfs-site.xml");
      }
    }
    handleConfigurationOverrides(conf, nnaConf);

    final long start = System.currentTimeMillis();
    if (preloadedInodes == null) {
      UserGroupInformation.setConfiguration(conf);
      reloadKeytab();

      // Auto fetch namespace on start-up if configured.
      if (nnaConf.allowBootstrapAutomaticFetch()) {
        final long fetchStart = System.currentTimeMillis();
        new TransferFsImageWrapper(this).downloadMostRecentImage();
        final long fetchEnd = System.currentTimeMillis();
        LOG.info("FSImage auto-fetched in: {} ms.", (fetchEnd - fetchStart));
      }

      LOG.info("Loading with configuration: {}", conf.toString());
      LOG.info(
          "FileSystem seen as: {}", conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
      LOG.info("Loading image from: {}", conf.get(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY));
      long start1 = System.currentTimeMillis();
      try {
        namesystem = FSNamesystem.loadFromDisk(conf);
        namesystem.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_ENTER);
      } catch (IOException e) {
        LOG.error("Failed to load namesystem.", e);
        return;
      }
      long end1 = System.currentTimeMillis();
      LOG.info("FSImage loaded in: {} ms.", (end1 - start1));
      LOG.info(
          "Loaded in {} Inodes + {} blocks",
          namesystem.getFilesTotal(),
          namesystem.getBlocksTotal());

      tokenExtractor = new TokenExtractor(namesystem.dtSecretManager, namesystem);
    } else {
      tokenExtractor = new TokenExtractor(null, null);
    }

    // Let QueryEngine deal with inode set from here.
    queryEngine.handleGSet(preloadedInodes, nnaConf, namesystem);

    if (preloadedInodes == null) {
      // Start tailing and updating security credentials threads.
      try {
        versionLoader.setNamesystem(namesystem);
        versionLoader.startStandbyServices(conf);
      } catch (Throwable e) {
        LOG.error("Failed to start EditLogTailer.", e);
      }
    }
    queryEngine.setVersionContext(versionLoader);

    long end = System.currentTimeMillis();
    LOG.info("NameNodeLoader bootstrap'd in: {} ms.", (end - start));
    inited.set(true);
  }

  @SuppressWarnings("unchecked")
  private QueryEngine initializeQueryEngine(ApplicationConfiguration nnaConf)
      throws ClassNotFoundException {
    String queryEngineClassString = nnaConf.getQueryEngineImplementation();
    LOG.info("Starting with QueryEngine implementation: {}", queryEngineClassString);
    Class<QueryEngine> queryEngineClass =
        (Class<QueryEngine>) Class.forName(queryEngineClassString);
    return ReflectionUtils.newInstance(queryEngineClass, null);
  }

  private void handleConfigurationOverrides(Configuration conf, ApplicationConfiguration nnaConf)
      throws URISyntaxException {
    if (nnaConf.allowBootstrapConfigurationOverrides()) {
      LOG.info("Setting: {} to: {} ", DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, (-1));
      conf.setInt(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY, -1);

      LOG.info("Setting: {} to: {}", DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_KEY, false);
      conf.setBoolean(DFSConfigKeys.DFS_HA_STANDBY_CHECKPOINTS_KEY, false);

      LOG.info("Setting: {} to: {}", DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_KEY, 0);
      conf.setInt(DFSConfigKeys.DFS_CONTENT_SUMMARY_LIMIT_KEY, 0);

      String baseDir = nnaConf.getBaseDir();
      LOG.info("Setting: {} to: {}/dfs/name", DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, baseDir);
      conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY, new URI(baseDir + "/dfs/name").getPath());

      String nameserviceId = DFSUtil.getOnlyNameServiceIdOrNull(conf);
      nameserviceId =
          (nameserviceId == null) ? conf.get(DFSConfigKeys.DFS_NAMESERVICE_ID) : nameserviceId;
      if (nameserviceId == null || nameserviceId.isEmpty()) {
        /* Hack for 2.4.0 support. Attempt to override with internal nameservices. */
        nameserviceId = conf.get("dfs.internal.nameservices");

        LOG.info("Setting: {} to: {}", DFSConfigKeys.DFS_NAMESERVICE_ID, nameserviceId);
        conf.set(DFSConfigKeys.DFS_NAMESERVICE_ID, nameserviceId);
      }

      /* Hack for 2.4.0 support. Unset external attribute provider. No Ranger support. */
      LOG.info("Unsetting: dfs.namenode.inode.attributes.provider.class");
      conf.unset("dfs.namenode.inode.attributes.provider.class");
    } else {
      LOG.warn(
          "Not performing defensive configuration overrides; using configuration as-is.\n"
              + "This instance may be acting upon your active cluster unless configured properly.\n"
              + "Please run with 'nna.support.bootstrap.overrides=true' if you did not intend this.");
    }
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

  /** Reloads the Kerberos keytab if security enabled. */
  public void reloadKeytab() {
    if (UserGroupInformation.isSecurityEnabled()) {
      try {
        SecurityUtil.login(
            conf,
            DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY,
            DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY,
            InetAddress.getLocalHost().getCanonicalHostName());
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Wipes out all the in-memory INode tree. Stops EditLog tailing. Stops report processing. */
  public void clear(boolean softClear) {
    if (!softClear) {
      if (keytabReloader != null) {
        keytabReloader.shutdown();
      }
      if (suggestionsReloader != null) {
        suggestionsReloader.shutdown();
      }
    }
    suggestionsEngine.stop();
    if (namesystem != null) {
      try {
        namesystem.stopStandbyServices();
      } catch (IOException e) {
        LOG.info("Failed to shutdown namesystem: " + e);
      }
      try {
        namesystem.getFSImage().getStorage().unlockAll();
      } catch (IOException e) {
        LOG.info("Failed to unlock storages of namesystem: " + e);
      }
      namesystem.shutdown();
      namesystem = null;
    }
    queryEngine.clear();
    inited.set(false);
  }

  /**
   * Takes the FSNamesystem writeLock. Certain queries may wish to take the lock if they are finding
   * inconsistent results or for debugging.
   *
   * @param useLock boolean for whether to take the lock or not
   */
  public void namesystemWriteLock(Boolean useLock) {
    if (useLock != null && useLock && namesystem != null) {
      namesystem.writeLock();
    }
  }

  /**
   * Releases the FSNamesystem writeLock. Any query that took the writeLock will also release the
   * lock.
   *
   * @param useLock boolean for whether to take the lock or not
   */
  public void namesystemWriteUnlock(Boolean useLock) {
    if (useLock != null && useLock && namesystem != null) {
      namesystem.writeUnlock();
    }
  }

  /**
   * Get the INode set that represents the String parameter.
   *
   * @param set the set of INodes the user wishes to query against
   * @return the in-memory set that represents the inodes asked for; a large collection typically
   */
  public Collection<INode> getINodeSet(String set) {
    return queryEngine.getINodeSet(set);
  }

  /**
   * Perform a content summary call against the underlying FSNamesystem. Do not send Exception
   * upwards - treat as empty.
   *
   * @param path the dir/file path to call content summary on
   * @return a summary of the subtree or file
   */
  public ContentSummary getContentSummary(String path) {
    try {
      return getContentSummaryImpl(path);
    } catch (IOException e) {
      LOG.error("Error with getContentSummary.", e);
    }
    return new ContentSummary.Builder().fileCount(0L).directoryCount(0L).build();
  }

  /**
   * Perform a content summary call against the underlying FSNamesystem.
   *
   * @param path the dir/file path to call content summary on
   * @return a summary of the subtree or file
   */
  public ContentSummary getContentSummaryImpl(String path) throws IOException {
    return namesystem.getContentSummary(path);
  }

  /**
   * Initializes the background thread that performs cached reporting for all users. Initializes the
   * background thread that refreshes Kerberos keytab for NNA process.
   *
   * @param internalService threadExecutor service hosted by {@link WebServerMain}
   * @param conf the application configuration
   */
  public void initReloadThreads(ExecutorService internalService, ApplicationConfiguration conf) {
    suggestionsReloader = new SuggestionsReloader(conf, suggestionsEngine, this);
    internalService.submit(suggestionsReloader);
    keytabReloader = new KeytabReloader(getConfiguration());
    internalService.submit(keytabReloader);
  }

  /**
   * If enabled, initializes embedded DB for local trending.
   *
   * @param hsqlDriver the hsqldriver; if null then error occurred above
   * @param conf the application configuration
   * @param isEnabled whether to attempt to enable or not
   * @throws SQLException error in starting embedded DB
   */
  public void initHistoryRecorder(
      HsqlDriver hsqlDriver, ApplicationConfiguration conf, boolean isEnabled) throws SQLException {
    if (isEnabled && hsqlDriver != null) {
      this.hsqlDriver = hsqlDriver;
      hsqlDriver.startDatabase(conf);
      hsqlDriver.createTable();
      historical.set(true);
    }
  }
}
