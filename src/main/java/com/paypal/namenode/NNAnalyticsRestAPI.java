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

package com.paypal.namenode;

import static spark.Spark.after;
import static spark.Spark.before;
import static spark.Spark.exception;
import static spark.Spark.get;

import com.google.common.annotations.VisibleForTesting;
import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JWEAlgorithm;
import com.paypal.security.SecurityConfiguration;
import com.paypal.security.SecurityContext;
import com.sun.management.OperatingSystemMXBean;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.NNAConstants;
import org.apache.hadoop.hdfs.server.namenode.NNLoader;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImageWrapper;
import org.apache.hadoop.hdfs.server.namenode.operations.BaseOperation;
import org.apache.hadoop.hdfs.server.namenode.operations.Delete;
import org.apache.hadoop.hdfs.server.namenode.operations.SetReplication;
import org.apache.hadoop.hdfs.server.namenode.operations.SetStoragePolicy;
import org.apache.hadoop.hdfs.server.namenode.queries.BaseQuery;
import org.apache.hadoop.hdfs.server.namenode.queries.Histograms;
import org.apache.hadoop.hdfs.server.namenode.queries.Transforms;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpStatus;
import org.ldaptive.ConnectionConfig;
import org.ldaptive.DefaultConnectionFactory;
import org.ldaptive.auth.Authenticator;
import org.ldaptive.auth.PooledBindAuthenticationHandler;
import org.ldaptive.pool.BlockingConnectionPool;
import org.ldaptive.pool.IdlePruneStrategy;
import org.ldaptive.pool.PoolConfig;
import org.ldaptive.pool.PooledConnectionFactory;
import org.ldaptive.pool.SearchValidator;
import org.ldaptive.ssl.KeyStoreCredentialConfig;
import org.ldaptive.ssl.SslConfig;
import org.pac4j.core.exception.BadCredentialsException;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.jwt.config.encryption.EncryptionConfiguration;
import org.pac4j.jwt.config.encryption.SecretEncryptionConfiguration;
import org.pac4j.jwt.config.signature.SecretSignatureConfiguration;
import org.pac4j.jwt.config.signature.SignatureConfiguration;
import org.pac4j.jwt.credentials.authenticator.JwtAuthenticator;
import org.pac4j.jwt.profile.JwtGenerator;
import org.pac4j.ldap.credentials.authenticator.LdapAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;

/**
 * This is the main class for launching a NameNode Analytics (NNA) instance.
 *
 * <p>It is best described as: “A modified, isolated, read-only, Standby NameNode, with no RPC
 * Server, and with a Web Server and custom query engine embedded inside it”.
 *
 * <p>NNA is a utility to provide real-time queries against an active HDFS instance without having
 * to touch the active cluster itself. It is meant to replace the old practice of parsing an FSImage
 * offline using a legacy FSImage and OfflineImageViewer (OIV) tool.
 *
 * <p>The goal for NNA is to be able aid cluster administrators in visualizing how data is being
 * used throughout their cluster via a simple query API.
 *
 * <p>NNA is bootstrap'd via a one-time cost of fetching the latest FSImage from the active cluster
 * and then utilizing that cluster's JournalNodes in order to stay up-to-date. Queries can be ran
 * against the NNA instance while it is updating since all queries are performed as read operations
 * and all filtered results are separate from the set of updating INodes in-memory.
 */
public class NNAnalyticsRestAPI {

  public static final Logger LOG = LoggerFactory.getLogger(NNAnalyticsRestAPI.class.getName());

  private final NNLoader nnLoader = new NNLoader();
  private final HSQLDriver hsqlDriver = new HSQLDriver();
  private final TransferFsImageWrapper transferFsImage = new TransferFsImageWrapper(nnLoader);
  private final List<BaseQuery> runningQueries = Collections.synchronizedList(new LinkedList<>());
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final SecurityContext secContext = new SecurityContext();

  private final ExecutorService operationService = Executors.newFixedThreadPool(1);
  private final ExecutorService internalService = Executors.newFixedThreadPool(2);
  private final Map<String, BaseOperation> runningOperations =
      Collections.synchronizedMap(new HashMap<>());
  private final AtomicBoolean savingNamespace = new AtomicBoolean(false);

  /**
   * This is the main launching call for use in production. Should not accept any arguments --
   * service is dictated by configuration files.
   *
   * @param args main argument although its not used
   * @throws InterruptedException InterruptedException
   * @throws IllegalAccessException IllegalAccessException
   * @throws NoSuchFieldException NoSuchFieldException
   */
  public static void main(String[] args)
      throws InterruptedException, IllegalAccessException, NoSuchFieldException {
    try {
      NNAnalyticsRestAPI main = new NNAnalyticsRestAPI();
      main.initAuth(null, null);
      main.initRestServer();
      main.initLoader(null, null);
    } catch (Throwable e) {
      LOG.info("FATAL: {}", e);
    }
  }

  public NNAnalyticsRestAPI() {}

  @VisibleForTesting
  public NNLoader initLoader(GSet<INode, INodeWithAdditionalFields> inodes, Boolean historical)
      throws Exception {
    return initLoader(inodes, historical, null);
  }

  /**
   * This is the method responsible for initializing the core of NNA -- the NNLoader. It is made
   * public for those wishing to set-up a customized NNA instance in tests.
   *
   * @param inodes Optional parameter if you wish to initialize NNLoader with your own set of
   *     INodes.
   * @param historical Optional parameter whether to load up embedded SQL DB for historical
   *     tracking; null means default to configuration. tracking; null means default to
   *     configuration.
   * @param preloadedHadoopConf Optional parameter for whether you want to load with predetermined
   *     configuration; null means to load configuration from classpath.
   * @return An initialized NNLoader with predetermined INodes else INodes loaded from local
   *     FSImage.
   * @throws Exception SQLException, InterruptedException, NoSuchFieldException,
   *     IllegalAccessException
   */
  @VisibleForTesting
  public NNLoader initLoader(
      GSet<INode, INodeWithAdditionalFields> inodes,
      Boolean historical,
      Configuration preloadedHadoopConf)
      throws Exception {
    SecurityConfiguration secConf = new SecurityConfiguration();
    hsqlDriver.dropConnection();
    if (historical != null) {
      nnLoader.initHistoryRecorder(hsqlDriver, secConf, historical);
    } else {
      nnLoader.initHistoryRecorder(hsqlDriver, secConf, secConf.getHistoricalEnabled());
    }
    nnLoader.load(inodes, preloadedHadoopConf);
    nnLoader.initReloadThreads(internalService, secConf);
    return nnLoader;
  }

  /**
   * This is the method responsible for initializing the security layer of NNA -- the
   * SecurityContext. It is made public for those wishing to set-up a customized NNA instance in
   * tests.
   *
   * @param overrideAuthentication Optional parameter dictating how you wish to override LDAP
   *     authentication; null means default to configuration.
   * @param overrideAuthorization Optional parameter dictating how you wish to override
   *     authorization; null means default to configuration.
   */
  @VisibleForTesting
  public void initAuth(Boolean overrideAuthentication, Boolean overrideAuthorization) {
    SecurityConfiguration secConf = new SecurityConfiguration();
    if (overrideAuthentication != null) {
      secConf.overrideLdapEnabled(overrideAuthentication);
    }
    if (overrideAuthorization != null) {
      secConf.overrideAuthorization(overrideAuthorization);
    }

    String sslKeystorePath = secConf.getSslKeystorePath();
    String sslKeystorePassword = secConf.getSslKeystorePassword();
    if (sslKeystorePath == null && sslKeystorePassword == null) {
      LOG.info("Running web server in HTTP mode.");
    } else if (sslKeystorePath != null && sslKeystorePassword != null) {
      LOG.info("Running web server in HTTPS mode.");
      Spark.secure(sslKeystorePath, sslKeystorePassword, null, null);
    } else {
      throw new IllegalStateException(
          "Illegal SSL configuration. Check config/security.properties file.");
    }

    boolean ldapEnabled = secConf.getLdapEnabled();
    Spark.port(secConf.getPort());
    if (ldapEnabled) {
      LOG.info("Enabled web security.");
      // jwt:
      SignatureConfiguration sigConf =
          new SecretSignatureConfiguration(secConf.getJwtSignatureSecret());
      EncryptionConfiguration encConf =
          new SecretEncryptionConfiguration(
              secConf.getJwtEncryptionSecret(), JWEAlgorithm.DIR, EncryptionMethod.A128GCM);
      JwtGenerator<CommonProfile> jwtGenerator = new JwtGenerator<>(sigConf, encConf);
      JwtAuthenticator jwtAuthenticator = new JwtAuthenticator(sigConf, encConf);

      // ldaptive:
      ConnectionConfig connectionConfig = new ConnectionConfig();
      KeyStoreCredentialConfig keyStoreCredentialConfig = new KeyStoreCredentialConfig();
      keyStoreCredentialConfig.setTrustStore(secConf.getLdapTrustStorePath());
      keyStoreCredentialConfig.setTrustStorePassword(secConf.getLdapTruststorePassword());
      connectionConfig.setUseStartTLS(secConf.getLdapUseStartTLS());
      connectionConfig.setUseSSL(true);
      connectionConfig.setSslConfig(new SslConfig(keyStoreCredentialConfig));
      connectionConfig.setConnectTimeout(secConf.getLdapConnectTimeout());
      connectionConfig.setResponseTimeout(secConf.getLdapResponseTimeout());
      connectionConfig.setLdapUrl(secConf.getLdapUrl());
      DefaultConnectionFactory connectionFactory = new DefaultConnectionFactory();
      connectionFactory.setConnectionConfig(connectionConfig);
      PoolConfig poolConfig = new PoolConfig();
      poolConfig.setMinPoolSize(secConf.getLdapConnectionPoolMinSize());
      poolConfig.setMaxPoolSize(secConf.getLdapConnectionPoolMaxSize());
      poolConfig.setValidateOnCheckOut(true);
      poolConfig.setValidateOnCheckIn(true);
      poolConfig.setValidatePeriodically(false);
      SearchValidator searchValidator = new SearchValidator();
      IdlePruneStrategy pruneStrategy = new IdlePruneStrategy();
      BlockingConnectionPool connectionPool = new BlockingConnectionPool();
      connectionPool.setPoolConfig(poolConfig);
      connectionPool.setBlockWaitTime(1000);
      connectionPool.setValidator(searchValidator);
      connectionPool.setPruneStrategy(pruneStrategy);
      connectionPool.setConnectionFactory(connectionFactory);
      connectionPool.initialize();
      PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory();
      pooledConnectionFactory.setConnectionPool(connectionPool);
      PooledBindAuthenticationHandler handler = new PooledBindAuthenticationHandler();
      handler.setConnectionFactory(pooledConnectionFactory);
      Authenticator ldaptiveAuthenticator = new Authenticator();
      ldaptiveAuthenticator.setAuthenticationHandler(handler);
      // pac4j:
      LdapAuthenticator ldapAuth = new LdapAuthenticator();
      ldapAuth.setLdapAuthenticator(ldaptiveAuthenticator);

      secContext.init(secConf, jwtAuthenticator, jwtGenerator, ldapAuth);
    } else {
      secContext.init(secConf, null, null, null);
      LOG.info("Disabled web security.");
    }
  }

  /**
   * This is the main method for launching the SparkJava web server. Once this call is made the web
   * server will run and REST API endpoints will be available.
   *
   * <p>It is made public for those wishing to have control of the Rest Server start-up of NNA
   * instances in tests.
   */
  @VisibleForTesting
  public void initRestServer() {
    /* This is the call to load everything under ./resources/public as HTML resources. */
    Spark.staticFileLocation("/public");

    /* ENDPOINTS endpoint is meant to showcase all available REST API endpoints in JSON list form. */
    get(
        "/endpoints",
        (req, res) -> {
          NNAHelper.toJsonList(res.raw(), NNAConstants.ENDPOINT.values());
          return res;
        });

    /* LOADINGSTATUS endpoint is meant to show the loading status of the NNA instance in JSON form. */
    get(
        "/loadingStatus",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          nnLoader.sendLoadingStatus(res.raw());
          return res;
        });

    /* SYSTEM endpoint is meant to show the system resource usage of the NNA instance in PLAINTEXT form. */
    /* TODO: Convert the output to JSON form. */
    get(
        "/system",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          Runtime runtime = Runtime.getRuntime();
          OperatingSystemMXBean operatingSystemMXBean =
              (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
          RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();
          MemoryMXBean mb = ManagementFactory.getMemoryMXBean();
          MemoryUsage memNonHeap = mb.getNonHeapMemoryUsage();
          MemoryUsage memHeap = mb.getHeapMemoryUsage();

          StringBuilder sb = new StringBuilder();
          int availableCores = runtime.availableProcessors();
          double cpuLoad = operatingSystemMXBean.getSystemCpuLoad();
          double procLoad = operatingSystemMXBean.getProcessCpuLoad();

          sb.append("Server uptime (ms): ").append(rb.getUptime()).append("\n\n");

          sb.append("Available Processor Cores: ").append(availableCores).append("\n");
          sb.append("System CPU Load: ").append(cpuLoad).append("\n");
          sb.append("Process CPU Load: ").append(procLoad).append("\n\n");

          sb.append("Non-Heap Used Memory (KB): ").append(memNonHeap.getUsed() / 1024).append("\n");
          sb.append("Non-Heap Committed Memory (KB): ")
              .append((memNonHeap.getCommitted() / 1024))
              .append("\n");
          sb.append("Non-Heap Max Memory (KB): ")
              .append((memNonHeap.getMax() / 1024))
              .append("\n\n");

          sb.append("Heap Used Memory (KB): ").append(memHeap.getUsed() / 1024).append("\n");
          sb.append("Heap Committed Memory (KB): ")
              .append((memHeap.getCommitted() / 1024))
              .append("\n");
          sb.append("Heap Max Memory (KB): ").append((memHeap.getMax() / 1024)).append("\n\n");

          sb.append("Max Memory (KB): ").append((runtime.maxMemory() / 1024)).append("\n");

          return sb.toString();
        });

    /* INFO endpoint is meant to show the information about the NNA instance in PLAINTEXT form. */
    /* TODO: Convert the output to JSON form. */
    get(
        "/info",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          StringBuilder sb = new StringBuilder();

          sb.append("Current active connections: ").append(runningQueries.size()).append("\n");
          sb.append("Current running queries:\n");
          for (BaseQuery query : runningQueries) {
            sb.append(query.toString()).append("\n");
          }
          sb.append("\n");

          boolean isInit = nnLoader.isInit();
          boolean isHistorical = nnLoader.isHistorical();
          boolean isProvidingSuggestions = nnLoader.getSuggestionsEngine().isLoaded();
          sb.append("Current system time (ms): ").append(Time.now()).append("\n");
          sb.append("Ready to service queries: ").append(isInit).append("\n");
          sb.append("Ready to service history: ").append(isHistorical).append("\n");
          sb.append("Ready to service suggestions: ").append(isProvidingSuggestions).append("\n\n");
          if (isInit) {
            long allSetSize = nnLoader.getINodeSet(NNAConstants.SET.all.name()).size();
            long fileSetSize = nnLoader.getINodeSet(NNAConstants.SET.files.name()).size();
            long dirSetSize = nnLoader.getINodeSet(NNAConstants.SET.dirs.name()).size();
            sb.append("Current TxID: ").append(nnLoader.getCurrentTxID()).append("\n");
            sb.append("INode GSet size: ").append(allSetSize).append("\n\n");
            sb.append("INodeFile set size: ").append(fileSetSize).append("\n");
            sb.append("INodeFile set percentage: ")
                .append(((fileSetSize * 100.0f) / allSetSize))
                .append("\n\n");
            sb.append("INodeDir set size: ").append(dirSetSize).append("\n");
            sb.append("INodeDir set percentage: ")
                .append(((dirSetSize * 100.0f) / allSetSize))
                .append("\n\n");
          }
          sb.append("Cached directories for analysis::\n");
          Set<String> dirs = nnLoader.getSuggestionsEngine().getDirectoriesForAnalysis();
          sb.append("Cached directories size: ").append(dirs.size()).append("\n");
          for (String dir : dirs) {
            sb.append(dir).append("\n");
          }
          return sb.toString();
        });

    /* THREADS endpoint is meant to show the thread information about the NNA instance in PLAINTEXT form. */
    /* TODO: Convert the output to JSON form. */
    get(
        "/threads",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          ThreadMXBean tb = ManagementFactory.getThreadMXBean();
          int threadsNew = 0;
          int threadsRunnable = 0;
          int threadsBlocked = 0;
          int threadsWaiting = 0;
          int threadsTimedWaiting = 0;
          int threadsTerminated = 0;
          long threadIds[] = tb.getAllThreadIds();
          for (ThreadInfo threadInfo : tb.getThreadInfo(threadIds, 0)) {
            if (threadInfo == null) {
              continue; // race protection
            }
            switch (threadInfo.getThreadState()) {
              case NEW:
                threadsNew++;
                break;
              case RUNNABLE:
                threadsRunnable++;
                break;
              case BLOCKED:
                threadsBlocked++;
                break;
              case WAITING:
                threadsWaiting++;
                break;
              case TIMED_WAITING:
                threadsTimedWaiting++;
                break;
              case TERMINATED:
                threadsTerminated++;
                break;
            }
          }
          return "New Threads: "
              + threadsNew
              + "\n"
              + "Runnable Threads: "
              + threadsRunnable
              + "\n"
              + "Blocked Threads: "
              + threadsBlocked
              + "\n"
              + "Waiting Threads: "
              + threadsWaiting
              + "\n"
              + "Timed-Waiting Threads: "
              + threadsTimedWaiting
              + "\n"
              + "Terminated Threads: "
              + threadsTerminated
              + "\n";
        });

    /* CONFIG endpoint is a dump of the system configuration in XML form. */
    /* TODO: Convert the output to JSON form. */
    get(
        "/config",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          String key = req.queryMap("key").value();
          if (key != null && !key.isEmpty()) {
            res.header("Content-Type", "text/plain");
            return nnLoader.getConfigValue(key);
          } else {
            res.header("Content-Type", "application/xml; charset=UTF-8");
            nnLoader.dumpConfig(res.raw());
            return res;
          }
        });

    /* REFRESH endpoint is an admin endpoint meant to change the set of read, write, and admin users. */
    get(
        "/refresh",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          secContext.refresh(new SecurityConfiguration());
          res.body(secContext.toString());
          return res;
        });

    /* CREDENTIALS endpoint is meant to showcase what authorizations the querying user possesses in JSON form. */
    get(
        "/credentials",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          NNAHelper.toJsonList(res.raw(), secContext.getAccessLevels());
          return res;
        });

    /* All security and query tracking should be done prior to accessing endpoints. */
    before(
        (req, res) -> {
          secContext.handleAuthentication(req, res);
          secContext.handleAuthorization(req, res);
          runningQueries.add(NNAHelper.createQuery(req.raw(), secContext.getUserName()));
        });

    /* HISTOGRAMS endpoint is meant to showcase the different types of histograms available in the "&type="
    parameter in JSON form. */
    get(
        "/histograms",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          NNAHelper.toJsonList(res.raw(), NNAConstants.HISTOGRAM.values());
          return res;
        });

    /* HISTOGRAMOUTPUTS endpoint is meant to showcase the different types of histogram output options available in
    the "&histogramOutput=" parameter in JSON form.
    These are only applicable under the /histogram endpoint. */
    get(
        "/histogramOutputs",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          NNAHelper.toJsonList(res.raw(), NNAConstants.HISTOGRAM_OUTPUT.values());
          return res;
        });

    /* FILTERS endpoint is meant to showcase the different types of filters options available in
    the "&filters=" parameter in JSON form.
    These are only applicable under the /divide, /filter, and /histogram endpoints. */
    get(
        "/filters",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          NNAHelper.toJsonList(res.raw(), NNAConstants.FILTER.values());
          return res;
        });

    /* FINDS endpoint is meant to showcase the different types of find options available in
    the "&find=" parameter in JSON form.
    These are only applicable under the /filter and /histogram endpoints. */
    get(
        "/finds",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          NNAHelper.toJsonList(res.raw(), NNAConstants.FIND.values());
          return res;
        });

    /* TRANSFORMS endpoint is meant to showcase the different types of INode field transform options available in
    the "&transformFields=" parameter in JSON form.
    These are only applicable under the /histogram endpoints. */
    /* TODO: Add transforms for /filter endpoint. */
    get(
        "/transforms",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          NNAHelper.toJsonList(res.raw(), NNAConstants.TRANSFORM.values());
          return res;
        });

    /* SETS endpoint is meant to showcase the different types of INode sets available in
    the "&set=" parameter in JSON form.
    These are only applicable under the /divide, /filter and /histogram endpoints. */
    get(
        "/sets",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          NNAHelper.toJsonList(res.raw(), NNAConstants.SET.values());
          return res;
        });

    /* FILTEROPS endpoint is meant to showcase the different types of filter operations options available in
    the "&filterOps=" parameter in JSON form.
    These are only applicable while the "&filters" parameter is utilized.
    Certain filterOps are only applicable to matching types of filters as well. */
    /* TODO: Remove this and build filterOps directly into filters API. */
    get(
        "/filterOps",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          NNAHelper.toJsonList(res.raw(), NNAConstants.FILTER_OP.values());
          return res;
        });

    /* SUMS endpoint is meant to showcase the different types of summations available in JSON form.
    These are only applicable under the /divide, /filter and /histogram endpoints.
    Under /divide and /filter, the "&sum=" type dictates what to sum of the filtered set.
    Under /histogram, the "&sum=" type dictates what the Y axis of the histogram represents. */
    get(
        "/sums",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          NNAHelper.toJsonList(res.raw(), NNAConstants.SUM.values());
          return res;
        });

    /* OPERATIONS endpoint is meant to showcase the different types of operations available in JSON form.
    These are only applicable under the /submitOperation endpoint.
    Under /submitOperation, the "&operation=" type dictates what operation to do with the filtered set. */
    get(
        "/operations",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          NNAHelper.toJsonList(res.raw(), NNAConstants.OPERATION.values());
          return res;
        });

    /* DUMP endpoint is for dumping an INode path's information in PLAINTEXT form. */
    get(
        "/dump",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          if (!nnLoader.isInit()) {
            return "Namesystem is not fully initialized.\n";
          }
          String path = req.queryMap("path").value();
          nnLoader.dumpINodeInDetail(path, res.raw());
          return res;
        });

    /* DIVIDE endpoint takes 2 sets of "set", "filter", "sum" parameters and returns the result of
    the filtered set "1" divided by filtered set "2" in PLAINTEXT form. */
    get(
        "/divide",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          if (!nnLoader.isInit()) {
            return "";
          }

          lock.writeLock().lock();
          try {
            String filterStr1 = req.queryMap("filters1").value();
            String filterStr2 = req.queryMap("filters2").value();
            String emailsToStr = req.queryMap("emailTo").value();
            String emailsCCStr = req.queryMap("emailCC").value();
            String emailFrom = req.queryMap("emailFrom").value();
            String emailHost = req.queryMap("emailHost").value();
            String emailConditionsStr = req.queryMap("emailConditions").value();
            String[] filters1 = NNAHelper.parseFilters(filterStr1);
            String[] filterOps1 = NNAHelper.parseFilterOps(filterStr1);
            String[] filters2 = NNAHelper.parseFilters(filterStr2);
            String[] filterOps2 = NNAHelper.parseFilterOps(filterStr2);
            String[] emailsTo = (emailsToStr != null) ? emailsToStr.split(",") : null;
            String[] emailsCC = (emailsCCStr != null) ? emailsCCStr.split(",") : null;
            String set1 = req.queryMap("set1").value();
            String set2 = req.queryMap("set2").value();
            String sumStr1 = req.queryMap("sum1").value();
            String sum1 = (sumStr1 != null) ? sumStr1 : "count";
            String sumStr2 = req.queryMap("sum2").value();
            String sum2 = (sumStr2 != null) ? sumStr2 : "count";
            QueryChecker.isValidQuery(set1, filters1, null, sum1, filterOps1, null);
            QueryChecker.isValidQuery(set2, filters2, null, sum2, filterOps2, null);

            Collection<INode> inodes1 =
                NNAHelper.performFilters(nnLoader, set1, filters1, filterOps1);
            Collection<INode> inodes2 =
                NNAHelper.performFilters(nnLoader, set2, filters2, filterOps2);

            if (!sum1.isEmpty() && !sum2.isEmpty()) {
              long sumValue1 = nnLoader.sum(inodes1, sum1);
              long sumValue2 = nnLoader.sum(inodes2, sum2);
              float division = (float) sumValue1 / (float) sumValue2;

              LOG.info("The result of {} dividied by {} is: {}", sumValue1, sumValue2, division);
              String message;
              if (division > 100) {
                message = String.valueOf(((long) division));
              } else {
                message = String.valueOf(division);
              }
              if (emailsTo != null
                  && emailsTo.length != 0
                  && emailHost != null
                  && emailFrom != null) {
                String subject = nnLoader.getAuthority() + " | DIVISION";
                try {
                  if (emailConditionsStr != null) {
                    MailOutput.check(emailConditionsStr, (long) division, nnLoader);
                  }
                  MailOutput.write(subject, message, emailHost, emailsTo, emailsCC, emailFrom);
                } catch (Exception e) {
                  LOG.info("Failed to email output with exception: {}", e);
                }
              }
              res.body(message);
            }

            return res;
          } finally {
            lock.writeLock().unlock();
          }
        });

    /* FILTER endpoint takes 1 set of "set", "filter", "sum" / "limit" parameters and returns either
    the list of file paths that pass the filters or the summation of the INode fields that pass the filters
    in PLAINTEXT form. */
    /* TODO: Consider separating logic of "list of file paths" to /dump endpoint. */
    /* TODO: Move "&filterOps=" into API of "&filters=" by making filter triplets separated by ":". */
    get(
        "/filter",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          if (!nnLoader.isInit()) {
            return "";
          }

          lock.writeLock().lock();
          try {
            String fullFilterStr = req.queryMap("filters").value();
            String emailsToStr = req.queryMap("emailTo").value();
            String emailsCCStr = req.queryMap("emailCC").value();
            String emailFrom = req.queryMap("emailFrom").value();
            String emailHost = req.queryMap("emailHost").value();
            String find = req.queryMap("find").value();
            String emailConditionsStr = req.queryMap("emailConditions").value();
            String[] filters = NNAHelper.parseFilters(fullFilterStr);
            String[] filterOps = NNAHelper.parseFilterOps(fullFilterStr);
            String[] emailsTo = (emailsToStr != null) ? emailsToStr.split(",") : null;
            String[] emailsCC = (emailsCCStr != null) ? emailsCCStr.split(",") : null;
            String set = req.queryMap("set").value();
            String sumStr = req.queryMap("sum").value();
            String[] sums = (sumStr != null) ? sumStr.split(",") : new String[] {"count"};
            Integer limit = req.queryMap("limit").integerValue();
            if (limit == null) {
              limit = Integer.MAX_VALUE;
            }

            for (String sum : sums) {
              QueryChecker.isValidQuery(set, filters, null, sum, filterOps, find);
            }

            Collection<INode> inodes =
                NNAHelper.performFilters(nnLoader, set, filters, filterOps, find);

            if (sums.length == 1 && sumStr != null) {
              String sum = sums[0];
              long sumValue = nnLoader.sum(inodes, sum);
              String message = String.valueOf(sumValue);
              if (emailsTo != null
                  && emailsTo.length != 0
                  && emailHost != null
                  && emailFrom != null) {
                String subject =
                    nnLoader.getAuthority()
                        + " | "
                        + sum
                        + " | "
                        + set
                        + " | Filters: "
                        + fullFilterStr;
                try {
                  if (emailConditionsStr != null) {
                    MailOutput.check(emailConditionsStr, sumValue, nnLoader);
                  }
                  MailOutput.write(subject, message, emailHost, emailsTo, emailsCC, emailFrom);
                } catch (Exception e) {
                  LOG.info("Failed to email output with exception: {}", e);
                }
              }
              LOG.info("Returning filter result: {}.", message);
              res.body(message);
            } else if (sums.length > 1 && sumStr != null) {
              StringBuilder message = new StringBuilder();
              for (String sum : sums) {
                long sumValue = nnLoader.sum(inodes, sum);
                message.append(sumValue).append("\n");
              }
              res.body(message.toString());
            } else {
              nnLoader.dumpINodePaths(inodes, limit, res.raw());
            }

            return res;
          } finally {
            lock.writeLock().unlock();
          }
        });

    /* HISTOGRAM endpoint takes 1 set of "set", "filter", "type", and  "sum" parameters and returns a histogram
    where the X-axis represents the "type" type and the Y-axis represents the "sum" type.
    Output types available dictated by "&histogramOutput=". Default is CHART form. */
    /* TODO: Consider separating logic of "list of file paths" to /dump endpoint. */
    /* TODO: Move "&filterOps=" into API of "&filters=" by making filter triplets separated by ":". */
    /* TODO: Consider renaming "type" parameter to something more meaningful. */
    get(
        "/histogram",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");

          if (!nnLoader.isInit()) {
            res.header("Content-Type", "application/json");
            return Histograms.toChartJsJson(new HashMap<>(), "not_loaded", "", "");
          }

          lock.writeLock().lock();
          try {
            String fullFilterStr = req.queryMap("filters").value();
            String histogramConditionsStr = req.queryMap("histogramConditions").value();
            String emailsToStr = req.queryMap("emailTo").value();
            String emailsCCStr = req.queryMap("emailCC").value();
            String emailFrom = req.queryMap("emailFrom").value();
            String emailHost = req.queryMap("emailHost").value();
            String emailConditionsStr = req.queryMap("emailConditions").value();
            String[] filters = NNAHelper.parseFilters(fullFilterStr);
            String[] filterOps = NNAHelper.parseFilterOps(fullFilterStr);
            String histType = req.queryMap("type").value();
            String set = req.queryMap("set").value();
            Integer top = req.queryMap("top").integerValue();
            Integer bottom = req.queryMap("bottom").integerValue();
            String sumStr = req.queryMap("sum").value();
            Boolean useLock = req.queryMap("useLock").booleanValue();
            Boolean sortAscending = req.queryMap("sortAscending").booleanValue();
            Boolean sortDescending = req.queryMap("sortDescending").booleanValue();
            String sum = (sumStr != null) ? sumStr : "count";
            String[] emailsTo = (emailsToStr != null) ? emailsToStr.split(",") : null;
            String[] emailsCC = (emailsCCStr != null) ? emailsCCStr.split(",") : null;
            String transformConditionsStr = req.queryMap("transformConditions").value();
            String transformFieldsStr = req.queryMap("transformFields").value();
            String transformOutputsStr = req.queryMap("transformOutputs").value();
            Integer parentDirDepth = req.queryMap("parentDirDepth").integerValue();
            String timeRangeStr = req.queryMap("timeRange").value();
            String timeRange = (timeRangeStr != null) ? timeRangeStr : "weekly";
            String outputTypeStr = req.queryMap("histogramOutput").value();
            String outputType = (outputTypeStr != null) ? outputTypeStr : "chart";
            String type = req.queryMap("type").value();
            String find = req.queryMap("find").value();

            QueryChecker.isValidQuery(set, filters, type, sum, filterOps, find);
            Collection<INode> inodes = NNAHelper.performFilters(nnLoader, set, filters, filterOps);

            NNAConstants.HISTOGRAM htEnum = NNAConstants.HISTOGRAM.valueOf(histType);
            Map<String, Function<INode, Long>> transformMap =
                Transforms.getAttributeTransforms(
                    transformConditionsStr, transformFieldsStr, transformOutputsStr, nnLoader);
            Map<String, Long> histogram;
            long startTime = System.currentTimeMillis();
            String xAxis;

            nnLoader.namesystemWriteLock(useLock);
            try {
              switch (htEnum) {
                case user:
                  histogram = nnLoader.byUserHistogram(inodes, sum, find);
                  xAxis = "User Names";
                  break;
                case group:
                  histogram = nnLoader.byGroupHistogram(inodes, sum, find);
                  xAxis = "Group Names";
                  break;
                case accessTime:
                  histogram = nnLoader.accessTimeHistogram(inodes, sum, find, timeRange);
                  xAxis = "Last Accessed Time";
                  break;
                case modTime:
                  histogram = nnLoader.modTimeHistogram(inodes, sum, find, timeRange);
                  xAxis = "Last Modified Time";
                  break;
                case fileSize:
                  histogram = nnLoader.fileSizeHistogram(inodes, sum, find);
                  xAxis = "File Sizes (No Replication Factor)";
                  break;
                case diskspaceConsumed:
                  histogram = nnLoader.diskspaceConsumedHistogram(inodes, sum, find, transformMap);
                  xAxis = "Diskspace Consumed (File Size * Replication Factor)";
                  break;
                case fileReplica:
                  histogram = nnLoader.fileReplicaHistogram(inodes, sum, find, transformMap);
                  xAxis = "File Replication Factor";
                  break;
                case storageType:
                  histogram = nnLoader.storageTypeHistogram(inodes, sum, find);
                  xAxis = "Storage Type Policy";
                  break;
                case memoryConsumed:
                  histogram = nnLoader.memoryConsumedHistogram(inodes, sum, find);
                  xAxis = "Memory Consumed";
                  break;
                case parentDir:
                  histogram = nnLoader.parentDirHistogram(inodes, parentDirDepth, sum, find);
                  xAxis = "Directory Path";
                  break;
                case fileType:
                  histogram = nnLoader.fileTypeHistogram(inodes, sum, find);
                  xAxis = "File Type";
                  break;
                case dirQuota:
                  histogram = nnLoader.dirQuotaHistogram(inodes, sum);
                  xAxis = "Directory Path";
                  break;
                default:
                  throw new IllegalArgumentException(
                      "Could not determine histogram type: "
                          + histType
                          + ".\nPlease check /histograms for available histograms.");
              }
            } finally {
              nnLoader.namesystemWriteUnlock(useLock);
            }

            // Perform conditions filtering.
            if (histogramConditionsStr != null && !histogramConditionsStr.isEmpty()) {
              histogram = nnLoader.removeKeysOnConditional(histogram, histogramConditionsStr);
            }

            // Slice top and bottom.
            if (top != null && bottom != null) {
              throw new IllegalArgumentException("Please choose only one type of slice.");
            } else if (top != null && top > 0) {
              histogram = Histograms.sliceToTop(histogram, top);
            } else if (bottom != null && bottom > 0) {
              histogram = Histograms.sliceToBottom(histogram, bottom);
            }

            // Sort results.
            if (sortAscending != null && sortDescending != null) {
              throw new IllegalArgumentException("Please choose one type of sort.");
            } else if (sortAscending != null && sortAscending) {
              histogram = Histograms.sortByValue(histogram, true);
            } else if (sortDescending != null && sortDescending) {
              histogram = Histograms.sortByValue(histogram, false);
            }

            long endTime = System.currentTimeMillis();
            LOG.info("Performing histogram: {} took: {} ms.", histType, (endTime - startTime));

            // Email out.
            if (emailsTo != null
                && emailsTo.length != 0
                && emailHost != null
                && emailFrom != null) {
              String subject =
                  nnLoader.getAuthority()
                      + " | X: "
                      + histType
                      + " | Y: "
                      + sum
                      + " | "
                      + set
                      + " | Filters: "
                      + fullFilterStr;
              try {
                Set<String> highlightKeys = new HashSet<>();
                if (emailConditionsStr != null) {
                  MailOutput.check(emailConditionsStr, histogram, highlightKeys, nnLoader);
                }
                MailOutput.write(
                    subject, histogram, highlightKeys, emailHost, emailsTo, emailsCC, emailFrom);
              } catch (Exception e) {
                LOG.info("Failed to email output with exception: {}", e);
              }
            }

            // Return final histogram to Web UI as output type.
            NNAConstants.HISTOGRAM_OUTPUT output =
                NNAConstants.HISTOGRAM_OUTPUT.valueOf(outputType);
            switch (output) {
              case chart:
                res.header("Content-Type", "application/json");
                return Histograms.toChartJsJson(
                    histogram, NNAHelper.toTitle(histType, sum), NNAHelper.toYAxis(sum), xAxis);
              case json:
                res.header("Content-Type", "application/json");
                return Histograms.toJson(histogram);
              case csv:
                res.header("Content-Type", "text/plain");
                return Histograms.toCSV(histogram, find);
              default:
                throw new IllegalArgumentException(
                    "Could not determine output type: "
                        + histType
                        + ".\nPlease check /histogramOutputs for available histogram outputs.");
            }
          } finally {
            lock.writeLock().unlock();
          }
        });

    /* HISTOGRAM2 endpoint takes 1 set of "set", "filter", "type", and  "sum" parameters and returns a histogram
    where the X-axis represents the "type" type and the Y-axis represents the "sum" type.
    Output types available dictated by "&histogramOutput=". Default is CHART form.
    This differs from HISTOGRAM endpoint in that it can output multiple sums and values in a single query. */
    get(
        "/histogram2",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");

          if (!nnLoader.isInit()) {
            res.header("Content-Type", "application/json");
            return Histograms.toChartJsJson(new HashMap<>(), "not_loaded", "", "");
          }

          lock.writeLock().lock();
          try {
            String fullFilterStr = req.queryMap("filters").value();
            String[] filters = NNAHelper.parseFilters(fullFilterStr);
            String[] filterOps = NNAHelper.parseFilterOps(fullFilterStr);
            String histType = req.queryMap("type").value();
            String set = req.queryMap("set").value();
            String sumStr = req.queryMap("sum").value();
            Integer sortAscendingIndex = req.queryMap("sortAscendingIndex").integerValue();
            Integer sortDescendingIndex = req.queryMap("sortDescendingIndex").integerValue();
            String histogramConditionsStr = req.queryMap("histogramConditions").value();
            Boolean useLock = req.queryMap("useLock").booleanValue();
            String[] sums = (sumStr != null) ? sumStr.split(",") : new String[0];
            Integer parentDirDepth = req.queryMap("parentDirDepth").integerValue();
            String outputTypeStr = req.queryMap("histogramOutput").value();
            String timeRangeStr = req.queryMap("timeRange").value();
            String timeRange = (timeRangeStr != null) ? timeRangeStr : "weekly";
            String outputType = (outputTypeStr != null) ? outputTypeStr : "json";
            String type = req.queryMap("type").value();
            String findStr = req.queryMap("find").value();
            String[] finds = (findStr != null) ? findStr.split(",") : new String[0];

            for (String sum : sums) {
              QueryChecker.isValidQuery(set, filters, type, sum, filterOps, null);
            }
            for (String find : finds) {
              QueryChecker.isValidQuery(set, filters, type, null, filterOps, find);
            }
            Collection<INode> inodes = NNAHelper.performFilters(nnLoader, set, filters, filterOps);

            NNAConstants.HISTOGRAM htEnum = NNAConstants.HISTOGRAM.valueOf(histType);
            List<Map<String, Long>> histograms = new ArrayList<>(sums.length + finds.length);

            long startTime = System.currentTimeMillis();
            for (int i = 0, j = 0; i < sums.length || j < finds.length; ) {
              Map<String, Long> histogram;
              String sum = null;
              String find = null;
              if (i < sums.length) {
                sum = sums[i];
                i++;
              } else {
                find = finds[j];
                j++;
              }

              nnLoader.namesystemWriteLock(useLock);
              try {
                switch (htEnum) {
                  case user:
                    histogram = nnLoader.byUserHistogram(inodes, sum, find);
                    break;
                  case group:
                    histogram = nnLoader.byGroupHistogram(inodes, sum, find);
                    break;
                  case accessTime:
                    histogram = nnLoader.accessTimeHistogram(inodes, sum, find, timeRange);
                    break;
                  case modTime:
                    histogram = nnLoader.modTimeHistogram(inodes, sum, find, timeRange);
                    break;
                  case fileSize:
                    histogram = nnLoader.fileSizeHistogram(inodes, sum, find);
                    break;
                  case diskspaceConsumed:
                    histogram = nnLoader.diskspaceConsumedHistogram(inodes, sum, find, null);
                    break;
                  case fileReplica:
                    histogram = nnLoader.fileReplicaHistogram(inodes, sum, find, null);
                    break;
                  case storageType:
                    histogram = nnLoader.storageTypeHistogram(inodes, sum, find);
                    break;
                  case memoryConsumed:
                    histogram = nnLoader.memoryConsumedHistogram(inodes, sum, find);
                    break;
                  case parentDir:
                    histogram = nnLoader.parentDirHistogram(inodes, parentDirDepth, sum, find);
                    break;
                  case fileType:
                    histogram = nnLoader.fileTypeHistogram(inodes, sum, find);
                    break;
                  default:
                    throw new IllegalArgumentException(
                        "Could not determine histogram type: "
                            + histType
                            + ".\nPlease check /histograms for available histograms.");
                }
              } finally {
                nnLoader.namesystemWriteUnlock(useLock);
              }
              histograms.add(histogram);
            }

            Map<String, List<Long>> mergedHistogram =
                histograms
                    .parallelStream()
                    .flatMap(m -> m.entrySet().stream())
                    .collect(
                        Collectors.groupingBy(
                            Map.Entry::getKey,
                            Collector.of(
                                ArrayList<Long>::new,
                                (list, item) -> list.add(item.getValue()),
                                (left, right) -> {
                                  left.addAll(right);
                                  return left;
                                })));

            // Perform conditions filtering.
            if (histogramConditionsStr != null && !histogramConditionsStr.isEmpty()) {
              mergedHistogram =
                  nnLoader.removeKeysOnConditional2(mergedHistogram, histogramConditionsStr);
            }

            // Sort results.
            if (sortAscendingIndex != null && sortDescendingIndex != null) {
              throw new IllegalArgumentException("Please choose one type of sort index.");
            } else if (sortAscendingIndex != null) {
              mergedHistogram = Histograms.sortByValue(mergedHistogram, sortAscendingIndex, true);
            } else if (sortDescendingIndex != null) {
              mergedHistogram = Histograms.sortByValue(mergedHistogram, sortDescendingIndex, false);
            }

            long endTime = System.currentTimeMillis();
            LOG.info("Performing histogram2: {} took: {} ms.", histType, (endTime - startTime));

            // Return final histogram to Web UI as output type.
            NNAConstants.HISTOGRAM_OUTPUT output =
                NNAConstants.HISTOGRAM_OUTPUT.valueOf(outputType);
            switch (output) {
              case json:
                res.header("Content-Type", "application/json");
                return Histograms.toJson(mergedHistogram);
              case csv:
                res.header("Content-Type", "text/plain");
                return Histograms.toCSV(mergedHistogram);
              default:
                throw new IllegalArgumentException(
                    "Could not determine output type: "
                        + histType
                        + ".\nPlease check /histogramOutputs for available histogram outputs.");
            }
          } finally {
            lock.writeLock().unlock();
          }
        });

    /* FILTER endpoint takes 1 set of "set", "filter", "sum" / "limit" parameters and returns either
    the list of file paths that pass the filters or the summation of the INode fields that pass the filters
    in PLAINTEXT form. */
    /* TODO: Consider separating logic of "list of file paths" to /dump endpoint. */
    /* TODO: Move "&filterOps=" into API of "&filters=" by making filter triplets separated by ":". */
    get(
        "/submitOperation",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          if (!nnLoader.isInit()) {
            return "";
          }

          lock.writeLock().lock();
          try {
            String fullFilterStr = req.queryMap("filters").value();
            String find = req.queryMap("find").value();
            String[] filters = NNAHelper.parseFilters(fullFilterStr);
            String[] filterOps = NNAHelper.parseFilterOps(fullFilterStr);
            String set = req.queryMap("set").value();
            Integer limit = req.queryMap("limit").integerValue();
            Integer sleep = req.queryMap("sleep").integerValue();
            String operation = req.queryMap("operation").value();
            if (limit == null) {
              limit = Integer.MAX_VALUE;
            }
            if (sleep == null) {
              sleep = NNAConstants.DEFAULT_DELETE_SLEEP_MS;
            }
            if (operation == null || operation.isEmpty()) {
              throw new IllegalArgumentException("No operation defined. Please check /operations.");
            }
            QueryChecker.isValidQuery(set, filters, null, null, filterOps, find);

            Collection<INode> inodes =
                NNAHelper.performFilters(nnLoader, set, filters, filterOps, find);
            if (inodes.size() == 0) {
              LOG.info("Skipping operation request because it resulted in empty INode set.");
              throw new IOException(
                  "Skipping operation request because it resulted in empty INode set.");
            }

            FileSystem fs = nnLoader.getFileSystem();
            String[] operationSplits = operation.split(":");
            BaseOperation operationObj;
            switch (operationSplits[0]) {
              case "delete":
                operationObj = new Delete(inodes, req.queryString(), secContext.getUserName(), fs);
                break;
              case "setReplication":
                short newReplFactor = Short.parseShort(operationSplits[1]);
                operationObj =
                    new SetReplication(
                        inodes, req.queryString(), secContext.getUserName(), fs, newReplFactor);
                break;
              case "setStoragePolicy":
                operationObj =
                    new SetStoragePolicy(
                        inodes,
                        req.queryString(),
                        secContext.getUserName(),
                        fs,
                        operationSplits[1]);
                break;
              default:
                throw new IllegalArgumentException(
                    "Unknown operation:" + operationSplits[0] + ". Please check /operations.");
            }
            runningOperations.put(operationObj.identity(), operationObj);

            final int finalSleep = sleep;
            Callable<Object> callable =
                Executors.callable(
                    () -> {
                      try {
                        operationObj.initialize();
                        while (operationObj.hasNext()) {
                          try {
                            if (finalSleep >= 100) {
                              Thread.sleep(finalSleep);
                            }
                          } catch (InterruptedException ignored) {
                          }
                          operationObj.performOp();
                        }
                        operationObj.close();
                      } catch (IllegalStateException e) {
                        operationObj.abort();
                        LOG.info("Aborted operation due to: {}", e);
                      }
                      runningOperations.remove(operationObj.identity());
                    });
            operationService.submit(callable);
            res.body(operationObj.identity());

            return res;
          } finally {
            lock.writeLock().unlock();
          }
        });

    /* FILTER endpoint takes 1 set of "set", "filter", "sum" / "limit" parameters and returns either
    the list of file paths that pass the filters or the summation of the INode fields that pass the filters
    in PLAINTEXT form. */
    /* TODO: Consider separating logic of "list of file paths" to /dump endpoint. */
    /* TODO: Move "&filterOps=" into API of "&filters=" by making filter triplets separated by ":". */
    get(
        "/listOperations",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          if (!nnLoader.isInit()) {
            return "";
          }

          lock.writeLock().lock();
          try {
            String identity = req.queryMap("identity").value();
            Integer limit = req.queryMap("limit").integerValue();
            if (limit == null) {
              limit = 1;
            }
            if (identity == null || identity.length() == 0) {
              Set<Map.Entry<String, BaseOperation>> entries = runningOperations.entrySet();
              StringBuilder sb = new StringBuilder();
              sb.append("Total Operations: ").append(entries.size()).append('\n');
              for (Map.Entry<String, BaseOperation> entry : entries) {
                BaseOperation operation = entry.getValue();
                sb.append("ID: ");
                sb.append(entry.getKey());
                sb.append(", total: ");
                sb.append(operation.totalToPerform());
                sb.append(", operated: ");
                sb.append(operation.numPerformed());
                sb.append(" || owner: ");
                sb.append(operation.owner());
                sb.append(" || type: ");
                sb.append(operation.type());
                sb.append(" || query: ");
                sb.append(operation.query());
                sb.append('\n');
              }
              res.body(sb.toString());
            } else {
              BaseOperation operation = runningOperations.get(identity);
              if (operation == null) {
                throw new MalformedURLException("Operation not found.");
              }
              int totalToPerform = operation.totalToPerform();
              int numPerformed = operation.numPerformed();
              double percentageDone = ((double) numPerformed / (double) totalToPerform) * 100.0;
              List<String> lastDeleted = operation.lastPerformed(limit);
              Collections.reverse(lastDeleted);

              String sb =
                  "Identity: "
                      + operation.identity()
                      + '\n'
                      + "Owner: "
                      + operation.owner()
                      + '\n'
                      + "Query: "
                      + operation.query()
                      + '\n'
                      + "Total to perform: "
                      + totalToPerform
                      + '\n'
                      + "Total performed: "
                      + numPerformed
                      + '\n'
                      + "Total left to perform: "
                      + (totalToPerform - numPerformed)
                      + '\n'
                      + "Percentage done: "
                      + percentageDone
                      + '\n'
                      + "Up next: "
                      + operation.upNext()
                      + '\n'
                      + "Last ("
                      + limit
                      + ") performed: "
                      + lastDeleted.toString()
                      + "\n\n";
              res.body(sb);
            }
            return res;
          } finally {
            lock.writeLock().unlock();
          }
        });

    /* FILTER endpoint takes 1 set of "set", "filter", "sum" / "limit" parameters and returns either
    the list of file paths that pass the filters or the summation of the INode fields that pass the filters
    in PLAINTEXT form. */
    /* TODO: Consider separating logic of "list of file paths" to /dump endpoint. */
    /* TODO: Move "&filterOps=" into API of "&filters=" by making filter triplets separated by ":". */
    get(
        "/abortOperation",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          if (!nnLoader.isInit()) {
            return "";
          }

          lock.writeLock().lock();
          try {
            String identity = req.queryMap("identity").value();
            Integer limit = req.queryMap("limit").integerValue();
            if (limit == null) {
              limit = 1;
            }
            BaseOperation operation = runningOperations.get(identity);
            if (operation == null) {
              throw new MalformedURLException("Operation not found.");
            }
            operation.abort();

            LOG.info("Aborted Operation: {}", operation.identity());

            int totalToPerform = operation.totalToPerform();
            int numPerformed = operation.numPerformed();
            double percentageDone = ((double) numPerformed / (double) totalToPerform) * 100.0;
            List<String> lastPerformed = operation.lastPerformed(limit);
            Collections.reverse(lastPerformed);

            String sb =
                "Identity: "
                    + operation.identity()
                    + '\n'
                    + "Owner: "
                    + operation.owner()
                    + '\n'
                    + "Query: "
                    + operation.query()
                    + '\n'
                    + "Total to perform: "
                    + totalToPerform
                    + '\n'
                    + "Total performed: "
                    + numPerformed
                    + '\n'
                    + "Total left to perform: "
                    + (totalToPerform - numPerformed)
                    + '\n'
                    + "Percentage done: "
                    + percentageDone
                    + '\n'
                    + "Up next: "
                    + operation.upNext()
                    + '\n'
                    + "Last ("
                    + limit
                    + ") performed: "
                    + lastPerformed.toString()
                    + "\n\n";
            res.body(sb);
            return res;
          } finally {
            lock.writeLock().unlock();
          }
        });

    /* SUGGESTIONS endpoint is an admin-level endpoint meant to dump the cached analysis by NNA. */
    get(
        "/suggestions",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json");
          String username = req.queryMap("username").value();
          return nnLoader.getSuggestionsEngine().getSuggestionsAsJson(username);
        });

    /* DIRECTORIES endpoint is an reader-level endpoint meant to dump the cached directory analysis by NNA. */
    get(
        "/directories",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json");
          String directory = req.queryMap("dir").value();
          String sum = req.queryMap("sum").value();
          if (sum == null || sum.isEmpty()) {
            sum = "count";
          }
          return nnLoader.getSuggestionsEngine().getDirectoriesAsJson(directory, sum);
        });

    /* DIRECTORIES endpoint is an reader-level endpoint meant to dump the cached directory analysis by NNA. */
    get(
        "/fileAge",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json");
          String sum = req.queryMap("sum").value();
          if (sum == null || sum.isEmpty()) {
            sum = "count";
          }
          return nnLoader.getSuggestionsEngine().getFileAgeAsJson(sum);
        });

    /* ADDDIRECTORY endpoint is an admin-level endpoint meant to add a directory for cached analysis by NNA. */
    get(
        "/addDirectory",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          String directory = req.queryMap("dir").value();
          nnLoader.getSuggestionsEngine().addDirectoryToAnalysis(directory);
          res.status(HttpStatus.SC_OK);
          res.body(directory + " added for analysis.");
          return res;
        });

    /* REMOVEDIRECTORY endpoint is an admin-level endpoint meant to remove a directory from analysis by NNA. */
    get(
        "/removeDirectory",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          String directory = req.queryMap("dir").value();
          nnLoader.getSuggestionsEngine().removeDirectoryFromAnalysis(directory);
          res.status(HttpStatus.SC_OK);
          res.body(directory + " removed from analysis.");
          return res;
        });

    /* USERS endpoint is an admin-level endpoint meant to dump the cached set of detected quotas by NNA. */
    get(
        "/quotas",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json");
          String user = req.queryMap("user").value();
          String sum = req.queryMap("sum").value();
          return nnLoader.getSuggestionsEngine().getQuotaAsJson(user, sum);
        });

    /* USERS endpoint is an admin-level endpoint meant to dump the cached set of detected users by NNA. */
    get(
        "/users",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json");
          String suggestion = req.queryMap("suggestion").value();
          return nnLoader.getSuggestionsEngine().getUsersAsJson(suggestion);
        });

    /* TOP endpoint is an admin-level endpoint meant to dump the cached set of top issues by NNA. */
    get(
        "/top",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json");
          Integer limit = req.queryMap("limit").integerValue();
          if (limit == null) {
            limit = 10;
          }
          return nnLoader.getSuggestionsEngine().getIssuesAsJson(limit, false);
        });

    /* BOTTOM endpoint is an admin-level endpoint meant to dump the cached set of bottom issues by NNA.
     * The use-case here is for finding users to 'clear out'. */
    get(
        "/bottom",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json");
          Integer limit = req.queryMap("limit").integerValue();
          if (limit == null) {
            limit = 10;
          }
          return nnLoader.getSuggestionsEngine().getIssuesAsJson(limit, true);
        });

    /* HISTORY endpoint returns a set of data points from DB-stored suggestion snapshots. */
    get(
        "/history",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json");
          String usernameStr = req.queryMap("username").value();
          String username = (usernameStr == null) ? "" : usernameStr;
          String fromDate = req.queryMap("fromDate").value();
          String toDate = req.queryMap("toDate").value();
          return hsqlDriver.getAllHistory(fromDate, toDate, username);
        });

    /* TOKEN endpoint returns a set of user names and the last known DelegationToken issuance date. */
    get(
        "/token",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json");
          return nnLoader.getSuggestionsEngine().getTokens();
        });

    /* SAVENAMESPACE endpoint is an admin-level endpoint meant to dump the in-memory INode set
    to a fresh FSImage. */
    get(
        "/saveNamespace",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          String dirStr = req.queryMap("dir").value();
          String dir = (dirStr != null) ? dirStr : "/usr/local/nn-analytics/dfs/name/legacy/";
          Boolean legacy = req.queryMap("legacy").booleanValue();
          if (savingNamespace.get()) {
            return "Already saving namespace.";
          }
          PrintWriter writer = res.raw().getWriter();
          try {
            savingNamespace.set(true);
            writer.write("Saving namespace.<br />");
            writer.flush();
            if (legacy != null && legacy) {
              nnLoader.saveLegacyNamespace(dir);
            } else {
              nnLoader.saveNamespace();
            }
            writer.write("Done.");
            writer.flush();
          } catch (Throwable e) {
            LOG.warn("Namespace save failed.", e);
            writer.write("Save failed: " + e);
            writer.flush();
          } finally {
            writer.close();
            savingNamespace.set(false);
          }
          return res;
        });

    /* FETCHNAMESPACE endpoint is an admin-level endpoint meant to fetch a fresh, up-to-date, FSImage from
    the active cluster. */
    get(
        "/fetchNamespace",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          PrintWriter writer = res.raw().getWriter();
          writer.write("Attempting to bootstrap namespace.<br />");
          writer.flush();
          try {
            transferFsImage.downloadMostRecentImage();
            writer.write(
                "Done.<br />Please reload by going to `/reloadNamespace` or by `service nn-analytics restart` on command line.");
            writer.flush();
          } catch (Throwable e) {
            writer.write("Bootstrap failed: " + e);
            writer.flush();
          } finally {
            writer.close();
          }
          return res;
        });

    /* RELOADNAMESPACE endpoint is an admin-level endpoint meant to clear the current in-memory INode set
    and reload it from the latest FSImage found in the NNA instance's configured namespace directory. */
    get(
        "/reloadNamespace",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          lock.writeLock().lock();
          try {
            nnLoader.clear();
            nnLoader.load(null, null);
            res.body("Reload complete.");
          } catch (Throwable e) {
            res.body("Reload failed: " + e);
          } finally {
            lock.writeLock().unlock();
          }
          return res;
        });

    /* LOG endpoint is an admin-level endpoint meant to dump the last "limit" bytes of the log file. */
    get(
        "/log",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          Integer charsLimit = req.queryMap("limit").integerValue();
          nnLoader.dumpLog(charsLimit, res.raw());
          return res;
        });

    /* DROP endpoint is an admin-level endpoint meant to drop and rebuild embedded DB tables. */
    get(
        "/drop",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          String table = req.queryMap("table").value();
          hsqlDriver.rebuildTable(table);
          res.body("Successfully re-built table: " + table);
          return res;
        });

    /* TRUNCATE endpoint is an admin-level endpoint meant to truncate the history of embedded DB tables. */
    get(
        "/truncate",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          String table = req.queryMap("table").value();
          Integer limit = req.queryMap("limit").integerValue();
          hsqlDriver.truncateTable(table, limit);
          res.body("Successfully truncated table: " + table + ", to last: " + limit + " days.");
          return res;
        });

    /* Any query tracking should be removed once the query is completed. */
    after(
        (req, res) -> {
          res.header("Content-Encoding", "gzip");
          runningQueries.remove(NNAHelper.createQuery(req.raw(), secContext.getUserName()));
        });

    /* Any encountered Exceptions should be handled here and returned with appropriate HTTP error codes. */
    exception(
        Exception.class,
        (ex, req, res) -> {
          if (ex instanceof AuthenticationException || ex instanceof BadCredentialsException) {
            res.header("WWW-Authenticate", "Basic realm=\"Restricted\"");
            res.status(HttpStatus.SC_UNAUTHORIZED);
            res.body(ex.toString());
          } else if (ex instanceof AuthorizationException) {
            res.header("Access-Control-Allow-Origin", "*");
            res.header("Content-Type", "text/plain");
            res.status(HttpStatus.SC_FORBIDDEN);
            res.body(ex.getMessage());
          } else if (ex instanceof MalformedURLException || ex instanceof SQLException) {
            res.header("Access-Control-Allow-Origin", "*");
            res.header("Content-Type", "text/plain");
            res.status(HttpStatus.SC_BAD_REQUEST);
            res.body(ex.getMessage());
            runningQueries.remove(NNAHelper.createQuery(req.raw(), secContext.getUserName()));
          } else {
            res.header("Access-Control-Allow-Origin", "*");
            res.header("Content-Type", "text/plain");
            res.status(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            try {
              PrintWriter writer = res.raw().getWriter();
              writer.write("Query failed! Stacktrace is below:\n");
              writer.write(
                  "You can check all available queries at /sets, /filters, /filterOps, /histograms, and /sums.\n\n");
              StackTraceElement[] stackTrace = ex.getStackTrace();
              writer.println(ex.getLocalizedMessage());
              writer.flush();
              for (StackTraceElement element : stackTrace) {
                writer.println(element.toString());
                writer.flush();
              }
              IOUtils.closeStream(writer);
            } catch (IOException ignored) {
            } finally {
              runningQueries.remove(NNAHelper.createQuery(req.raw(), secContext.getUserName()));
            }
          }
          LOG.info("EXCEPTION encountered: {}", ex);
          LOG.info(Arrays.toString(ex.getStackTrace()));
        });

    Spark.awaitInitialization();
  }

  @VisibleForTesting
  public void shutdown() {
    try {
      hsqlDriver.dropConnection();
    } catch (Exception e) {
      LOG.error("Error during shutdown: ", e);
    }
    nnLoader.clear();
    runningOperations.clear();
    runningOperations.clear();
    runningQueries.clear();
    operationService.shutdown();
    internalService.shutdown();
    Spark.stop();
  }
}
