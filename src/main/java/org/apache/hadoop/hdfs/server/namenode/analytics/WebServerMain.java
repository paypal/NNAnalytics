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

package org.apache.hadoop.hdfs.server.namenode.analytics;

import static spark.Spark.after;
import static spark.Spark.before;
import static spark.Spark.exception;
import static spark.Spark.get;
import static spark.Spark.post;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JWEAlgorithm;
import com.sun.management.OperatingSystemMXBean;
import java.io.FileNotFoundException;
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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.namenode.Constants;
import org.apache.hadoop.hdfs.server.namenode.Constants.AnalysisState;
import org.apache.hadoop.hdfs.server.namenode.Constants.Endpoint;
import org.apache.hadoop.hdfs.server.namenode.Constants.Filter;
import org.apache.hadoop.hdfs.server.namenode.Constants.FilterOp;
import org.apache.hadoop.hdfs.server.namenode.Constants.Find;
import org.apache.hadoop.hdfs.server.namenode.Constants.Histogram;
import org.apache.hadoop.hdfs.server.namenode.Constants.HistogramOutput;
import org.apache.hadoop.hdfs.server.namenode.Constants.INodeSet;
import org.apache.hadoop.hdfs.server.namenode.Constants.Operation;
import org.apache.hadoop.hdfs.server.namenode.Constants.Sum;
import org.apache.hadoop.hdfs.server.namenode.Constants.Transform;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLoader;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImageWrapper;
import org.apache.hadoop.hdfs.server.namenode.analytics.security.SecurityContext;
import org.apache.hadoop.hdfs.server.namenode.analytics.sql.SqlParser;
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
public class WebServerMain implements ApplicationMain {

  public static final Logger LOG = LoggerFactory.getLogger(WebServerMain.class.getName());

  private final NameNodeLoader nameNodeLoader = new NameNodeLoader();
  private final HsqlDriver hsqlDriver = new HsqlDriver();
  private final TransferFsImageWrapper transferFsImage = new TransferFsImageWrapper(nameNodeLoader);
  private final List<BaseQuery> runningQueries = Collections.synchronizedList(new LinkedList<>());
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final SecurityContext secContext = new SecurityContext();
  private final UsageMetrics usageMetrics = new UsageMetrics();

  private final ExecutorService operationService = Executors.newFixedThreadPool(1);
  private final ExecutorService internalService = Executors.newFixedThreadPool(2);
  private final Map<String, BaseOperation> runningOperations =
      Collections.synchronizedMap(new HashMap<>());

  private final AtomicBoolean savingNamespace = new AtomicBoolean(false);

  /**
   * This is the main launching call for use in production. Should not accept any arguments. Service
   * is dictated by configuration files.
   *
   * @param args main argument although its not used
   */
  public static void main(String[] args) {
    try {
      ApplicationMain main = new WebServerMain();
      ApplicationConfiguration conf = new ApplicationConfiguration();
      main.init(conf);
    } catch (Throwable e) {
      LOG.error("FATAL", e);
    }
  }

  @Override // ApplicationMain
  @VisibleForTesting
  public NameNodeLoader getLoader() {
    return nameNodeLoader;
  }

  @Override // ApplicationMain
  public void init(ApplicationConfiguration conf) throws Exception {
    init(conf, null);
  }

  @Override // ApplicationMain
  @VisibleForTesting
  public void init(ApplicationConfiguration conf, GSet<INode, INodeWithAdditionalFields> inodes)
      throws Exception {
    init(conf, inodes, null);
  }

  /**
   * Main method of initializing an NNA instance. Web server will start first. Then INodes will
   * load.
   *
   * @param conf the application configuration
   * @param inodes preloaded inodes (used by tests)
   * @param preloadedHadoopConf preloaded hadoop configuration (used by tests)
   * @throws Exception errors here will crash NNA
   */
  @Override // ApplicationMain
  @VisibleForTesting
  public void init(
      ApplicationConfiguration conf,
      GSet<INode, INodeWithAdditionalFields> inodes,
      Configuration preloadedHadoopConf)
      throws Exception {
    String sslKeystorePath = conf.getSslKeystorePath();
    String sslKeystorePassword = conf.getSslKeystorePassword();
    if (sslKeystorePath == null && sslKeystorePassword == null) {
      LOG.info("Running web server in HTTP mode.");
    } else if (sslKeystorePath != null && sslKeystorePassword != null) {
      LOG.info("Running web server in HTTPS mode.");
      Spark.secure(sslKeystorePath, sslKeystorePassword, null, null);
    } else {
      throw new IllegalStateException(
          "Illegal SSL configuration. Check config/application.properties file.");
    }

    boolean ldapEnabled = conf.getLdapEnabled();
    boolean localUsersEnabled = !conf.getLocalOnlyUsers().isEmpty();
    Spark.port(conf.getPort());
    if (ldapEnabled) {
      LOG.info("Enabling LDAP web authentication.");
      // jwt:
      SignatureConfiguration sigConf =
          new SecretSignatureConfiguration(conf.getJwtSignatureSecret());
      EncryptionConfiguration encConf =
          new SecretEncryptionConfiguration(
              conf.getJwtEncryptionSecret(), JWEAlgorithm.DIR, EncryptionMethod.A128GCM);
      final JwtGenerator<CommonProfile> jwtGenerator = new JwtGenerator<>(sigConf, encConf);
      final JwtAuthenticator jwtAuthenticator = new JwtAuthenticator(sigConf, encConf);

      // ldaptive:
      ConnectionConfig connectionConfig = new ConnectionConfig();
      KeyStoreCredentialConfig keyStoreCredentialConfig = new KeyStoreCredentialConfig();
      keyStoreCredentialConfig.setTrustStore(conf.getLdapTrustStorePath());
      keyStoreCredentialConfig.setTrustStorePassword(conf.getLdapTruststorePassword());
      connectionConfig.setUseStartTLS(conf.getLdapUseStartTls());
      connectionConfig.setUseSSL(true);
      connectionConfig.setSslConfig(new SslConfig(keyStoreCredentialConfig));
      connectionConfig.setConnectTimeout(conf.getLdapConnectTimeout());
      connectionConfig.setResponseTimeout(conf.getLdapResponseTimeout());
      connectionConfig.setLdapUrl(conf.getLdapUrl());
      DefaultConnectionFactory connectionFactory = new DefaultConnectionFactory();
      connectionFactory.setConnectionConfig(connectionConfig);
      PoolConfig poolConfig = new PoolConfig();
      poolConfig.setMinPoolSize(conf.getLdapConnectionPoolMinSize());
      poolConfig.setMaxPoolSize(conf.getLdapConnectionPoolMaxSize());
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
      secContext.init(conf, jwtAuthenticator, jwtGenerator, ldapAuth);
    } else if (localUsersEnabled) {
      LOG.info("Enabling Local-Only-User web authentication.");
      // jwt:
      SignatureConfiguration sigConf =
          new SecretSignatureConfiguration(conf.getJwtSignatureSecret());
      EncryptionConfiguration encConf =
          new SecretEncryptionConfiguration(
              conf.getJwtEncryptionSecret(), JWEAlgorithm.DIR, EncryptionMethod.A128GCM);
      JwtGenerator<CommonProfile> jwtGenerator = new JwtGenerator<>(sigConf, encConf);
      JwtAuthenticator jwtAuthenticator = new JwtAuthenticator(sigConf, encConf);
      secContext.init(conf, jwtAuthenticator, jwtGenerator, null);
    } else {
      LOG.info("Disabling web authentication.");
      secContext.init(conf, null, null, null);
    }

    if (conf.getAuthorizationEnabled()) {
      LOG.info("Enabling web authorization.");
    } else {
      LOG.info("Disabling web authorization.");
    }

    /* This is the call to load everything under ./resources/public as HTML resources. */
    Spark.externalStaticFileLocation(conf.getWebBaseDir());

    /* LOGIN is used to log into authenticated web sessions. */
    post(
        "/login",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          secContext.login(req, res);
          usageMetrics.userLoggedIn(secContext, req);
          return res;
        });

    /* LOGOUT is used to log out of authenticated web sessions. */
    post(
        "/logout",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          secContext.logout(req, res);
          usageMetrics.userLoggedOut(secContext, req);
          return res;
        });

    /* ENDPOINTS endpoint is meant to showcase all available REST API endpoints in JSON list form. */
    get(
        "/endpoints",
        (req, res) -> {
          Helper.toJsonList(res.raw(), Endpoint.values());
          return res;
        });

    /* LOADINGSTATUS endpoint is meant to show the loading status of the NNA instance in JSON form. */
    get(
        "/loadingStatus",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          nameNodeLoader.sendLoadingStatus(res.raw());
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
          OperatingSystemMXBean systemMxBean =
              (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
          RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();
          MemoryMXBean mb = ManagementFactory.getMemoryMXBean();
          MemoryUsage memNonHeap = mb.getNonHeapMemoryUsage();
          MemoryUsage memHeap = mb.getHeapMemoryUsage();

          StringBuilder sb = new StringBuilder();
          int availableCores = runtime.availableProcessors();
          double cpuLoad = systemMxBean.getSystemCpuLoad();
          double procLoad = systemMxBean.getProcessCpuLoad();

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

          boolean isInit = nameNodeLoader.isInit();
          boolean isHistorical = nameNodeLoader.isHistorical();
          boolean isProvidingSuggestions = nameNodeLoader.getSuggestionsEngine().isLoaded();
          sb.append("Current system time (ms): ").append(Time.now()).append("\n");
          sb.append("Ready to service queries: ").append(isInit).append("\n");
          sb.append("Ready to service history: ").append(isHistorical).append("\n");
          sb.append("Ready to service suggestions: ").append(isProvidingSuggestions).append("\n\n");
          AnalysisState currentAnalysisState =
              nameNodeLoader.getSuggestionsEngine().getCurrentState();
          int analysisStateSetSize = AnalysisState.values().length;
          sb.append("Current suggestion analysis state: ")
              .append(nameNodeLoader.getSuggestionsEngine().getCurrentState().name())
              .append("\n");
          sb.append("Current suggestion analysis percentage: ")
              .append(((currentAnalysisState.ordinal() * 100.0f) / analysisStateSetSize))
              .append("%")
              .append("\n\n");
          if (isInit) {
            long allSetSize = nameNodeLoader.getINodeSet(INodeSet.all.name()).size();
            long fileSetSize = nameNodeLoader.getINodeSet(INodeSet.files.name()).size();
            long dirSetSize = nameNodeLoader.getINodeSet(INodeSet.dirs.name()).size();
            sb.append("Current TxID: ").append(nameNodeLoader.getCurrentTxId()).append("\n");
            sb.append("Analysis TxID: ")
                .append(nameNodeLoader.getSuggestionsEngine().getTransactionCount())
                .append("\n");
            sb.append("Analysis TxID delta: ")
                .append(nameNodeLoader.getSuggestionsEngine().getTransactionCountDiff())
                .append("\n");
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
          Set<String> dirs = nameNodeLoader.getSuggestionsEngine().getDirectoriesForAnalysis();
          sb.append("Cached directories size: ").append(dirs.size());
          for (String dir : dirs) {
            sb.append("\n").append(dir);
          }
          sb.append("\n\n");
          sb.append("Cached queries for analysis::\n");
          Map<String, String> queries =
              nameNodeLoader.getSuggestionsEngine().getQueriesForAnalysis();
          sb.append("Cached queries size: ").append(queries.size()).append("\n");
          for (Entry<String, String> queryEntry : queries.entrySet()) {
            sb.append(queryEntry.getKey()).append(" : ").append(queryEntry.getValue()).append("\n");
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
          long[] threadIds = tb.getAllThreadIds();
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
              default:
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
            return nameNodeLoader.getConfigValue(key);
          } else {
            res.header("Content-Type", "application/xml; charset=UTF-8");
            nameNodeLoader.dumpConfig(res.raw());
            return res;
          }
        });

    /* REFRESH endpoint is an admin endpoint meant to change the set of read, write, and admin users. */
    get(
        "/refresh",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          secContext.refresh(new ApplicationConfiguration());
          res.body(secContext.toString());
          return res;
        });

    /* CREDENTIALS endpoint is meant to showcase what authorizations the querying user possesses in JSON form. */
    get(
        "/credentials",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          if (secContext.isAuthenticationEnabled()) {
            res.status(HttpStatus.SC_OK);
          } else {
            res.status(HttpStatus.SC_BAD_REQUEST);
          }
          Helper.toJsonList(res.raw(), secContext.getAccessLevels());
          return res;
        });

    /* All security and query tracking should be done prior to accessing endpoints. */
    before(
        (req, res) -> {
          secContext.handleAuthentication(req, res);
          secContext.handleAuthorization(req, res);
          if (!"POST".equals(req.raw().getMethod())
              || req.raw().getRequestURI().contains(Constants.Endpoint.sql.name())) {
            runningQueries.add(Helper.createQuery(req.raw(), secContext.getUserName()));
            usageMetrics.userMadeQuery(secContext, req);
          }
        });

    /* METRICS endpoint is meant to return information on the users and the
    amount of queries they are making */
    get(
        "/metrics",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          res.body(usageMetrics.getUserMetricsJson());
          return res;
        });

    /* HISTOGRAMS endpoint is meant to showcase the different types of histograms available in the "&type="
    parameter in JSON form. */
    get(
        "/histograms",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          Helper.toJsonList(res.raw(), Histogram.values());
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
          Helper.toJsonList(res.raw(), HistogramOutput.values());
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
          Helper.toJsonList(res.raw(), Filter.values());
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
          Helper.toJsonList(res.raw(), Find.values());
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
          Helper.toJsonList(res.raw(), Transform.values());
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
          Helper.toJsonList(res.raw(), INodeSet.values());
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
          Helper.toJsonList(res.raw(), FilterOp.values());
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
          Helper.toJsonList(res.raw(), Sum.values());
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
          Helper.toJsonList(res.raw(), Operation.values());
          return res;
        });

    /* DUMP endpoint is for dumping an INode path's information in PLAINTEXT form. */
    get(
        "/dump",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json; charset=UTF-8");
          if (!nameNodeLoader.isInit()) {
            return "Namesystem is not fully initialized.\n";
          }
          String path = req.queryMap("path").value();
          nameNodeLoader.dumpINodeInDetail(path, res.raw());
          return res;
        });

    /* DIVIDE endpoint takes 2 sets of "set", "filter", "sum" parameters and returns the result of
    the filtered set "1" divided by filtered set "2" in PLAINTEXT form. */
    get(
        "/divide",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          if (!nameNodeLoader.isInit()) {
            return "";
          }

          lock.writeLock().lock();
          try {
            String filterStr1 = req.queryMap("filters1").value();
            String filterStr2 = req.queryMap("filters2").value();
            String emailsToStr = req.queryMap("emailTo").value();
            String emailsCcStr = req.queryMap("emailCC").value();
            String emailFrom = req.queryMap("emailFrom").value();
            String emailHost = req.queryMap("emailHost").value();
            String emailConditionsStr = req.queryMap("emailConditions").value();
            String[] filters1 = Helper.parseFilters(filterStr1);
            String[] filterOps1 = Helper.parseFilterOps(filterStr1);
            String[] filters2 = Helper.parseFilters(filterStr2);
            String[] filterOps2 = Helper.parseFilterOps(filterStr2);
            String[] emailsTo = (emailsToStr != null) ? emailsToStr.split(",") : null;
            String[] emailsCc = (emailsCcStr != null) ? emailsCcStr.split(",") : null;
            String set1 = req.queryMap("set1").value();
            String set2 = req.queryMap("set2").value();
            String sumStr1 = req.queryMap("sum1").value();
            String sum1 = (sumStr1 != null) ? sumStr1 : "count";
            String sumStr2 = req.queryMap("sum2").value();
            String sum2 = (sumStr2 != null) ? sumStr2 : "count";
            QueryChecker.isValidQuery(set1, filters1, null, sum1, filterOps1, null);
            QueryChecker.isValidQuery(set2, filters2, null, sum2, filterOps2, null);

            Collection<INode> inodes1 =
                Helper.performFilters(nameNodeLoader, set1, filters1, filterOps1);
            Collection<INode> inodes2 =
                Helper.performFilters(nameNodeLoader, set2, filters2, filterOps2);

            if (!sum1.isEmpty() && !sum2.isEmpty()) {
              long sumValue1 = nameNodeLoader.getQueryEngine().sum(inodes1, sum1);
              long sumValue2 = nameNodeLoader.getQueryEngine().sum(inodes2, sum2);
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
                String subject = nameNodeLoader.getAuthority() + " | DIVISION";
                try {
                  if (emailConditionsStr != null) {
                    MailOutput.check(emailConditionsStr, (long) division, nameNodeLoader);
                  }
                  MailOutput.write(subject, message, emailHost, emailsTo, emailsCc, emailFrom);
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

    /* Filter endpoint takes 1 set of "set", "filter", "sum" / "limit" parameters and returns either
    the list of file paths that pass the filters or the summation of the INode fields that pass the filters
    in PLAINTEXT form. */
    /* TODO: Consider separating logic of "list of file paths" to /dump endpoint. */
    /* TODO: Move "&filterOps=" into API of "&filters=" by making filter triplets separated by ":". */
    get(
        "/filter",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          if (!nameNodeLoader.isInit()) {
            return "";
          }

          lock.writeLock().lock();
          try {
            String fullFilterStr = req.queryMap("filters").value();
            String emailsToStr = req.queryMap("emailTo").value();
            String emailsCcStr = req.queryMap("emailCC").value();
            String emailFrom = req.queryMap("emailFrom").value();
            String emailHost = req.queryMap("emailHost").value();
            String find = req.queryMap("find").value();
            String emailConditionsStr = req.queryMap("emailConditions").value();
            String[] filters = Helper.parseFilters(fullFilterStr);
            String[] filterOps = Helper.parseFilterOps(fullFilterStr);
            String[] emailsTo = (emailsToStr != null) ? emailsToStr.split(",") : null;
            String[] emailsCc = (emailsCcStr != null) ? emailsCcStr.split(",") : null;
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

            Collection<INode> filteredINodes =
                Helper.performFilters(nameNodeLoader, set, filters, filterOps, find);

            if (sums.length == 1 && sumStr != null) {
              String sum = sums[0];
              long sumValue = nameNodeLoader.getQueryEngine().sum(filteredINodes, sum);
              String message = String.valueOf(sumValue);
              if (emailsTo != null
                  && emailsTo.length != 0
                  && emailHost != null
                  && emailFrom != null) {
                String subject =
                    nameNodeLoader.getAuthority()
                        + " | "
                        + sum
                        + " | "
                        + set
                        + " | Filters: "
                        + fullFilterStr;
                try {
                  if (emailConditionsStr != null) {
                    MailOutput.check(emailConditionsStr, sumValue, nameNodeLoader);
                  }
                  MailOutput.write(subject, message, emailHost, emailsTo, emailsCc, emailFrom);
                } catch (Exception e) {
                  LOG.error("Failed to email output with exception.", e);
                }
              }
              LOG.info("Returning filter result: {}.", message);
              res.body(message);
            } else if (sums.length > 1) {
              StringBuilder message = new StringBuilder();
              for (String sum : sums) {
                long sumValue = nameNodeLoader.getQueryEngine().sum(filteredINodes, sum);
                message.append(sumValue).append("\n");
              }
              res.body(message.toString());
            } else {
              nameNodeLoader.getQueryEngine().dumpINodePaths(filteredINodes, limit, res.raw());
            }

            return res;
          } finally {
            lock.writeLock().unlock();
          }
        });

    /* Histogram endpoint takes 1 set of "set", "filter", "type", and  "sum" parameters and returns a histogram
    where the X-axis represents the "type" type and the Y-axis represents the "sum" type.
    Output types available dictated by "&histogramOutput=". Default is CHART form. */
    /* TODO: Consider separating logic of "list of file paths" to /dump endpoint. */
    /* TODO: Move "&filterOps=" into API of "&filters=" by making filter triplets separated by ":". */
    /* TODO: Consider renaming "type" parameter to something more meaningful. */
    get(
        "/histogram",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");

          if (!nameNodeLoader.isInit()) {
            res.header("Content-Type", "application/json");
            return Histograms.toChartJsJson(new HashMap<>(), "not_loaded", "", "");
          }

          lock.writeLock().lock();
          try {
            final String fullFilterStr = req.queryMap("filters").value();
            final String histogramConditionsStr = req.queryMap("histogramConditions").value();
            final String emailsToStr = req.queryMap("emailTo").value();
            final String emailsCcStr = req.queryMap("emailCC").value();
            final String emailFrom = req.queryMap("emailFrom").value();
            final String emailHost = req.queryMap("emailHost").value();
            final String emailConditionsStr = req.queryMap("emailConditions").value();
            final String[] filters = Helper.parseFilters(fullFilterStr);
            final String[] filterOps = Helper.parseFilterOps(fullFilterStr);
            final String histType = req.queryMap("type").value();
            final String set = req.queryMap("set").value();
            final Integer top = req.queryMap("top").integerValue();
            final Integer bottom = req.queryMap("bottom").integerValue();
            final String sumStr = req.queryMap("sum").value();
            final Boolean useLock = req.queryMap("useLock").booleanValue();
            final Boolean sortAscending = req.queryMap("sortAscending").booleanValue();
            final Boolean sortDescending = req.queryMap("sortDescending").booleanValue();
            final String sum = (sumStr != null) ? sumStr : "count";
            final String[] emailsTo = (emailsToStr != null) ? emailsToStr.split(",") : null;
            final String[] emailsCc = (emailsCcStr != null) ? emailsCcStr.split(",") : null;
            final String transformConditionsStr = req.queryMap("transformConditions").value();
            final String transformFieldsStr = req.queryMap("transformFields").value();
            final String transformOutputsStr = req.queryMap("transformOutputs").value();
            final Integer parentDirDepth = req.queryMap("parentDirDepth").integerValue();
            final String timeRangeStr = req.queryMap("timeRange").value();
            final String timeRange = (timeRangeStr != null) ? timeRangeStr : "weekly";
            final String outputTypeStr = req.queryMap("histogramOutput").value();
            final String outputType = (outputTypeStr != null) ? outputTypeStr : "chart";
            final String type = req.queryMap("type").value();
            final String find = req.queryMap("find").value();
            final Boolean rawTimestampsBool = req.queryMap("rawTimestamps").booleanValue();
            final boolean rawTimestamps = rawTimestampsBool != null ? rawTimestampsBool : false;

            QueryChecker.isValidQuery(set, filters, type, sum, filterOps, find);
            Stream<INode> filteredINodes =
                Helper.setFilters(nameNodeLoader, set, filters, filterOps);

            Map<String, Function<INode, Long>> transformMap =
                Transforms.getAttributeTransforms(
                    transformConditionsStr,
                    transformFieldsStr,
                    transformOutputsStr,
                    nameNodeLoader);

            final long startTime = System.currentTimeMillis();

            Map<String, Long> histogram;
            String binLabels;

            nameNodeLoader.namesystemWriteLock(useLock);
            try {
              HistogramInvoker histogramInvoker =
                  new HistogramInvoker(
                          nameNodeLoader.getQueryEngine(),
                          histType,
                          sum,
                          parentDirDepth,
                          timeRange,
                          find,
                          filteredINodes,
                          transformMap,
                          histogramConditionsStr,
                          top,
                          bottom,
                          sortAscending,
                          sortDescending)
                      .invoke();
              histogram = histogramInvoker.getHistogram();
              binLabels = histogramInvoker.getBinLabels();
            } finally {
              nameNodeLoader.namesystemWriteUnlock(useLock);
            }

            long endTime = System.currentTimeMillis();
            LOG.info("Performing histogram: {} took: {} ms.", histType, (endTime - startTime));

            // Email out.
            if (emailsTo != null
                && emailsTo.length != 0
                && emailHost != null
                && emailFrom != null) {
              String subject =
                  nameNodeLoader.getAuthority()
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
                  MailOutput.check(emailConditionsStr, histogram, highlightKeys, nameNodeLoader);
                }
                MailOutput.write(
                    subject, histogram, highlightKeys, emailHost, emailsTo, emailsCc, emailFrom);
              } catch (Exception e) {
                LOG.error("Failed to email output.", e);
              }
            }

            // Return final histogram to Web UI as output type.
            HistogramOutput output = HistogramOutput.valueOf(outputType);
            switch (output) {
              case chart:
                res.header("Content-Type", "application/json");
                return Histograms.toChartJsJson(
                    histogram, Helper.toTitle(histType, sum), Helper.toYAxis(sum), binLabels);
              case json:
                res.header("Content-Type", "application/json");
                return Histograms.toJson(histogram);
              case csv:
                res.header("Content-Type", "text/plain");
                return Histograms.toCsv(histogram, find, rawTimestamps);
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
    This differs from Histogram endpoint in that it can group by multiple fields in a single query. */
    get(
        "/histogram2",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");

          if (!nameNodeLoader.isInit()) {
            res.header("Content-Type", "application/json");
            return Histograms.toChartJsJson(new HashMap<>(), "not_loaded", "", "");
          }

          lock.writeLock().lock();
          try {
            final String fullFilterStr = req.queryMap("filters").value();
            final String[] filters = Helper.parseFilters(fullFilterStr);
            final String[] filterOps = Helper.parseFilterOps(fullFilterStr);
            final String set = req.queryMap("set").value();
            final String sum = req.queryMap("sum").value();
            final Boolean useLock = req.queryMap("useLock").booleanValue();
            final Integer parentDirDepth = req.queryMap("parentDirDepth").integerValue();
            final String outputTypeStr = req.queryMap("histogramOutput").value();
            final String timeRangeStr = req.queryMap("timeRange").value();
            final String timeRange = (timeRangeStr != null) ? timeRangeStr : "weekly";
            final String outputType = (outputTypeStr != null) ? outputTypeStr : "json";
            final String typeStr = req.queryMap("type").value();
            final String[] types = (typeStr != null) ? typeStr.split(",") : new String[0];

            for (String type : types) {
              QueryChecker.isValidQuery(set, filters, type, sum, filterOps, null);
            }

            Map<String, Map<String, Long>> histogram;
            final long startTime = System.currentTimeMillis();
            nameNodeLoader.namesystemWriteLock(useLock);
            try {
              HistogramTwoLevelInvoker histogramInvoker =
                  new HistogramTwoLevelInvoker(
                          nameNodeLoader.getQueryEngine(),
                          types[0],
                          types[1],
                          sum,
                          parentDirDepth,
                          timeRange,
                          nameNodeLoader.getINodeSet(set).parallelStream())
                      .invoke();
              histogram = histogramInvoker.getHistogram();
            } finally {
              nameNodeLoader.namesystemWriteUnlock(useLock);
            }

            long endTime = System.currentTimeMillis();
            LOG.info("Performing histogram2: {} took: {} ms.", typeStr, (endTime - startTime));

            // Return final histogram to Web UI as output type.
            HistogramOutput output = HistogramOutput.valueOf(outputType);
            switch (output) {
              case json:
                res.header("Content-Type", "application/json");
                return Histograms.toJson(histogram);
              case csv:
                res.header("Content-Type", "text/plain");
                return Histograms.twoLeveltoCsv(histogram);
              default:
                throw new IllegalArgumentException(
                    "Could not determine output type: "
                        + outputType
                        + ".\nPlease check /histogramOutputs for available histogram outputs.");
            }
          } finally {
            lock.writeLock().unlock();
          }
        });

    /* HISTOGRAM3 endpoint takes 1 set of "set", "filter", "type", and  "sum" parameters and returns a histogram
    where the X-axis represents the "type" type and the Y-axis represents the "sum" type.
    Output types available dictated by "&histogramOutput=". Default is CHART form.
    This differs from Histogram endpoint in that it can output multiple sums and values in a single query. */
    get(
        "/histogram3",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");

          if (!nameNodeLoader.isInit()) {
            res.header("Content-Type", "application/json");
            return Histograms.toChartJsJson(new HashMap<>(), "not_loaded", "", "");
          }

          lock.writeLock().lock();
          try {
            final String fullFilterStr = req.queryMap("filters").value();
            final String[] filters = Helper.parseFilters(fullFilterStr);
            final String[] filterOps = Helper.parseFilterOps(fullFilterStr);
            final String histType = req.queryMap("type").value();
            final String set = req.queryMap("set").value();
            final String sumStr = req.queryMap("sum").value();
            final Integer sortAscendingIndex = req.queryMap("sortAscendingIndex").integerValue();
            final Integer sortDescendingIndex = req.queryMap("sortDescendingIndex").integerValue();
            final String histogramConditionsStr = req.queryMap("histogramConditions").value();
            final Boolean useLock = req.queryMap("useLock").booleanValue();
            final String[] sums = (sumStr != null) ? sumStr.split(",") : new String[0];
            final Integer parentDirDepth = req.queryMap("parentDirDepth").integerValue();
            final String outputTypeStr = req.queryMap("histogramOutput").value();
            final String timeRangeStr = req.queryMap("timeRange").value();
            final String timeRange = (timeRangeStr != null) ? timeRangeStr : "weekly";
            final String outputType = (outputTypeStr != null) ? outputTypeStr : "json";
            final String type = req.queryMap("type").value();
            final String findStr = req.queryMap("find").value();
            final String[] finds = (findStr != null) ? findStr.split(",") : new String[0];

            for (String sum : sums) {
              QueryChecker.isValidQuery(set, filters, type, sum, filterOps, null);
            }
            for (String find : finds) {
              QueryChecker.isValidQuery(set, filters, type, null, filterOps, find);
            }
            Collection<INode> filteredINodes =
                Helper.performFilters(nameNodeLoader, set, filters, filterOps);

            List<Map<String, Long>> histograms = new ArrayList<>(sums.length + finds.length);

            final long startTime = System.currentTimeMillis();
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

              nameNodeLoader.namesystemWriteLock(useLock);
              try {
                HistogramInvoker histogramInvoker =
                    new HistogramInvoker(
                            nameNodeLoader.getQueryEngine(),
                            histType,
                            sum,
                            parentDirDepth,
                            timeRange,
                            find,
                            filteredINodes.parallelStream(),
                            null,
                            histogramConditionsStr,
                            null,
                            null,
                            null,
                            null)
                        .invoke();
                histogram = histogramInvoker.getHistogram();
              } finally {
                nameNodeLoader.namesystemWriteUnlock(useLock);
              }
              histograms.add(histogram);
            }

            Map<String, List<Long>> mergedHistogram =
                histograms
                    .parallelStream()
                    .flatMap(m -> m.entrySet().stream())
                    .collect(
                        Collectors.groupingBy(
                            Entry::getKey,
                            Collector.of(
                                ArrayList::new,
                                (list, item) -> list.add(item.getValue()),
                                (left, right) -> {
                                  left.addAll(right);
                                  return left;
                                })));

            // Perform conditions filtering.
            if (histogramConditionsStr != null && !histogramConditionsStr.isEmpty()) {
              mergedHistogram =
                  nameNodeLoader
                      .getQueryEngine()
                      .removeKeysOnConditional2(mergedHistogram, histogramConditionsStr);
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
            LOG.info("Performing histogram3: {} took: {} ms.", histType, (endTime - startTime));

            // Return final histogram to Web UI as output type.
            HistogramOutput output = HistogramOutput.valueOf(outputType);
            switch (output) {
              case json:
                res.header("Content-Type", "application/json");
                return Histograms.toJson(mergedHistogram);
              case csv:
                res.header("Content-Type", "text/plain");
                return Histograms.toCsv(mergedHistogram);
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

    /* Filter endpoint takes 1 set of "set", "filter", "sum" / "limit" parameters and returns either
    the list of file paths that pass the filters or the summation of the INode fields that pass the filters
    in PLAINTEXT form. */
    /* TODO: Consider separating logic of "list of file paths" to /dump endpoint. */
    /* TODO: Move "&filterOps=" into API of "&filters=" by making filter triplets separated by ":". */
    get(
        "/submitOperation",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          if (!nameNodeLoader.isInit()) {
            return "";
          }

          lock.writeLock().lock();
          try {
            final String fullFilterStr = req.queryMap("filters").value();
            final String find = req.queryMap("find").value();
            final String[] filters = Helper.parseFilters(fullFilterStr);
            final String[] filterOps = Helper.parseFilterOps(fullFilterStr);
            final String set = req.queryMap("set").value();
            final String operation = req.queryMap("operation").value();
            Integer limit = req.queryMap("limit").integerValue();
            if (limit == null) {
              limit = Integer.MAX_VALUE;
            }
            Integer sleep = req.queryMap("sleep").integerValue();
            if (sleep == null) {
              sleep = Constants.DEFAULT_DELETE_SLEEP_MS;
            }

            if (operation == null || operation.isEmpty()) {
              throw new IllegalArgumentException("No operation defined. Please check /operations.");
            }
            QueryChecker.isValidQuery(set, filters, null, null, filterOps, find);

            Collection<INode> filteredINodes =
                Helper.performFilters(nameNodeLoader, set, filters, filterOps, find);
            if (filteredINodes.size() == 0) {
              LOG.info("Skipping operation request because it resulted in empty INode set.");
              throw new IOException(
                  "Skipping operation request because it resulted in empty INode set.");
            }

            FileSystem fs = nameNodeLoader.getFileSystem();
            String[] operationSplits = operation.split(":");
            String logBaseDir = conf.getBaseDir();
            BaseOperation operationObj;
            switch (operationSplits[0]) {
              case "delete":
                operationObj =
                    new Delete(
                        filteredINodes,
                        req.queryString(),
                        secContext.getUserName(),
                        logBaseDir,
                        fs);
                break;
              case "setReplication":
                short newReplFactor = Short.parseShort(operationSplits[1]);
                operationObj =
                    new SetReplication(
                        filteredINodes,
                        req.queryString(),
                        secContext.getUserName(),
                        logBaseDir,
                        fs,
                        newReplFactor);
                break;
              case "setStoragePolicy":
                operationObj =
                    new SetStoragePolicy(
                        filteredINodes,
                        req.queryString(),
                        secContext.getUserName(),
                        logBaseDir,
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
                          } catch (InterruptedException debugEx) {
                            LOG.debug("Operation sleep interrupted.", debugEx);
                          }
                          operationObj.performOp();
                        }
                        operationObj.close();
                      } catch (IllegalStateException e) {
                        operationObj.abort();
                        LOG.error("Aborted operation.", e);
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

    /* Filter endpoint takes 1 set of "set", "filter", "sum" / "limit" parameters and returns either
    the list of file paths that pass the filters or the summation of the INode fields that pass the filters
    in PLAINTEXT form. */
    /* TODO: Consider separating logic of "list of file paths" to /dump endpoint. */
    /* TODO: Move "&filterOps=" into API of "&filters=" by making filter triplets separated by ":". */
    get(
        "/listOperations",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          if (!nameNodeLoader.isInit()) {
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
              Set<Entry<String, BaseOperation>> entries = runningOperations.entrySet();
              StringBuilder sb = new StringBuilder();
              sb.append("Total Operations: ").append(entries.size()).append('\n');
              for (Entry<String, BaseOperation> entry : entries) {
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
                throw new FileNotFoundException("Operation not found.");
              }
              StringBuilder sb = new StringBuilder();
              sb.append("Identity: ");
              sb.append(operation.identity());
              sb.append('\n');
              sb.append("Owner: ");
              sb.append(operation.owner());
              sb.append('\n');
              sb.append("Type: ");
              sb.append(operation.type());
              sb.append('\n');
              sb.append("Query: ");
              sb.append(operation.query());
              sb.append('\n');
              sb.append("Total to perform: ");
              int totalToPerform = operation.totalToPerform();
              sb.append(totalToPerform);
              sb.append('\n');
              sb.append("Total performed: ");
              int numPerformed = operation.numPerformed();
              sb.append(numPerformed);
              sb.append('\n');
              sb.append("Total left to perform: ");
              sb.append(totalToPerform - numPerformed);
              sb.append('\n');
              sb.append("Percentage done: ");
              double percentageDone = ((double) numPerformed / (double) totalToPerform) * 100.0;
              sb.append(percentageDone);
              sb.append('\n');
              sb.append("Up next: ");
              sb.append(operation.upNext());
              sb.append('\n');
              sb.append("Last (");
              sb.append(limit);
              sb.append(") performed: ");
              List<String> lastDeleted = operation.lastPerformed(limit);
              Collections.reverse(lastDeleted);
              sb.append(lastDeleted.toString());
              sb.append('\n');
              res.body(sb.toString());
            }
            return res;
          } finally {
            lock.writeLock().unlock();
          }
        });

    /* Filter endpoint takes 1 set of "set", "filter", "sum" / "limit" parameters and returns either
    the list of file paths that pass the filters or the summation of the INode fields that pass the filters
    in PLAINTEXT form. */
    /* TODO: Consider separating logic of "list of file paths" to /dump endpoint. */
    /* TODO: Move "&filterOps=" into API of "&filters=" by making filter triplets separated by ":". */
    get(
        "/abortOperation",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");
          if (!nameNodeLoader.isInit()) {
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
              throw new FileNotFoundException("Operation not found.");
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
          boolean allJson = req.queryMap().toMap().containsKey("all");
          if (allJson) {
            return nameNodeLoader.getSuggestionsEngine().getAllSuggestionsAsJson();
          } else {
            String username = req.queryMap("username").value();
            return nameNodeLoader.getSuggestionsEngine().getSuggestionsAsJson(username);
          }
        });

    /* SUGGESTIONS endpoint is a cache-level endpoint meant to dump meta keys of cached analysis by NNA. */
    get(
        "/cachedMaps",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json");
          return Histograms.toJson(nameNodeLoader.getSuggestionsEngine().getCachedMapKeys());
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
          return nameNodeLoader.getSuggestionsEngine().getDirectoriesAsJson(directory, sum);
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
          return nameNodeLoader.getSuggestionsEngine().getFileAgeAsJson(sum);
        });

    /* ADDDIRECTORY endpoint is an admin-level endpoint meant to add a directory for cached analysis by NNA. */
    get(
        "/addDirectory",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          String directory = req.queryMap("dir").value();
          nameNodeLoader.getSuggestionsEngine().addDirectoryToAnalysis(directory);
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
          nameNodeLoader.getSuggestionsEngine().removeDirectoryFromAnalysis(directory);
          res.status(HttpStatus.SC_OK);
          res.body(directory + " removed from analysis.");
          return res;
        });

    /* SETCACHEDQUERY endpoint is an admin-level endpoint meant to add a query for cached analysis by NNA.
     * It can also be used to modify existing cached queries by supplying an existing queryName. */
    get(
        "/setCachedQuery",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          String queryName = req.queryMap("queryName").value();
          String query = req.queryString();
          nameNodeLoader.getSuggestionsEngine().setQueryToAnalysis(queryName, query);
          res.status(HttpStatus.SC_OK);
          res.body(queryName + " set for analysis.");
          return res;
        });

    /* REMOVECACHEDQUERY endpoint is an admin-level endpoint meant to remove a query from cached analysis by NNA. */
    get(
        "/removeCachedQuery",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          String queryName = req.queryMap("queryName").value();
          nameNodeLoader.getSuggestionsEngine().removeQueryFromAnalysis(queryName);
          res.status(HttpStatus.SC_OK);
          res.body(queryName + " removed from analysis.");
          return res;
        });

    /* GETCACHEDQUERY endpoint is an admin-level endpoint meant to add a query for cached analysis by NNA.
     * It can also be used to modify existing cached queries by supplying an existing queryName. */
    get(
        "/getCachedQuery",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          String queryName = req.queryMap("queryName").value();
          String body =
              nameNodeLoader.getSuggestionsEngine().getLatestCacheQueryResult(queryName, res.raw());
          res.status(HttpStatus.SC_OK);
          res.body(body);
          return res;
        });

    /* QUOTAS endpoint is an admin-level endpoint meant to dump the cached set of detected quotas by NNA. */
    get(
        "/quotas",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json");
          boolean allJson = req.queryMap().toMap().containsKey("all");
          String sum = req.queryMap("sum").value();
          if (allJson) {
            return nameNodeLoader.getSuggestionsEngine().getAllQuotasAsJson(sum);
          } else {
            String user = req.queryMap("user").value();
            return nameNodeLoader.getSuggestionsEngine().getQuotaAsJson(user, sum);
          }
        });

    /* USERS endpoint is an admin-level endpoint meant to dump the cached set of detected users by NNA. */
    get(
        "/users",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "application/json");
          final String suggestion = req.queryMap("suggestion").value();
          final Integer top = req.queryMap("top").integerValue();
          final String outputTypeStr = req.queryMap("histogramOutput").value();
          final String outputType = (outputTypeStr != null) ? outputTypeStr : "json";
          switch (outputType) {
            case "json":
              return nameNodeLoader.getSuggestionsEngine().getUsersAsJson(suggestion);
            case "csv":
              Map<String, Long> suggestionHistCsv =
                  nameNodeLoader.getSuggestionsEngine().getSuggestion(suggestion);
              if (top != null && top > 0) {
                suggestionHistCsv = Histograms.sliceToTop(suggestionHistCsv, top);
                suggestionHistCsv = Histograms.sortByValue(suggestionHistCsv, false);
              }
              return Histograms.toCsv(suggestionHistCsv, null, true);
            case "chart":
              Map<String, Long> suggestionHistChart =
                  nameNodeLoader.getSuggestionsEngine().getSuggestion(suggestion);
              if (top != null && top > 0) {
                suggestionHistChart = Histograms.sliceToTop(suggestionHistChart, top);
                suggestionHistChart = Histograms.sortByValue(suggestionHistChart, false);
              }
              return Histograms.toChartJsJson(suggestionHistChart, suggestion, "Username", "Count");
            default:
              throw new IllegalArgumentException("Unknown histogram output type:" + outputType);
          }
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
          return nameNodeLoader.getSuggestionsEngine().getIssuesAsJson(limit, false);
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
          return nameNodeLoader.getSuggestionsEngine().getIssuesAsJson(limit, true);
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
          return nameNodeLoader.getSuggestionsEngine().getTokens();
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
              nameNodeLoader.saveLegacyNamespace(dir);
            } else {
              nameNodeLoader.saveNamespace();
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
                "Done.<br />Please reload by going to `/reloadNamespace` "
                    + "or by `service nn-analytics restart` on command line.");
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
            nameNodeLoader.clear(true);
            nameNodeLoader.load(null, null, conf);
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
          nameNodeLoader.dumpLog(charsLimit, res.raw());
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

    /* SQL an experimental reader-level endpoint API for running queries on NNA using SQL-like syntax. */
    post(
        "/sql",
        (req, res) -> {
          res.header("Access-Control-Allow-Origin", "*");
          res.header("Content-Type", "text/plain");

          if (!nameNodeLoader.isInit()) {
            res.header("Content-Type", "application/json");
            return "not_loaded";
          }

          String sqlStatement = req.params("sqlStatement");
          if (Strings.isNullOrEmpty(sqlStatement)) {
            sqlStatement = req.queryParams("sqlStatement");
          }

          SqlParser sqlParser = new SqlParser();

          if (sqlStatement.toUpperCase().contains("SHOW TABLES")) {
            res.header("Content-Type", "application/json");
            return sqlParser.showTables();
          }
          if (sqlStatement.toUpperCase().contains("DESCRIBE")) {
            res.header("Content-Type", "application/json");
            return sqlParser.describeInJson(sqlStatement);
          }

          sqlParser.parse(sqlStatement);
          boolean isHistogram = !Strings.isNullOrEmpty(sqlParser.getType());
          boolean hasSumOrFind =
              !Strings.isNullOrEmpty(sqlParser.getSum())
                  || !Strings.isNullOrEmpty(sqlParser.getFind());

          String set = sqlParser.getINodeSet();
          String[] filters = Helper.parseFilters(sqlParser.getFilters());
          String[] filterOps = Helper.parseFilterOps(sqlParser.getFilters());
          String type = sqlParser.getType();
          String sum = sqlParser.getSum();
          String find = sqlParser.getFind();
          Integer limit = sqlParser.getLimit();
          int parentDirDepth = sqlParser.getParentDirDepth();
          String timeRange = sqlParser.getTimeRange();
          Boolean sortAscending = sqlParser.getSortAscending();
          Boolean sortDescending = sqlParser.getSortDescending();
          Integer top = (limit != null && sortDescending != null && sortDescending) ? limit : null;
          Integer bottom = (limit != null && sortAscending != null && sortAscending) ? limit : null;

          if (isHistogram) {
            QueryChecker.isValidQuery(set, filters, type, sum, filterOps, find);
            Stream<INode> filteredINodes =
                Helper.setFilters(nameNodeLoader, set, filters, filterOps);
            HistogramInvoker histogramInvoker =
                new HistogramInvoker(
                        nameNodeLoader.getQueryEngine(),
                        type,
                        sum,
                        parentDirDepth,
                        timeRange,
                        find,
                        filteredINodes,
                        null,
                        null,
                        top,
                        bottom,
                        sortAscending,
                        sortDescending)
                    .invoke();
            Map<String, Long> histogram = histogramInvoker.getHistogram();
            res.header("Content-Type", "application/json");
            return Histograms.toJson(histogram);
          } else {
            Collection<INode> filteredINodes =
                Helper.performFilters(nameNodeLoader, set, filters, filterOps, find);
            if (hasSumOrFind) {
              res.header("Content-Type", "plain/text");
              return nameNodeLoader.getQueryEngine().sum(filteredINodes, sum);
            } else {
              res.header("Content-Type", "plain/text");
              nameNodeLoader.getQueryEngine().dumpINodePaths(filteredINodes, limit, res.raw());
              return res;
            }
          }
        });

    /* Any query tracking should be removed once the query is completed. */
    after(
        (req, res) -> {
          res.header("Content-Encoding", "gzip");
          runningQueries.remove(Helper.createQuery(req.raw(), secContext.getUserName()));
        });

    /* Any encountered Exceptions should be handled here and returned with appropriate HTTP error codes. */
    exception(
        Exception.class,
        (ex, req, res) -> {
          if (ex instanceof AuthenticationException || ex instanceof BadCredentialsException) {
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
            runningQueries.remove(Helper.createQuery(req.raw(), secContext.getUserName()));
          } else if (ex instanceof FileNotFoundException) {
            res.header("Access-Control-Allow-Origin", "*");
            res.header("Content-Type", "text/plain");
            res.status(HttpStatus.SC_NOT_FOUND);
            res.body(ex.getMessage());
            runningQueries.remove(Helper.createQuery(req.raw(), secContext.getUserName()));
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
            } catch (IOException debugEx) {
              LOG.debug("Failed to send failure stacktrace.", debugEx);
            } finally {
              runningQueries.remove(Helper.createQuery(req.raw(), secContext.getUserName()));
            }
          }
          LOG.error("Exception encountered in system: ", ex);
          LOG.info(Arrays.toString(ex.getStackTrace()));
        });

    Spark.awaitInitialization();

    nameNodeLoader.initHistoryRecorder(hsqlDriver, conf, conf.getHistoricalEnabled());
    nameNodeLoader.load(inodes, preloadedHadoopConf, conf);
    nameNodeLoader.initReloadThreads(internalService, conf);
  }

  /**
   * Shutdown all stateful NNA objects. Should not kill JVM. Goal is that calling `init` again
   * should bring NNA back from persisted state.
   */
  @Override // ApplicationMain
  @VisibleForTesting
  public void shutdown() {
    try {
      hsqlDriver.dropConnection();
    } catch (Exception e) {
      LOG.error("Error during shutdown: ", e);
    }
    nameNodeLoader.clear(false);
    runningOperations.clear();
    runningQueries.clear();
    operationService.shutdown();
    try {
      internalService.awaitTermination(1L, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.debug("Internal service shutdown interrupted!", e);
    }
    Spark.stop();
  }
}
