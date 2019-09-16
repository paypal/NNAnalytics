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

import com.google.common.annotations.VisibleForTesting;
import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JWEAlgorithm;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLoader;
import org.apache.hadoop.hdfs.server.namenode.analytics.security.SecurityContext;
import org.apache.hadoop.hdfs.server.namenode.operations.BaseOperation;
import org.apache.hadoop.hdfs.server.namenode.queries.BaseQuery;
import org.apache.hadoop.util.GSet;
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

/**
 * This is the next generation of launching NNA.
 *
 * <p>The goal of this class is to launch NNA using only the native Hadoop dependencies (or as close
 * to possible).
 */
public class HadoopWebServerMain implements ApplicationMain {

  public static final Logger LOG = LoggerFactory.getLogger(HadoopWebServerMain.class.getName());

  private final ExecutorService internalService = Executors.newFixedThreadPool(2);
  private final ExecutorService operationService = Executors.newFixedThreadPool(1);
  private final Map<String, BaseOperation> runningOperations =
      Collections.synchronizedMap(new HashMap<>());

  private final List<BaseQuery> runningQueries = Collections.synchronizedList(new LinkedList<>());
  private final ReentrantReadWriteLock queryLock = new ReentrantReadWriteLock();
  private final AtomicBoolean savingNamespace = new AtomicBoolean(false);

  private final NameNodeLoader nameNodeLoader = new NameNodeLoader();
  private final HsqlDriver hsqlDriver = new HsqlDriver();

  private NameNodeAnalyticsHttpServer nnaHttpServer;

  /**
   * This is the main launching call for use in production. Should not accept any arguments. Service
   * is dictated by configuration files.
   *
   * @param args main argument although its not used
   * @throws InterruptedException InterruptedException
   * @throws IllegalAccessException IllegalAccessException
   * @throws NoSuchFieldException NoSuchFieldException
   */
  public static void main(String[] args) {
    try {
      HadoopWebServerMain main = new HadoopWebServerMain();
      ApplicationConfiguration conf = new ApplicationConfiguration();
      main.init(conf);
    } catch (Throwable e) {
      LOG.info("FATAL: {}", e);
    }
  }

  public void init(ApplicationConfiguration conf) throws Exception {
    init(conf, null);
  }

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
  @VisibleForTesting
  public void init(
      ApplicationConfiguration conf,
      GSet<INode, INodeWithAdditionalFields> inodes,
      Configuration preloadedHadoopConf)
      throws Exception {
    // Initialize classes needed for NNA.
    SecurityContext securityContext = loadSecurityContext(conf);
    UsageMetrics usageMetrics = new UsageMetrics();

    // Start NameNodeAnalytics HTTP server.
    nnaHttpServer =
        new NameNodeAnalyticsHttpServer(
            preloadedHadoopConf,
            conf,
            getHttpServerBindAddress(conf),
            securityContext,
            nameNodeLoader,
            hsqlDriver,
            usageMetrics,
            runningQueries,
            runningOperations,
            internalService,
            operationService,
            queryLock,
            savingNamespace);
    nnaHttpServer.start();

    // Bootstrap classes.
    nameNodeLoader.load(inodes, preloadedHadoopConf, conf);
    nameNodeLoader.initHistoryRecorder(hsqlDriver, conf, conf.getHistoricalEnabled());
    nameNodeLoader.initReloadThreads(internalService, conf);
  }

  private SecurityContext loadSecurityContext(ApplicationConfiguration nnaConf) {
    SecurityContext securityContext = new SecurityContext();

    boolean ldapEnabled = nnaConf.getLdapEnabled();
    boolean localUsersEnabled = !nnaConf.getLocalOnlyUsers().isEmpty();

    if (ldapEnabled) {
      LOG.info("Enabling LDAP web authentication.");
      // jwt:
      SignatureConfiguration sigConf =
          new SecretSignatureConfiguration(nnaConf.getJwtSignatureSecret());
      EncryptionConfiguration encConf =
          new SecretEncryptionConfiguration(
              nnaConf.getJwtEncryptionSecret(), JWEAlgorithm.DIR, EncryptionMethod.A128GCM);
      final JwtGenerator<CommonProfile> jwtGenerator = new JwtGenerator<>(sigConf, encConf);
      final JwtAuthenticator jwtAuthenticator = new JwtAuthenticator(sigConf, encConf);

      // ldaptive:
      ConnectionConfig connectionConfig = new ConnectionConfig();
      KeyStoreCredentialConfig keyStoreCredentialConfig = new KeyStoreCredentialConfig();
      keyStoreCredentialConfig.setTrustStore(nnaConf.getLdapTrustStorePath());
      keyStoreCredentialConfig.setTrustStorePassword(nnaConf.getLdapTruststorePassword());
      connectionConfig.setUseStartTLS(nnaConf.getLdapUseStartTls());
      connectionConfig.setUseSSL(true);
      connectionConfig.setSslConfig(new SslConfig(keyStoreCredentialConfig));
      connectionConfig.setConnectTimeout(nnaConf.getLdapConnectTimeout());
      connectionConfig.setResponseTimeout(nnaConf.getLdapResponseTimeout());
      connectionConfig.setLdapUrl(nnaConf.getLdapUrl());
      DefaultConnectionFactory connectionFactory = new DefaultConnectionFactory();
      connectionFactory.setConnectionConfig(connectionConfig);
      PoolConfig poolConfig = new PoolConfig();
      poolConfig.setMinPoolSize(nnaConf.getLdapConnectionPoolMinSize());
      poolConfig.setMaxPoolSize(nnaConf.getLdapConnectionPoolMaxSize());
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
      securityContext.init(nnaConf, jwtAuthenticator, jwtGenerator, ldapAuth);
    } else if (localUsersEnabled) {
      LOG.info("Enabling Local-Only-User web authentication.");
      // jwt:
      SignatureConfiguration sigConf =
          new SecretSignatureConfiguration(nnaConf.getJwtSignatureSecret());
      EncryptionConfiguration encConf =
          new SecretEncryptionConfiguration(
              nnaConf.getJwtEncryptionSecret(), JWEAlgorithm.DIR, EncryptionMethod.A128GCM);
      JwtGenerator<CommonProfile> jwtGenerator = new JwtGenerator<>(sigConf, encConf);
      JwtAuthenticator jwtAuthenticator = new JwtAuthenticator(sigConf, encConf);
      securityContext.init(nnaConf, jwtAuthenticator, jwtGenerator, null);
    } else {
      LOG.info("Disabling web authentication.");
      securityContext.init(nnaConf, null, null, null);
    }

    if (nnaConf.getAuthorizationEnabled()) {
      LOG.info("Enabling web authorization.");
    } else {
      LOG.info("Disabling web authorization.");
    }
    return securityContext;
  }

  private InetSocketAddress getHttpServerBindAddress(ApplicationConfiguration conf) {
    return new InetSocketAddress("0.0.0.0", conf.getPort());
  }

  @VisibleForTesting
  public NameNodeLoader getLoader() {
    return nameNodeLoader;
  }

  /**
   * Shutdown all stateful NNA objects. Should not kill JVM. Goal is that calling `init` again
   * should bring NNA back from persisted state.
   */
  @VisibleForTesting
  public void shutdown() {
    try {
      hsqlDriver.dropConnection();
    } catch (Exception e) {
      LOG.error("Error during hsql connection shutdown: ", e);
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
    if (nnaHttpServer != null) {
      try {
        nnaHttpServer.stop();
      } catch (Exception e) {
        LOG.error("Error during http server shutdown: ", e);
      }
    }
  }
}
