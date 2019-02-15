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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ADMIN;
import static org.apache.hadoop.hdfs.DFSUtil.getHttpPolicy;
import static org.apache.hadoop.hdfs.DFSUtil.getSpnegoKeytabKey;
import static org.apache.hadoop.hdfs.DFSUtil.loadSslConfToHttpServerBuilder;
import static org.apache.hadoop.hdfs.DFSUtil.loadSslConfiguration;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLoader;
import org.apache.hadoop.hdfs.server.namenode.analytics.security.SecurityConfiguration;
import org.apache.hadoop.hdfs.server.namenode.analytics.security.SecurityContext;
import org.apache.hadoop.hdfs.server.namenode.analytics.web.NamenodeAnalyticsMethods;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Encapsulates the HTTP server used by NameNodeAnalytics. */
@InterfaceAudience.Private
public class NameNodeAnalyticsHttpServer {

  public static final Logger LOG =
      LoggerFactory.getLogger(NameNodeAnalyticsHttpServer.class.getName());

  public static final String NNA_NN_LOADER = "nna.namenode.loader";
  public static final String NNA_SECURITY_CONTEXT = "nna.security.context";
  public static final String NNA_USAGE_METRICS = "nna.usage.metrics";
  public static final String NNA_HSQL_DRIVER = "nna.hsql.driver";

  // NNA fields.
  private final NameNodeLoader nnLoader;
  private final SecurityContext secContext;
  private final HsqlDriver hsqlDriver;
  private final UsageMetrics usageMetrics;

  // Jetty Http server.
  private HttpServer2 httpServer;

  // Configuration.
  private Configuration conf;
  private SecurityConfiguration nnaConf;

  // Hosting addresses.
  private InetSocketAddress httpAddress;
  private InetSocketAddress httpsAddress;
  private final InetSocketAddress bindAddress;

  /**
   * Constructor.
   *
   * @param conf hadoop configuration
   * @param nnaConf nna configuration
   * @param bindAddress address to bind http server to
   * @param securityContext nna security context
   * @param nameNodeLoader the NameNodeLoader
   * @param hsqlDriver the HSQL embedded DB driver
   * @param usageMetrics nna user usage metrics
   */
  public NameNodeAnalyticsHttpServer(
      Configuration conf,
      SecurityConfiguration nnaConf,
      InetSocketAddress bindAddress,
      SecurityContext securityContext,
      NameNodeLoader nameNodeLoader,
      HsqlDriver hsqlDriver,
      UsageMetrics usageMetrics) {
    this.conf = conf;
    this.nnaConf = nnaConf;
    this.bindAddress = bindAddress;
    this.nnLoader = nameNodeLoader;
    this.secContext = securityContext;
    this.hsqlDriver = hsqlDriver;
    this.usageMetrics = usageMetrics;
  }

  /**
   * @see DFSUtil#getHttpPolicy(org.apache.hadoop.conf.Configuration) for information related to the
   *     different configuration options and Http Policy is decided.
   */
  public void start() throws IOException {
    final int nnaPort = nnaConf.getPort();

    String sslKeystorePath = nnaConf.getSslKeystorePath();
    String sslKeystorePassword = nnaConf.getSslKeystorePassword();
    if (sslKeystorePath == null && sslKeystorePassword == null) {
      LOG.info("Running web server in HTTP mode.");
      httpAddress = new InetSocketAddress(bindAddress.getAddress(), nnaPort);
    } else if (sslKeystorePath != null && sslKeystorePassword != null) {
      LOG.info("Running web server in HTTPS mode.");
      httpAddress = new InetSocketAddress(bindAddress.getAddress(), nnaPort);
    } else {
      throw new IllegalStateException(
          "Illegal SSL configuration. Check config/security.properties file.");
    }

    if (conf == null) {
      conf = new Configuration();
      conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, "HTTP_ONLY");
    }

    HttpServer2.Builder builder =
        httpServerTemplateForNnAnalytics(
            conf,
            httpAddress,
            httpsAddress,
            "nna",
            DFSConfigKeys.DFS_NAMENODE_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
            DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY);
    builder.keyPassword(sslKeystorePassword).keyStore(sslKeystorePath, sslKeystorePassword, "jks");

    httpServer = builder.build();

    // Main load of NNA classes and API into NNA Http Server.
    httpServer.getWebAppContext().setAttribute(NNA_NN_LOADER, nnLoader);
    httpServer.getWebAppContext().setAttribute(NNA_SECURITY_CONTEXT, secContext);
    httpServer.getWebAppContext().setAttribute(NNA_USAGE_METRICS, usageMetrics);
    httpServer.getWebAppContext().setAttribute(NNA_HSQL_DRIVER, hsqlDriver);

    // NNA Rest API hosted at /* and web-pages served from /* as well.
    httpServer.addJerseyResourcePackage(
        NamenodeAnalyticsMethods.class.getPackage().getName(), "/*");

    httpServer.start();

    LOG.info("NNA is up and servicing at port: " + nnaPort);
  }

  /** Joins the httpserver. */
  public void join() throws InterruptedException {
    if (httpServer != null) {
      httpServer.join();
    }
  }

  /** Stops the httpserver. */
  public void stop() throws Exception {
    try {
      hsqlDriver.dropConnection();
    } catch (Exception e) {
      LOG.error("Error during shutdown: ", e);
    }
    if (nnLoader != null) {
      //nnLoader.unload();
    }
    if (httpServer != null) {
      httpServer.stop();
    }
  }

  InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  InetSocketAddress getHttpsAddress() {
    return httpsAddress;
  }

  /**
   * Returns the httpServer.
   *
   * @return HttpServer2
   */
  @VisibleForTesting
  public HttpServer2 getHttpServer() {
    return httpServer;
  }

  private static HttpServer2.Builder httpServerTemplateForNnAnalytics(
      Configuration conf,
      final InetSocketAddress httpAddr,
      final InetSocketAddress httpsAddr,
      String name,
      String spnegoUserNameKey,
      String spnegoKeytabFileKey)
      throws IOException {
    HttpConfig.Policy policy = getHttpPolicy(conf);

    HttpServer2.Builder builder =
        new HttpServer2.Builder()
            .setName(name)
            .setConf(conf)
            .setACL(new AccessControlList(conf.get(DFS_ADMIN, " ")))
            .setSecurityEnabled(UserGroupInformation.isSecurityEnabled())
            .setUsernameConfKey(spnegoUserNameKey)
            .setKeytabConfKey(getSpnegoKeytabKey(conf, spnegoKeytabFileKey));

    // initialize the webserver for uploading/downloading files.
    if (UserGroupInformation.isSecurityEnabled()) {
      LOG.info(
          "Starting web server as: "
              + SecurityUtil.getServerPrincipal(
                  conf.get(spnegoUserNameKey), httpAddr.getHostName()));
    }

    if (policy.isHttpEnabled()) {
      if (httpAddr.getPort() == 0) {
        builder.setFindPort(true);
      }

      URI uri = URI.create("http://" + NetUtils.getHostPortString(httpAddr));
      builder.addEndpoint(uri);
      LOG.info("Starting Web-server for " + name + " at: " + uri);
    }

    if (policy.isHttpsEnabled() && httpsAddr != null) {
      Configuration sslConf = loadSslConfiguration(conf);
      loadSslConfToHttpServerBuilder(builder, sslConf);

      if (httpsAddr.getPort() == 0) {
        builder.setFindPort(true);
      }

      URI uri = URI.create("https://" + NetUtils.getHostPortString(httpsAddr));
      builder.addEndpoint(uri);
      LOG.info("Starting Web-server for " + name + " at: " + uri);
    }
    return builder;
  }
}
