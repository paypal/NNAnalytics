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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.hdfs.server.namenode.JavaStreamQueryEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationConfiguration {

  public static final Logger LOG =
      LoggerFactory.getLogger(ApplicationConfiguration.class.getName());

  private static final String APP_PROPERTIES = "application.properties";
  @Deprecated private static final String SEC_PROPERTIES = "security.properties";
  private final Properties properties = new Properties();

  private static final String NNA_PORT_DEFAULT = "8080";
  private static final String NNA_HISTORICAL_DEFAULT = "false";
  private static final String LDAP_ENABLED_DEFAULT = "false";
  private static final String AUTHORIZATION_ENABLED_DEFAULT = "false";
  private static final String LDAP_USE_STARTTLS_DEFAULT = "false";
  private static final String LDAP_CONNECT_TIMEOUT_DEFAULT = "1000";
  private static final String LDAP_RESPONSE_TIMEOUT_DEFAULT = "1000";
  private static final String NNA_SUGGESTIONS_RELOAD_TIMEOUT_DEFAULT = "900000";
  private static final String NNA_BASE_DIR_DEFAULT = "/usr/local/nn-analytics";
  private static final String NNA_WEB_BASE_DIR_DEFAULT = NNA_BASE_DIR_DEFAULT + "/webapps/nna";
  private static final String NNA_SUPPORT_BOOTSTRAP_OVERRIDES = "true";
  private static final String NNA_BOOTSTRAP_AUTO_FETCH_NAMESPACE = "false";
  private static final String NNA_QUERY_ENGINE_DEFAULT =
      JavaStreamQueryEngine.class.getCanonicalName();

  /** Constructor. Fetches configuration from ClassLoader stream. */
  public ApplicationConfiguration() {
    InputStream oldProps = this.getClass().getClassLoader().getResourceAsStream(SEC_PROPERTIES);
    try {
      properties.load(oldProps);
      LOG.warn(
          "Loaded deprecated configuration, {}. Please rename to {}.",
          SEC_PROPERTIES,
          APP_PROPERTIES);
      return;
    } catch (Exception e) {
      LOG.warn(
          "Failed to load old properties file: {}, due to: {}. Ignore if you are using {}.",
          SEC_PROPERTIES,
          e,
          APP_PROPERTIES);
    }
    InputStream input = this.getClass().getClassLoader().getResourceAsStream(APP_PROPERTIES);
    try {
      properties.load(input);
    } catch (IOException e) {
      LOG.error("Failed to load properties file: {}, due to: {}", APP_PROPERTIES, e);
    }
  }

  public void set(String key, String value) {
    properties.setProperty(key, value);
  }

  public String getBaseDir() {
    return properties.getProperty("nna.base.dir", NNA_BASE_DIR_DEFAULT);
  }

  public String getWebBaseDir() {
    return properties.getProperty("nna.web.base.dir", NNA_WEB_BASE_DIR_DEFAULT);
  }

  public boolean getHistoricalEnabled() {
    return Boolean.parseBoolean(properties.getProperty("nna.historical", NNA_HISTORICAL_DEFAULT));
  }

  public boolean getAuthorizationEnabled() {
    return Boolean.parseBoolean(
        properties.getProperty("authorization.enable", AUTHORIZATION_ENABLED_DEFAULT));
  }

  public boolean getLdapEnabled() {
    return Boolean.parseBoolean(properties.getProperty("ldap.enable", LDAP_ENABLED_DEFAULT));
  }

  public String getLdapUrl() {
    return properties.getProperty("ldap.url");
  }

  public String getLdapTrustStorePath() {
    return properties.getProperty("ldap.trust.store.path");
  }

  public String getLdapTruststorePassword() {
    return properties.getProperty("ldap.trust.store.password");
  }

  /**
   * Get the set of LDAP Base Domain Name's that will be attempted for authentication.
   *
   * @return set of ldap base dn's as per configuration
   */
  public Set<String> getLdapBaseDn() {
    Set<String> result = new HashSet<>();
    String value;

    for (int i = 1; (value = properties.getProperty("ldap.base.dn." + i)) != null; i++) {
      result.add(value);
    }

    return result;
  }

  public boolean getLdapUseStartTls() {
    return Boolean.parseBoolean(
        properties.getProperty("ldap.use.starttls", LDAP_USE_STARTTLS_DEFAULT));
  }

  public int getLdapConnectTimeout() {
    return Integer.parseInt(
        properties.getProperty("ldap.connect.timeout", LDAP_CONNECT_TIMEOUT_DEFAULT));
  }

  public int getLdapResponseTimeout() {
    return Integer.parseInt(
        properties.getProperty("ldap.response.timeout", LDAP_RESPONSE_TIMEOUT_DEFAULT));
  }

  public int getLdapConnectionPoolMinSize() {
    return Integer.parseInt(properties.getProperty("ldap.connection.pool.min.size"));
  }

  public int getLdapConnectionPoolMaxSize() {
    return Integer.parseInt(properties.getProperty("ldap.connection.pool.max.size"));
  }

  /**
   * Get the interval in milliseconds per suggestions report to sleep for.
   *
   * @return integer representing milliseconds to in-between each report
   */
  public int getSuggestionsReloadSleepMs() {
    return Integer.parseInt(
        properties.getProperty(
            "nna.suggestions.reload.sleep.ms", NNA_SUGGESTIONS_RELOAD_TIMEOUT_DEFAULT));
  }

  public String getJwtSignatureSecret() {
    return properties.getProperty("jwt.signature.secret");
  }

  public String getJwtEncryptionSecret() {
    return properties.getProperty("jwt.encryption.secret");
  }

  /**
   * Get list of user names representing NNA admins.
   *
   * @return set of user names that are NNA admins
   */
  public Set<String> getAdminUsers() {
    return new HashSet<String>() {
      {
        Collections.addAll(this, properties.getProperty("nna.admin.users").split(","));
      }
    };
  }

  /**
   * Get list of user names representing NNA write users. These users may issue operations that
   * touch data on the live cluster.
   *
   * @return set of user names that are NNA writers
   */
  public Set<String> getWriteUsers() {
    return new HashSet<String>() {
      {
        Collections.addAll(this, properties.getProperty("nna.write.users").split(","));
      }
    };
  }

  /**
   * Get list of user names representing NNA read-only users. These users may run queries on NNA and
   * view all live metadata.
   *
   * @return set of user names that are NNA readers
   */
  public Set<String> getReadOnlyUsers() {
    return new HashSet<String>() {
      {
        Collections.addAll(this, properties.getProperty("nna.readonly.users").split(","));
      }
    };
  }

  /**
   * Get list of user names representing NNA cache-only users. These users may only view aggregated
   * reports that are cached by NNA.
   *
   * @return set of user names that are NNA cache-only readers
   */
  public Set<String> getCacheReaderUsers() {
    return new HashSet<String>() {
      {
        Collections.addAll(this, properties.getProperty("nna.cache.users").split(","));
      }
    };
  }

  /**
   * These are NNA local only accounts that can be used by outside applications. If you intend to
   * utilize local-only accounts then you must lock down the permissions on the .properties file as
   * it will contain passwords.
   *
   * @return map of username : password for locally maintained NNA users
   */
  public Map<String, String> getLocalOnlyUsers() {
    HashMap<String, String> localOnlyUsers = new HashMap<>();
    String property = properties.getProperty("nna.localonly.users");
    if (property != null && !property.isEmpty()) {
      try {
        String[] splits = property.split(",");
        for (String split : splits) {
          String[] usernamePassword = split.split(":");
          String username = usernamePassword[0];
          String password = (usernamePassword.length == 1) ? "" : usernamePassword[1];
          localOnlyUsers.put(username, password);
        }
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new IllegalArgumentException(
            "Please configure nna.local.users with user:password.", e);
      }
    }
    return localOnlyUsers;
  }

  public String getSslKeystorePath() {
    return properties.getProperty("ssl.keystore.path");
  }

  public String getSslKeystorePassword() {
    return properties.getProperty("ssl.keystore.password");
  }

  public int getPort() {
    return Integer.parseInt(properties.getProperty("nna.port", NNA_PORT_DEFAULT));
  }

  public String getHistoricalUsername() {
    return properties.getProperty("nna.historical.username", "root");
  }

  public String getHistoricalPassword() {
    return properties.getProperty("nna.historical.password", "root");
  }

  public boolean allowBootstrapConfigurationOverrides() {
    return Boolean.parseBoolean(
        properties.getProperty("nna.support.bootstrap.overrides", NNA_SUPPORT_BOOTSTRAP_OVERRIDES));
  }

  public String getQueryEngineImplementation() {
    return properties.getProperty("nna.query.engine.impl", NNA_QUERY_ENGINE_DEFAULT);
  }

  /** Returns true or false depending on if you configure NNA to auto-fetch on restart. */
  public boolean allowBootstrapAutomaticFetch() {
    return Boolean.parseBoolean(
        properties.getProperty(
            "nna.bootstrap.auto.fetch.namespace", NNA_BOOTSTRAP_AUTO_FETCH_NAMESPACE));
  }
}
