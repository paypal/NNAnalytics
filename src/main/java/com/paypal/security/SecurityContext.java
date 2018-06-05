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

package com.paypal.security;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Set;
import org.apache.hadoop.hdfs.server.namenode.NNAConstants;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.ldaptive.auth.FormatDnResolver;
import org.pac4j.core.credentials.UsernamePasswordCredentials;
import org.pac4j.core.exception.HttpAction;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.jwt.credentials.authenticator.JwtAuthenticator;
import org.pac4j.jwt.profile.JwtGenerator;
import org.pac4j.ldap.credentials.authenticator.LdapAuthenticator;
import org.pac4j.sparkjava.SparkWebContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;

public class SecurityContext {

  public static final Logger LOG = LoggerFactory.getLogger(SecurityContext.class.getName());

  private SecurityConfiguration securityConfiguration;
  private JwtAuthenticator jwtAuthenticator;
  private JwtGenerator<CommonProfile> jwtGenerator;
  private LdapAuthenticator ldapAuthenticator;

  private UserSet adminUsers;
  private UserSet writeUsers;
  private UserSet readOnlyUsers;
  private UserSet cacheReaderUsers;
  private UserPasswordSet localOnlyUsers;

  private boolean init = false;

  private static final ThreadLocal<String> currentUser =
      ThreadLocal.withInitial(() -> "default_unsecured_user");

  private enum ACCESS_LEVEL {
    ADMIN,
    WRITER,
    READER,
    CACHE
  }

  public SecurityContext() {}

  public void init(
      SecurityConfiguration secConf,
      JwtAuthenticator jwtAuth,
      JwtGenerator<CommonProfile> jwtGen,
      LdapAuthenticator ldapAuthenticator) {
    this.securityConfiguration = secConf;
    this.jwtAuthenticator = jwtAuth;
    this.jwtGenerator = jwtGen;
    this.ldapAuthenticator = ldapAuthenticator;

    this.adminUsers = new UserSet(secConf.getAdminUsers());
    this.writeUsers = new UserSet(secConf.getWriteUsers());
    this.readOnlyUsers = new UserSet(secConf.getReadOnlyUsers());
    this.cacheReaderUsers = new UserSet(secConf.getCacheReaderUsers());
    this.localOnlyUsers = new UserPasswordSet(secConf.getLocalOnlyUsers());

    this.init = true;
  }

  public synchronized void refresh(SecurityConfiguration secConf) {
    this.adminUsers = new UserSet(secConf.getAdminUsers());
    this.writeUsers = new UserSet(secConf.getWriteUsers());
    this.readOnlyUsers = new UserSet(secConf.getReadOnlyUsers());
    this.cacheReaderUsers = new UserSet(secConf.getCacheReaderUsers());
    this.localOnlyUsers = new UserPasswordSet(secConf.getLocalOnlyUsers());
  }

  public void handleAuthentication(Request req, Response res)
      throws AuthenticationException, HttpAction {
    boolean authenticationEnabled = securityConfiguration.getLdapEnabled();
    if (!authenticationEnabled) {
      String reqUsername = req.queryParams("proxy");
      if (reqUsername != null && !reqUsername.isEmpty()) {
        currentUser.set(reqUsername);
      }
      return;
    }
    if (!init) {
      LOG.info("Request occurred before initialized from: {}", req.ip());
      throw new AuthenticationException("Please wait for initialization.");
    }

    String login = req.headers("Authorization");
    String token = req.cookie("nna-jwt-token");
    ProfileManager<CommonProfile> manager = new ProfileManager<>(new SparkWebContext(req, res));
    CommonProfile userProfile;
    if (token != null) {
      try {
        userProfile = jwtAuthenticator.validateToken(token);

        userProfile.removeAttribute("iat");
        String generate = jwtGenerator.generate(userProfile);
        res.header("Set-Cookie", "nna-jwt-token=" + generate);

        manager.save(true, userProfile, false);
        LOG.info("Login success via [TOKEN] for: {}", req.ip());

        currentUser.set(userProfile.getId());
      } catch (Exception e) {
        LOG.info("Login failed via [TOKEN] for: {}", req.ip());
        throw new AuthenticationException(e);
      }
    } else if (login != null && login.startsWith("Basic ")) {
      String b64Credentials = login.substring("Basic ".length()).trim();
      String nameAndPassword =
          new String(Base64.getDecoder().decode(b64Credentials), Charset.defaultCharset());
      String[] split = nameAndPassword.split(":");
      if (split.length != 2) {
        LOG.info("Login failed via [BASIC] for: {}", req.ip());
        throw new AuthenticationException(
            "Bad username / password provided. Should be username:password.");
      }
      Set<String> ldapBaseDns = securityConfiguration.getLdapBaseDn();

      UsernamePasswordCredentials credentials = null;
      RuntimeException authFailedEx = null;
      String user = split[0];
      String password = split[1];

      if (localOnlyUsers.allows(user)) {
        if (localOnlyUsers.authenticate(user, password)) {
          LOG.info("Login success via [LOCAL] for: {}", req.ip());
          currentUser.set(user);
          return;
        } else {
          LOG.info("Login failed via [LOCAL] for: {}", req.ip());
          throw new AuthenticationException("Authentication required.");
        }
      }

      for (String ldapBaseDn : ldapBaseDns) {
        String ldapDnRegexd = ldapBaseDn.replaceAll("%u", user);
        ldapAuthenticator.getLdapAuthenticator().setDnResolver(new FormatDnResolver(ldapDnRegexd));
        credentials = new UsernamePasswordCredentials(user, password, req.ip());

        try {
          ldapAuthenticator.validate(credentials, new SparkWebContext(req, res));
        } catch (RuntimeException e) {
          authFailedEx = e;
          continue;
        }

        authFailedEx = null;
        break;
      }

      if (authFailedEx != null) {
        LOG.info("Login failed via [BASIC] for: {}", req.ip());
        throw authFailedEx;
      }

      assert credentials != null;
      userProfile = credentials.getUserProfile();
      String generate = jwtGenerator.generate(userProfile);
      res.header("Set-Cookie", "nna-jwt-token=" + generate);

      manager.save(true, userProfile, false);
      LOG.info("Login success via [BASIC] for: {}", req.ip());

      currentUser.set(userProfile.getId());
    } else {
      LOG.info("Login failed via [NULL] for: {}", req.ip());
      throw new AuthenticationException("Authentication required.");
    }
  }

  public synchronized void handleAuthorization(Request req, Response res)
      throws AuthenticationException, HttpAction, AuthorizationException {
    boolean authorizationEnabled = securityConfiguration.getAuthorizationEnabled();
    if (!authorizationEnabled) {
      return;
    }
    String user = getUserName();
    String uri = req.raw().getRequestURI();
    for (NNAConstants.ENDPOINT unsecured : NNAConstants.UNSECURED_ENDPOINTS) {
      if (uri.startsWith("/" + unsecured.name())) {
        return;
      }
    }
    for (NNAConstants.ENDPOINT admins : NNAConstants.ADMIN_ENDPOINTS) {
      if (uri.startsWith("/" + admins.name())) {
        if (adminUsers.allows(user)) {
          return;
        } else {
          throw new AuthorizationException("User: " + user + ", is not authorized for URI: " + uri);
        }
      }
    }
    for (NNAConstants.ENDPOINT writers : NNAConstants.WRITER_ENDPOINTS) {
      if (uri.startsWith("/" + writers.name())) {
        if (writeUsers.allows(user)) {
          return;
        } else {
          throw new AuthorizationException("User: " + user + ", is not authorized for URI: " + uri);
        }
      }
    }
    for (NNAConstants.ENDPOINT readers : NNAConstants.READER_ENDPOINTS) {
      if (uri.startsWith("/" + readers.name())) {
        if (readOnlyUsers.allows(user)) {
          return;
        } else {
          throw new AuthorizationException("User: " + user + ", is not authorized for URI: " + uri);
        }
      }
    }
    for (NNAConstants.ENDPOINT cacheReaders : NNAConstants.CACHE_READER_ENDPOINTS) {
      if (uri.startsWith("/" + cacheReaders.name())) {
        if (cacheReaderUsers.allows(user)) {
          return;
        } else {
          throw new AuthorizationException("User: " + user + ", is not authorized for URI: " + uri);
        }
      }
    }
    throw new AuthorizationException("User: " + user + ", is not authorized for URI: " + uri);
  }

  public String getUserName() {
    return currentUser.get();
  }

  public synchronized Enum[] getAccessLevels() {
    String username = currentUser.get();
    boolean isAdmin = adminUsers.allows(username);
    boolean isWriter = writeUsers.allows(username);
    boolean isReader = readOnlyUsers.allows(username);
    boolean isCacheReader = cacheReaderUsers.allows(username);

    return new Enum[] {
      (isAdmin ? ACCESS_LEVEL.ADMIN : null),
      (isWriter ? ACCESS_LEVEL.WRITER : null),
      (isReader ? ACCESS_LEVEL.READER : null),
      (isCacheReader ? ACCESS_LEVEL.CACHE : null)
    };
  }

  public String toString() {
    return "admins: "
        + adminUsers.toString()
        + "\n"
        + "writers: "
        + writeUsers.toString()
        + "\n"
        + "readers: "
        + readOnlyUsers.toString()
        + "\n"
        + "cacheReaders: "
        + cacheReaderUsers.toString();
  }
}
