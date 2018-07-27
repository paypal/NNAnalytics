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
import java.util.Optional;
import java.util.Set;
import javax.servlet.http.HttpSession;
import org.apache.hadoop.hdfs.server.namenode.NNAConstants;
import org.apache.hadoop.hdfs.server.namenode.NNAConstants.ENDPOINT;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.eclipse.jetty.http.HttpStatus;
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

  public boolean isAuthenticationEnabled() {
    return ldapAuthenticator != null || (jwtAuthenticator != null && jwtGenerator != null);
  }

  /**
   * Begin authenticated web session; set a JWT on response if successful; 401/403 otherwise.
   *
   * @param req - The HTTP request.
   * @param res - The HTTP response.
   */
  public void login(Request req, Response res) throws AuthenticationException, HttpAction {
    boolean authenticationEnabled = isAuthenticationEnabled();
    if (!authenticationEnabled) {
      String reqUsername = req.queryParams("proxy");
      if (reqUsername != null && !reqUsername.isEmpty()) {
        currentUser.set(reqUsername);
      }
      return;
    }

    String username = req.queryMap().get("username").value();
    String password = req.queryMap().get("password").value();

    if (username == null || username.isEmpty() || password == null || password.isEmpty()) {
      LOG.info("Corrupt login credentials for: {}", req.ip());
      throw new AuthenticationException("Bad username / password provided.");
    }

    UsernamePasswordCredentials credentials;
    CommonProfile profile;

    // Perform local authentication if found.
    if (localOnlyUsers.allows(username)) {
      if (localOnlyUsers.authenticate(username, password)) {
        LOG.info("Login success via [LOCAL] for: {} at {}", username, req.ip());
        profile = new CommonProfile();
        profile.setId(username);
        String generate = jwtGenerator.generate(profile);
        res.header("Set-Cookie", "nna-jwt-token=" + generate);
        currentUser.set(username);
        return;
      } else {
        LOG.info("Login failed via [LOCAL] for: {}", req.ip());
        throw new AuthenticationException("Authentication required.");
      }
    }

    // Perform LDAP authentication if found.
    if (ldapAuthenticator != null) {
      RuntimeException authFailedEx = null;
      Set<String> ldapBaseDns = securityConfiguration.getLdapBaseDn();
      for (String ldapBaseDn : ldapBaseDns) {
        String ldapDnRegexd = ldapBaseDn.replaceAll("%u", username);
        ldapAuthenticator.getLdapAuthenticator().setDnResolver(new FormatDnResolver(ldapDnRegexd));
        credentials = new UsernamePasswordCredentials(username, password, req.ip());
        try {
          ldapAuthenticator.validate(credentials, new SparkWebContext(req, res));
        } catch (RuntimeException e) {
          authFailedEx = e;
          continue;
        }
        LOG.info("Login success via [LDAP] for: {} at {}", username, req.ip());
        profile = credentials.getUserProfile();
        profile.setId(username);
        String generate = jwtGenerator.generate(profile);
        res.header("Set-Cookie", "nna-jwt-token=" + generate);
        currentUser.set(username);
        break;
      }

      if (authFailedEx != null) {
        LOG.info("Login failed via [LDAP] for: {}", req.ip());
        throw authFailedEx;
      } else {
        return;
      }
    }

    LOG.info("Login failed for: {}", req.ip());
    throw new AuthenticationException("Authentication required.");
  }

  /**
   * Perform logout of authenticated web session.
   *
   * @param req - The HTTP request.
   * @param res - The HTTP response.
   */
  public void logout(Request req, Response res) {
    boolean authenticationEnabled = isAuthenticationEnabled();
    ProfileManager<CommonProfile> manager = new ProfileManager<>(new SparkWebContext(req, res));
    Optional<CommonProfile> profile = manager.get(false);
    if (authenticationEnabled && profile.isPresent()) {
      manager.logout();
      HttpSession session = req.raw().getSession();
      if (session != null) {
        session.invalidate();
      }
      res.removeCookie("nna-jwt-token");
      res.header("Cache-Control", "no-cache, no-store, must-revalidate");
      res.header("Pragma", "no-cache");
      res.header("Expires", "0");
      res.status(HttpStatus.OK_200);
      res.body("You have been logged out.");
    } else {
      res.status(HttpStatus.BAD_REQUEST_400);
      res.body("No login session.");
    }
  }

  public void handleAuthentication(Request req, Response res)
      throws AuthenticationException, HttpAction {
    if (!init) {
      LOG.info("Request occurred before initialized from: {}", req.ip());
      throw new AuthenticationException("Please wait for initialization.");
    }

    boolean isLoginAttempt = req.raw().getRequestURI().startsWith("/" + ENDPOINT.login.name());
    if (isLoginAttempt) {
      return;
    }

    boolean authenticationEnabled = isAuthenticationEnabled();
    if (!authenticationEnabled) {
      String proxyUsername = req.queryParams("proxy");
      if (proxyUsername != null && !proxyUsername.isEmpty()) {
        currentUser.set(proxyUsername);
      }
      return;
    }

    // Allow basic authentication for simple applications.
    String basic = req.headers("Authorization");
    if (basic != null && basic.startsWith("Basic ")) {
      String b64Credentials = basic.substring("Basic ".length()).trim();
      String nameAndPassword =
          new String(Base64.getDecoder().decode(b64Credentials), Charset.defaultCharset());
      String[] split = nameAndPassword.split(":");
      String username = split[0];
      String password = split[1];
      if (localOnlyUsers.authenticate(username, password)) {
        currentUser.set(username);
        return;
      }
    }

    // JWT authentication for end users whom have logged in.
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
        String profileId = userProfile.getId();
        LOG.info("Login success via [TOKEN] for: {} at {}", profileId, req.ip());
        currentUser.set(profileId);
        return;
      } catch (Exception e) {
        LOG.info("Login failed via [TOKEN] for: {}", req.ip());
        throw new AuthenticationException(e);
      }
    }

    LOG.info("Login failed via [NULL] for: {}", req.ip());
    throw new AuthenticationException("Authentication required.");
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
