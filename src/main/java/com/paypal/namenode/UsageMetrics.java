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

import com.google.gson.Gson;
import com.paypal.security.SecurityContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import spark.Request;

/**
 * This class is used for analytical purposes as a way to guage how many users are using and
 * querying NNAnalytics. It contains public methods to store users and IP addresses logging in and
 * out and the number of queries they made.
 *
 * <p>Although UsageMetrics will function when NNAnalytics authentication is disabled, its purpose
 * is intended to be used on authenticated instances where we have access to more basic information
 * on a user.
 */
public class UsageMetrics {

  private final Map<String, UserMetrics> users;

  /** Constructor: Should only be called one time during initialization. */
  public UsageMetrics() {
    users = new HashMap<>();
  }

  /**
   * Called when a user logs in to NNAnalytics.
   *
   * @param secContext SecurityContext
   * @param request spark Request object
   */
  public synchronized void userLoggedIn(SecurityContext secContext, Request request) {
    userLoggedIn(secContext, request.raw());
  }

  /**
   * Called when a user logs in to NNAnalytics.
   *
   * @param secContext SecurityContext
   * @param request http Request object
   */
  public synchronized void userLoggedIn(SecurityContext secContext, HttpServletRequest request) {
    String userName = secContext.getUserName();

    users.putIfAbsent(userName, new UserMetrics(userName));
    users.get(userName).loggedIn(getIpFromRequest(request));
  }

  /**
   * Called when a user logs out of NNAnalytics.
   *
   * @param secContext SecurityContext
   * @param request spark Request object
   */
  public synchronized void userLoggedOut(SecurityContext secContext, Request request) {
    userLoggedOut(secContext, request.raw());
  }

  /**
   * Called when a user logs out of NNAnalytics.
   *
   * @param secContext SecurityContext
   * @param request http Request object
   */
  public synchronized void userLoggedOut(SecurityContext secContext, HttpServletRequest request) {
    String userName = secContext.getUserName();

    users.putIfAbsent(userName, new UserMetrics(userName));
    users.get(userName).loggedOut(getIpFromRequest(request));
  }

  /**
   * Called when a user makes a query.
   *
   * @param secContext SecurityContext
   * @param request spark Request object
   */
  public synchronized void userMadeQuery(SecurityContext secContext, Request request) {
    userMadeQuery(secContext, request.raw());
  }

  /**
   * Called when a user makes a query.
   *
   * @param secContext SecurityContext
   * @param request http Request object
   */
  public synchronized void userMadeQuery(SecurityContext secContext, HttpServletRequest request) {
    String userName = secContext.getUserName();

    users.putIfAbsent(userName, new UserMetrics(userName));
    users.get(userName).queried(getIpFromRequest(request));
  }

  /**
   * Get the appropriate IP from the spark Request object. Returns first value found in the
   * following order: "X-Real-IP" header > "X-Forwarded-For" header > request remote address
   *
   * @param request http Request object
   * @return String
   */
  private String getIpFromRequest(HttpServletRequest request) {
    String realIp = request.getHeader("X-Real-IP");
    if (realIp != null && !realIp.isEmpty()) {
      return realIp;
    }
    String forwardedFor = request.getHeader("X-Forwarded-For");
    if (forwardedFor != null && !forwardedFor.isEmpty()) {
      return forwardedFor.split(",")[0];
    }
    return request.getRemoteAddr();
  }

  /**
   * Return a JSON encoded string of user metrics to be used on the front-end.
   *
   * @return String
   */
  public synchronized String getUserMetricsJson() {
    ArrayList<Map> userList = new ArrayList<>();
    for (UserMetrics user : users.values()) {
      userList.add(user.formatForJson());
    }

    Map<String, Object> returnValues = new HashMap<>();
    returnValues.put("users", userList);

    return new Gson().toJson(returnValues);
  }
}
