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

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is used for analytical purposes as a way to guage how many users are
 * using and querying NNAnalytics. It contains public methods to store users and IP
 * addresses logging in and out and the number of queries they made.
 *
 * Although UsageMetrics will function when NNAnalytics authentication is disabled,
 * its purpose is intended to be used on authenticated instances where we
 * have access to more basic information on a user.
 */
public class UsageMetrics {

    private final Map<String, UserMetrics> users;

    private final Map<String, AtomicInteger> uniqueUserLoginCount;
    private final Map<String, AtomicInteger> uniqueIpLoginCount;
    private final Map<String, AtomicInteger> uniqueUserLogoutCount;
    private final Map<String, AtomicInteger> uniqueIpLogoutCount;
    private final Map<String, AtomicInteger> uniqueUserQueryCount;
    private final Map<String, AtomicInteger> uniqueIpQueryCount;

    public UsageMetrics() {
        users = new HashMap<>();

        uniqueUserLoginCount = new HashMap<>();
        uniqueIpLoginCount = new HashMap<>();
        uniqueUserLogoutCount = new HashMap<>();
        uniqueIpLogoutCount = new HashMap<>();
        uniqueUserQueryCount = new HashMap<>();
        uniqueIpQueryCount = new HashMap<>();
    }

    /**
     * Called when a user logs in to NNAnalytics
     *
     * @param secContext SecurityContext
     * @param ipAddress String
     */
    public synchronized void userLoggedIn(SecurityContext secContext, String ipAddress) {
        String userName = secContext.getUserName();

        users.putIfAbsent(userName, new UserMetrics(userName));
        users.get(userName).loggedIn(ipAddress);

        uniqueUserLoginCount.putIfAbsent(userName, new AtomicInteger(0));
        uniqueUserLoginCount.get(userName).incrementAndGet();

        uniqueIpLoginCount.putIfAbsent(userName, new AtomicInteger(0));
        uniqueIpLoginCount.get(userName).incrementAndGet();
    }

    /**
     * Called when a user logs out of NNAnalytics
     *
     * @param secContext SecurityContext
     * @param ipAddress String
     */
    public synchronized void userLoggedOut(SecurityContext secContext, String ipAddress) {
        String userName = secContext.getUserName();

        users.putIfAbsent(userName, new UserMetrics(userName));
        users.get(userName).loggedOut(ipAddress);

        uniqueUserLogoutCount.putIfAbsent(userName, new AtomicInteger(0));
        uniqueUserLogoutCount.get(userName).incrementAndGet();

        uniqueIpLogoutCount.putIfAbsent(userName, new AtomicInteger(0));
        uniqueIpLogoutCount.get(userName).incrementAndGet();
    }

    /**
     * Called when a user makes a query
     *
     * @param secContext SecurityContext
     * @param ipAddress String
     */
    public synchronized void userMadeQuery(SecurityContext secContext, String ipAddress) {
        String userName = secContext.getUserName();

        users.putIfAbsent(userName, new UserMetrics(userName));
        users.get(userName).queried(ipAddress);

        uniqueUserQueryCount.putIfAbsent(userName, new AtomicInteger(0));
        uniqueUserQueryCount.get(userName).incrementAndGet();

        uniqueIpQueryCount.putIfAbsent(ipAddress, new AtomicInteger(0));
        uniqueIpQueryCount.get(ipAddress).incrementAndGet();
    }

    /**
     * Return a JSON encoded string of user metrics to be used on the front-end
     *
     * @return String
     */
    public synchronized String getUserMetricsJson() {
        ArrayList<Map> userList = new ArrayList<>();
        for(UserMetrics user : users.values()) {
            userList.add(user.formatForJson());
        }

        Map<String, Object> returnValues = new HashMap<>();
        returnValues.put("users", userList);

        return new Gson().toJson(returnValues);
    }

}