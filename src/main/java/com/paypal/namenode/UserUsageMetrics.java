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

import java.util.HashMap;
import java.util.Map;

/**
 * This class is used for analytical purposes as a way to guage how many users are
 * using and querying NNAnalytics. It contains public methods to store users and IP
 * addresses logging in and out and the number of queries they made.
 *
 * Although UserUsageMetrics will function when NNAnalytics authentication is disabled,
 * its purpose is intended to be used on authenticated instances where we
 * have access to more basic information on a user.
 */
public class UserUsageMetrics {

    private final Map<String, UserUsageMetricsUser> users;

    public UserUsageMetrics() {

        users = new HashMap<>();

    }

    /**
     * Called when a user logs in to NNAnalytics
     *
     * @param secContext SecurityContext
     * @param ipAddress String
     */
    public Integer UserLoggedIn(SecurityContext secContext, String ipAddress) {

        String userName = secContext.getUserName();

        users.putIfAbsent(userName, new UserUsageMetricsUser(userName));
        return users.get(userName).LoggedIn(ipAddress);

    }

    /**
     * Called when a user logs out of NNAnalytics
     *
     * @param secContext SecurityContext
     * @param ipAddress String
     */
    public Integer UserLoggedOut(SecurityContext secContext, String ipAddress) {

        String userName = secContext.getUserName();

        users.putIfAbsent(userName, new UserUsageMetricsUser(userName));
        return users.get(userName).LoggedOut(ipAddress);

    }

    /**
     * Called when a user makes a query
     *
     * @param secContext SecurityContext
     * @param ipAddress String
     */
    public Integer UserMadeQuery(SecurityContext secContext, String ipAddress) {

        String userName = secContext.getUserName();

        users.putIfAbsent(userName, new UserUsageMetricsUser(userName));
        return users.get(userName).Queried(ipAddress);

    }

    /**
     * Return a JSON encoded string of tracked class properties
     *
     * @return String
     */
    public String GetUserMetrics() {

        Map<String, Map> userMetrics = new HashMap<>();
        userMetrics.put("users", this.users);

        return new Gson().toJson(userMetrics);

    }

}