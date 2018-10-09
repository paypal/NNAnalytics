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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class UserUsageMetricsUser {

    private final String userName;
    private final Map<String, Integer> totalQueryCountsByIp;
    private final Map<String, Integer> totalLoginCountsByIp;
    private final Map<String, Integer> totalLogoutCountsByIp;
    private Integer totalQueryCount;
    private Integer totalLoginCount;
    private Integer totalLogoutCount;
    private ArrayList<Map<String,Map<String,Integer>>> userMetrics;

    public UserUsageMetricsUser(String userName) {
        this.userMetrics = new ArrayList<>();
        this.userName = userName;
        this.totalQueryCountsByIp = new HashMap<>();
        this.totalLoginCountsByIp = new HashMap<>();
        this.totalLogoutCountsByIp = new HashMap<>();
        this.totalQueryCount = 0;
        this.totalLoginCount = 0;
        this.totalLogoutCount = 0;
    }

    public void loggedIn(String ipAddress) {
        int count = totalLoginCountsByIp.getOrDefault(ipAddress, 0);
        totalLoginCountsByIp.put(ipAddress, count + 1);
        totalLoginCount = totalLoginCount + 1;
    }

    public void loggedOut(String ipAddress) {
        int count = totalLogoutCountsByIp.getOrDefault(ipAddress, 0);
        totalLogoutCountsByIp.put(ipAddress, count + 1);
        totalLogoutCount = totalLogoutCount + 1;
    }

    public void queried(String ipAddress) {
        int count = totalQueryCountsByIp.getOrDefault(ipAddress, 0);
        totalQueryCountsByIp.put(ipAddress, count + 1);
        totalQueryCount = totalQueryCount + 1;
    }

    /**
     * Refresh the userMetrics ArrayList with the most recent data.
     */
    public void refreshUserMetrics() {
        userMetrics.clear();

        HashMap<String, Map<String, Integer>> ipMap = new HashMap<>();

        for(String ip : totalQueryCountsByIp.keySet()) {
            ipMap.putIfAbsent(ip, new HashMap<>());
            ipMap.get(ip).putIfAbsent("queryCount", totalQueryCountsByIp.getOrDefault(ip, 0));
        }

        for(String ip : totalLoginCountsByIp.keySet()) {
            ipMap.putIfAbsent(ip, new HashMap<>());
            ipMap.get(ip).putIfAbsent("loginCount", totalLoginCountsByIp.getOrDefault(ip, 0));
        }

        for(String ip : totalLogoutCountsByIp.keySet()) {
            ipMap.putIfAbsent(ip, new HashMap<>());
            ipMap.get(ip).putIfAbsent("logoutCount", totalLogoutCountsByIp.getOrDefault(ip, 0));
        }

        ipMap.keySet().forEach(key -> {
            Map<String,Map<String,Integer>> ipMetricMap = new HashMap<>();
            ipMetricMap.put(key, ipMap.get(key));
            userMetrics.add(ipMetricMap);
        });

    }

}