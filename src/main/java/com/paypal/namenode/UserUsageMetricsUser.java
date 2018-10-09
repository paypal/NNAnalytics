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
import java.util.concurrent.atomic.AtomicInteger;

public class UserUsageMetricsUser {

    private final String userName;
    private final Map<String, AtomicInteger> totalQueryCountsByIp;
    private final Map<String, AtomicInteger> totalLoginCountsByIp;
    private final Map<String, AtomicInteger> totalLogoutCountsByIp;
    private final AtomicInteger totalQueryCount;
    private final AtomicInteger totalLoginCount;
    private final AtomicInteger totalLogoutCount;
    private final ArrayList<Map> userMetrics;

    public UserUsageMetricsUser(String userName) {
        this.userMetrics = new ArrayList<>();
        this.userName = userName;
        this.totalQueryCountsByIp = new HashMap<>();
        this.totalLoginCountsByIp = new HashMap<>();
        this.totalLogoutCountsByIp = new HashMap<>();
        this.totalQueryCount = new AtomicInteger(0);
        this.totalLoginCount = new AtomicInteger(0);
        this.totalLogoutCount = new AtomicInteger(0);
    }

    public void loggedIn(String ipAddress) {
        totalLoginCountsByIp.putIfAbsent(ipAddress, new AtomicInteger(0));
        totalLoginCountsByIp.get(ipAddress).incrementAndGet();
        totalLoginCount.incrementAndGet();
    }

    public void loggedOut(String ipAddress) {
        totalLogoutCountsByIp.putIfAbsent(ipAddress, new AtomicInteger(0));
        totalLogoutCountsByIp.get(ipAddress).incrementAndGet();
        totalLogoutCount.incrementAndGet();
    }

    public void queried(String ipAddress) {
        totalQueryCountsByIp.putIfAbsent(ipAddress, new AtomicInteger(0));
        totalQueryCountsByIp.get(ipAddress).incrementAndGet();
        totalQueryCount.incrementAndGet();
    }

    public AtomicInteger GetTotalQueryCount() {
        return totalQueryCount;
    }

    public AtomicInteger GetTotalLoginCount() {
        return totalLoginCount;
    }

    public AtomicInteger GetTotalLogoutCount() {
        return totalLogoutCount;
    }

    /**
     * Refresh the userMetrics ArrayList with the most recent data.
     */
    public void refreshUserMetrics() {
        userMetrics.clear();

        // set up the array metricsByIp
        ArrayList<Map> metricsByIp = new ArrayList<>();
        // now we need to build the hashmap with the individual ips as keys
        HashMap<String, Map> ipMap = new HashMap<>();

        for(String ip : totalQueryCountsByIp.keySet()) {
            ipMap.putIfAbsent(ip, new HashMap<>());
            ipMap.get(ip).putIfAbsent("ip", ip);
            ipMap.get(ip).putIfAbsent("queryCount", totalQueryCountsByIp.getOrDefault(ip, new AtomicInteger(0)));
        }

        for(String ip : totalLoginCountsByIp.keySet()) {
            ipMap.putIfAbsent(ip, new HashMap<>());
            ipMap.get(ip).putIfAbsent("ip", ip);
            ipMap.get(ip).putIfAbsent("loginCount", totalLoginCountsByIp.getOrDefault(ip, new AtomicInteger(0)));
        }

        for(String ip : totalLogoutCountsByIp.keySet()) {
            ipMap.putIfAbsent(ip, new HashMap<>());
            ipMap.get(ip).putIfAbsent("ip", ip);
            ipMap.get(ip).putIfAbsent("logoutCount", totalLogoutCountsByIp.getOrDefault(ip, new AtomicInteger(0)));
        }

        metricsByIp.addAll(ipMap.values());

        userMetrics.addAll(metricsByIp);
    }

}