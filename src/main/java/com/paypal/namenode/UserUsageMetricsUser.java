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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class UserUsageMetricsUser {

    private final String userName;
    private final Map<String, AtomicInteger> queries;
    private final Map<String, AtomicInteger> logins;
    private final Map<String, AtomicInteger> logouts;

    public UserUsageMetricsUser(String userName) {

        this.userName = userName;
        this.queries = new HashMap<>();
        this.logins = new HashMap<>();
        this.logouts = new HashMap<>();

    }

    public Integer LoggedIn(String ipAddress) {
        logins.putIfAbsent(ipAddress, new AtomicInteger(0));
        return logins.get(ipAddress).incrementAndGet();
    }

    public Integer LoggedOut(String ipAddress) {
        logouts.putIfAbsent(ipAddress, new AtomicInteger(0));
        return logouts.get(ipAddress).incrementAndGet();
    }

    public Integer Queried(String ipAddress) {
        queries.putIfAbsent(ipAddress, new AtomicInteger(0));
        return queries.get(ipAddress).incrementAndGet();
    }

}