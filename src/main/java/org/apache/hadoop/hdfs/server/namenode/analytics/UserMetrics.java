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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class UserMetrics {

  private final String userName;
  private final Map<String, Long> totalQueryCountsByIp;
  private final Map<String, Long> totalLoginCountsByIp;
  private final Map<String, Long> totalLogoutCountsByIp;

  /**
   * Constructor for a User object. Can instantiate many user objects.
   *
   * @param userName the username of the user
   */
  public UserMetrics(String userName) {
    this.userName = userName;
    this.totalQueryCountsByIp = new HashMap<>();
    this.totalLoginCountsByIp = new HashMap<>();
    this.totalLogoutCountsByIp = new HashMap<>();
  }

  public void loggedIn(String ipAddress) {
    long count = totalLoginCountsByIp.getOrDefault(ipAddress, 0L);
    totalLoginCountsByIp.put(ipAddress, count + 1);
  }

  public void loggedOut(String ipAddress) {
    long count = totalLogoutCountsByIp.getOrDefault(ipAddress, 0L);
    totalLogoutCountsByIp.put(ipAddress, count + 1);
  }

  public void queried(String ipAddress) {
    long count = totalQueryCountsByIp.getOrDefault(ipAddress, 0L);
    totalQueryCountsByIp.put(ipAddress, count + 1);
  }

  /**
   * Builds a HashMap of this users metrics to be formatted into JSON.
   *
   * @return HashMap
   */
  public HashMap<String, Object> formatForJson() {
    HashMap<String, Object> jsonFormattedUser = new HashMap<>();
    jsonFormattedUser.put(
        "totalQueryCount", totalQueryCountsByIp.values().stream().mapToLong(l -> l).sum());
    jsonFormattedUser.put(
        "totalLoginCount", totalLoginCountsByIp.values().stream().mapToLong(l -> l).sum());
    jsonFormattedUser.put(
        "totalLogoutCount", totalLogoutCountsByIp.values().stream().mapToLong(l -> l).sum());
    jsonFormattedUser.put("userName", userName);

    HashMap<String, Map<String, Long>> ipMap = new HashMap<>();

    for (String ip : totalQueryCountsByIp.keySet()) {
      ipMap.putIfAbsent(ip, new HashMap<>());
      ipMap.get(ip).putIfAbsent("queryCount", totalQueryCountsByIp.getOrDefault(ip, 0L));
    }

    for (String ip : totalLoginCountsByIp.keySet()) {
      ipMap.putIfAbsent(ip, new HashMap<>());
      ipMap.get(ip).putIfAbsent("loginCount", totalLoginCountsByIp.getOrDefault(ip, 0L));
    }

    for (String ip : totalLogoutCountsByIp.keySet()) {
      ipMap.putIfAbsent(ip, new HashMap<>());
      ipMap.get(ip).putIfAbsent("logoutCount", totalLogoutCountsByIp.getOrDefault(ip, 0L));
    }

    ArrayList<Map<String, Map<String, Long>>> ipList = new ArrayList<>();

    ipMap
        .keySet()
        .forEach(
            key -> {
              Map<String, Map<String, Long>> ipMetricMap = new HashMap<>();
              ipMetricMap.put(key, ipMap.get(key));
              ipList.add(ipMetricMap);
            });

    jsonFormattedUser.put("ips", ipList);

    return jsonFormattedUser;
  }
}
