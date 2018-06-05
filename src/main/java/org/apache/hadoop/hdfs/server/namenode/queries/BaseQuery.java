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

package org.apache.hadoop.hdfs.server.namenode.queries;

public class BaseQuery {

  private final String trackingUrl;
  private final String userName;

  public BaseQuery(String trackingUrl, String userName) {
    this.trackingUrl = trackingUrl;
    this.userName = userName;
  }

  @Override
  public String toString() {
    return trackingUrl + ":" + userName;
  }

  @Override
  public int hashCode() {
    int result = trackingUrl != null ? trackingUrl.hashCode() : 0;
    result = 31 * result + (userName != null ? userName.hashCode() : 0);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BaseQuery baseQuery = (BaseQuery) o;

    return (trackingUrl != null
            ? trackingUrl.equals(baseQuery.trackingUrl)
            : baseQuery.trackingUrl == null)
        && (userName != null ? userName.equals(baseQuery.userName) : baseQuery.userName == null);
  }
}
