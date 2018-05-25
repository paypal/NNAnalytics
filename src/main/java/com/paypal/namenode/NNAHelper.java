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

import java.io.IOException;
import java.util.Collection;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NNLoader;
import org.apache.hadoop.hdfs.server.namenode.queries.BaseQuery;
import org.apache.hadoop.io.IOUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

class NNAHelper {

  static String toYAxis(String sum) {
    switch (sum) {
      case "count":
        return "# of INodes";
      case "fileSize":
        return "Bytes (No Replication Factor)";
      case "diskspaceConsumed":
        return "Bytes (With Replication Factor)";
      case "numBlocks":
        return "# of Blocks (No Replication Factor)";
      case "blockSize":
        return "Block Size (No Replication Factor)";
      case "numReplicas":
        return "# of Replicas (Blocks * Replication Factor)";
      case "memoryConsumed":
        return "Bytes";
      case "dsQuota":
        return "Quota";
      case "nsQuota":
        return "Quota";
      case "dsQuotaUsed":
        return "Quota Bytes Used";
      case "nsQuotaUsed":
        return "Namespace Quota Used";
      case "dsQuotaRatioUsed":
        return "Usage Percentage";
      case "nsQuotaRatioUsed":
        return "Usage Percentage";
      default:
        throw new IllegalArgumentException("Could not determine sum type: " + sum +
            ".\nPlease check /sums for available sums.");
    }
  }

  static String toTitle(String histType, String sum) {
    return histType.toUpperCase() + " HISTOGRAM | " + sum.toUpperCase();
  }

  static String getTrackingUrl(HttpServletRequest req) {
    String requestUri = req.getRequestURI();
    String queryString = req.getQueryString();
    if (queryString == null) {
      return requestUri;
    }
    return requestUri + "?" + queryString;
  }

  static Collection<INode> performFilters(NNLoader nnLoader,
      String set,
      String[] filters,
      String[] filterOps,
      String find) {
    Collection<INode> interim = performFilters(nnLoader, set, filters, filterOps);
    return nnLoader.findFilter(interim, find);
  }

  static Collection<INode> performFilters(NNLoader nnLoader,
      String set,
      String[] filters,
      String[] filterOps) {
    Collection<INode> inodes = nnLoader.getINodeSet(set);

    if (filters == null || filters.length == 0 ||
        filterOps == null || filterOps.length == 0) {
      return inodes;
    }

    inodes = nnLoader.combinedFilter(inodes, filters, filterOps);

    return inodes;
  }

  static void toJsonList(HttpServletResponse resp, Enum[]... values) throws IOException {
    JsonGenerator json = new JsonFactory().createJsonGenerator(resp.getWriter())
        .useDefaultPrettyPrinter();
    try {
      json.writeStartObject();
      for (int i = 0; i < values.length; i++) {
        Enum[] enumList = values[i];
        json.writeArrayFieldStart("Possibilities " + (i + 1));
        for (Enum value : enumList) {
          if (value != null) {
            json.writeStartObject();
            json.writeStringField("Name", value.name());
            json.writeEndObject();
          }
        }
        json.writeEndArray();
      }
      json.writeEndObject();
    } finally {
      IOUtils.closeStream(json);
    }
  }

  static String[] parseFilters(String fullFilterStr) {
    if (fullFilterStr != null && !fullFilterStr.isEmpty()) {
      String[] filterSplits = fullFilterStr.split(",");
      String[] filters = new String[filterSplits.length];
      for (int i = 0; i < filterSplits.length; i++) {
        String[] filterSplit = filterSplits[i].split(":");
        if (filterSplit.length != 3) {
          throw new IllegalArgumentException(
              "Incorrect filter argument format for: '" + filterSplits[i] +
                  "'. Needs to be <filter>:<op>:<filed>.");
        }
        String filter = filterSplit[0];
        filters[i] = filter;
      }
      return filters;
    }
    return null;
  }

  static String[] parseFilterOps(String fullFilterStr) {
    if (fullFilterStr != null && !fullFilterStr.isEmpty()) {
      String[] filterOpSplits = fullFilterStr.split(",");
      String[] filterOps = new String[filterOpSplits.length];
      for (int i = 0; i < filterOpSplits.length; i++) {
        String[] filterOpSplit = filterOpSplits[i].split(":");
        if (filterOpSplit.length != 3) {
          throw new IllegalArgumentException(
              "Incorrect filter argument format for: '" + filterOpSplits[i] +
                  "'. Needs to be <filter>:<op>:<filed>.");
        }
        String filterOp = filterOpSplit[1];
        String filterOpField = filterOpSplit[2];
        filterOps[i] = String.join(":", filterOp, filterOpField);
      }
      return filterOps;
    }
    return null;
  }

  static BaseQuery createQuery(HttpServletRequest raw, String userName) {
    return new BaseQuery(NNAHelper.getTrackingUrl(raw), userName);
  }
}
