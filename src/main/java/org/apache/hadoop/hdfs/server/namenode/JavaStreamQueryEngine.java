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

package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JavaStreamQueryEngine extends AbstractQueryEngine {

  /**
   * Main filter method for filtering down a set of INodes to a smaller subset.
   *
   * @param inodes the main inode set to work on
   * @param filters set of filters to use
   * @param filterOps matching length set of filter operands and operators
   * @return the filtered set of inodes
   */
  @Override // QueryEngine
  public Collection<INode> combinedFilter(
      Collection<INode> inodes, String[] filters, String[] filterOps) {
    final ArrayList<Function<INode, Boolean>> filterArray = new ArrayList<>();

    for (int i = 0; i < filters.length; i++) {
      String filter = filters[i];
      String[] filterOp = filterOps[i].split(":");
      Function<INode, Boolean> filterFunc = getFilter(filter, filterOp);
      filterArray.add(filterFunc);
    }

    if (filterArray.size() == 0) {
      return inodes;
    }

    long start = System.currentTimeMillis();
    try {
      Stream<INode> stream = inodes.parallelStream();
      for (Function<INode, Boolean> filter : filterArray) {
        stream = stream.filter(filter::apply);
      }
      return stream.collect(Collectors.toList());
    } finally {
      long end = System.currentTimeMillis();
      LOG.info(
          "Performing filters: {} with filterOps: {} took: {} ms.",
          Arrays.asList(filters),
          Arrays.asList(filterOps),
          (end - start));
    }
  }

  private Function<INode, Boolean> getFilter(String filter, String[] filterOps) {
    long start = System.currentTimeMillis();
    try {
      // Values for all other filters
      String op = filterOps[0];
      String opValue = filterOps[1];

      // Long value filters
      Function<INode, Long> longFunction = getFilterFunctionToLongForINode(filter);
      if (longFunction != null) {
        Function<Long, Boolean> longCompFunction =
            getFilterFunctionForLong(Long.parseLong(opValue), op);
        return longFunction.andThen(longCompFunction);
      }

      // String value filters
      Function<INode, String> strFunction = getFilterFunctionToStringForINode(filter);
      if (strFunction != null) {
        Function<String, Boolean> strCompFunction = getFilterFunctionForString(opValue, op);
        return strFunction.andThen(strCompFunction);
      }

      // Boolean value filters
      Function<INode, Boolean> boolFunction = getFilterFunctionToBooleanForINode(filter);
      if (boolFunction != null) {
        Function<Boolean, Boolean> boolCompFunction =
            getFilterFunctionForBoolean(Boolean.parseBoolean(opValue), op);
        return boolFunction.andThen(boolCompFunction);
      }

      throw new IllegalArgumentException(
          "Failed to determine filter: "
              + filter
              + ", with operations: "
              + Arrays.asList(filterOps)
              + ".\nCheck your filter arguments."
              + "\nPossible filters and operations available at /filters and /filterOps.");
    } finally {
      long end = System.currentTimeMillis();
      LOG.info(
          "Obtaining filter: {} with filterOps:{} took: {} ms.",
          filter,
          Arrays.asList(filterOps),
          (end - start));
    }
  }
}
