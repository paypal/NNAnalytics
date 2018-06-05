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

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;

class BiggerValueComperator implements Comparator<Map.Entry<String, Long>>, Serializable {

  /**
   * compares the given value of the map entry.
   *
   * @param o1 first map entry containing value to be compared
   * @param o2 second map entry containing value to be compared
   * @return -1 if first is bigger, 0 if both are equal value, 1 if second is bigger.
   */
  @Override
  public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
    long result = o2.getValue() - o1.getValue();
    if (result > 0L) {
      return 1;
    }
    if (result < 0L) {
      return -1;
    }
    return 0;
  }
}
