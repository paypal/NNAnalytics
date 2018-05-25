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

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SpaceSizeHistogram {

  private static final long kilobyteBase = 1024L;
  private static final long megabyteBase = kilobyteBase * kilobyteBase;
  private static final long gigabyteBase = kilobyteBase * megabyteBase;
  private static final DecimalFormat ONE_DECIMAL_FORMAT = new DecimalFormat("#0.0");

  public static Long[] getBinsArray() {
    return binsArray.clone();
  }

  public static List<String> getKeys() {
    return Collections.unmodifiableList(keys);
  }

  private static final Long[] binsArray = new Long[]{
      0L, kilobyteBase, megabyteBase, 16L * megabyteBase, 64L * megabyteBase,
      128L * megabyteBase, 256L * megabyteBase, 512 * megabyteBase, gigabyteBase};
  private static final List<Long> bins = Arrays.asList(binsArray);
  private static final List<String> keys =
      bins.stream().map(SpaceSizeHistogram::readableFileSize).collect(Collectors.toList());

  private static String readableFileSize(long size) {
    if (size <= 0) {
      return "0 B";
    }
    String[] units = new String[]{"B", "KB", "MB", "GB", "TB", "PB", "EB"};
    int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
    return ONE_DECIMAL_FORMAT.format(size / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
  }
}
