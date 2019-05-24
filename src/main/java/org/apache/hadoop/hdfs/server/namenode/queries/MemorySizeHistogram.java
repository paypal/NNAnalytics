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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang.math.LongRange;

public class MemorySizeHistogram {

  private static final long kilobyteBase = 1024L;
  private static final DecimalFormat NO_DECIMAL_FORMAT = new DecimalFormat("#0");

  public static List<String> getKeys() {
    return Collections.unmodifiableList(keys);
  }

  private static final Long[] binsArray =
      new Long[] {
        256L,
        512L,
        768L,
        kilobyteBase,
        2 * kilobyteBase,
        4 * kilobyteBase,
        8 * kilobyteBase,
        16 * kilobyteBase,
        32 * kilobyteBase,
        64 * kilobyteBase
      };
  private static final List<Long> bins = Arrays.asList(binsArray);
  private static final List<String> keys =
      bins.stream().map(MemorySizeHistogram::readableFileSize).collect(Collectors.toList());

  static {
    {
      keys.add("64 KB+");
    }
  }

  private static final Map<String, LongRange> ranges =
      new HashMap<String, LongRange>() {
        {
          put("256 B", new LongRange(1L, 256L));
          put("512 B", new LongRange(257L, 512L));
          put("768 B", new LongRange(513L, 768L));
          put("1 KB", new LongRange(768L, kilobyteBase));
          put("2 KB", new LongRange(kilobyteBase + 1, 2 * kilobyteBase));
          put("4 KB", new LongRange(2 * kilobyteBase + 1, 4 * kilobyteBase));
          put("8 KB", new LongRange(4 * kilobyteBase + 1, 8 * kilobyteBase));
          put("16 KB", new LongRange(8 * kilobyteBase + 1, 16 * kilobyteBase));
          put("32 KB", new LongRange(16 * kilobyteBase + 1, 32 * kilobyteBase));
          put("64 KB", new LongRange(32 * kilobyteBase + 1, 64 * kilobyteBase));
          put("64 KB+", new LongRange(64 * kilobyteBase, Long.MAX_VALUE));
        }
      };

  public static final Function<Long, String> determineBucketFunction =
      size -> {
        for (Entry<String, LongRange> range : ranges.entrySet()) {
          if (range.getValue().containsLong(size)) {
            return range.getKey();
          }
        }
        return "NO_MAPPING";
      };

  private static String readableFileSize(long size) {
    if (size <= 0) {
      return "0 B";
    }
    String[] units = new String[] {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
    int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
    return NO_DECIMAL_FORMAT.format(size / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
  }
}
