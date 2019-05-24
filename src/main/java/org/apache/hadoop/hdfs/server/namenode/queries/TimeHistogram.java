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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.commons.lang.math.LongRange;

public class TimeHistogram {

  public enum TimeRange {
    daily,
    weekly,
    monthly,
    yearly
  }

  /**
   * Get range of Strings that represent the time range desired.
   *
   * @param timeRange the time range desired
   * @return String array representing time range desired.
   */
  public static List<String> getKeys(String timeRange) {
    TimeRange timeRangeEnum = TimeRange.valueOf(timeRange);
    switch (timeRangeEnum) {
      case daily:
        return Collections.unmodifiableList(daily_keys);
      case weekly:
        return Collections.unmodifiableList(weekly_keys);
      case monthly:
        return Collections.unmodifiableList(monthly_keys);
      case yearly:
        return Collections.unmodifiableList(yearly_keys);
      default:
        return Collections.unmodifiableList(weekly_keys);
    }
  }

  /* We can lower the number of days to improve performance on multi-filter bin'ing. */
  private static final List<Long> daily_keys0 =
      LongStream.range(1, 365).boxed().collect(Collectors.toList());
  private static final List<String> daily_keys =
      daily_keys0.stream().map(k -> k + " Days").collect(Collectors.toList());
  private static final Long[] daily_binsArray =
      daily_keys0.stream().mapToLong(TimeUnit.DAYS::toMillis).boxed().toArray(Long[]::new);

  /* We can lower the number of days to improve performance on multi-filter bin'ing. */
  private static final List<Long> weekly_keys0 =
      LongStream.range(1, 50).map(l -> l * 7).boxed().collect(Collectors.toList());
  private static final List<String> weekly_keys =
      weekly_keys0.stream().map(k -> k / 7 + " Weeks").collect(Collectors.toList());
  private static final Long[] weekly_binsArray =
      weekly_keys0.stream().mapToLong(TimeUnit.DAYS::toMillis).boxed().toArray(Long[]::new);

  /* We can lower the number of days to improve performance on multi-filter bin'ing. */
  private static final List<Long> monthly_keys0 =
      LongStream.range(1, 24).map(l -> l * 30).boxed().collect(Collectors.toList());
  private static final List<String> monthly_keys =
      monthly_keys0.stream().map(k -> k / 30 + " Months").collect(Collectors.toList());
  private static final Long[] monthly_binsArray =
      monthly_keys0.stream().mapToLong(TimeUnit.DAYS::toMillis).boxed().toArray(Long[]::new);

  /* We can lower the number of days to improve performance on multi-filter bin'ing. */
  private static final List<Long> yearly_keys0 =
      LongStream.range(1, 5).map(l -> l * 365).boxed().collect(Collectors.toList());
  private static final List<String> yearly_keys =
      yearly_keys0.stream().map(k -> k / 365 + " Years").collect(Collectors.toList());
  private static final Long[] yearly_binsArray =
      yearly_keys0.stream().mapToLong(TimeUnit.DAYS::toMillis).boxed().toArray(Long[]::new);

  static {
    {
      daily_keys.add("364 Days+");
      weekly_keys.add("49 Weeks+");
      monthly_keys.add("23 Months+");
      yearly_keys.add("4 Years+");
    }
  }

  private static final Map<String, LongRange> dailyRanges =
      new HashMap<String, LongRange>() {
        {
          // Construct first range.
          long priorBin = 0L;
          String dailyKey = daily_keys.get(0);
          long dailyBin = daily_binsArray[0];
          put(dailyKey, new LongRange(priorBin, dailyBin));

          // Construct middle ranges.
          priorBin = dailyBin;
          for (int i = 1; i < daily_keys0.size(); i++) {
            dailyKey = daily_keys.get(i);
            dailyBin = daily_binsArray[i];
            put(dailyKey, new LongRange(priorBin + 1, dailyBin));
            priorBin = dailyBin;
          }

          // Construct last range.
          String lastKey = daily_keys.get(daily_keys0.size() - 1) + "+";
          put(lastKey, new LongRange(priorBin + 1, Long.MAX_VALUE));
        }
      };

  private static final Map<String, LongRange> weeklyRanges =
      new HashMap<String, LongRange>() {
        {
          // Construct first range.
          long priorBin = 0L;
          String weeklyKey = weekly_keys.get(0);
          long weeklyBin = weekly_binsArray[0];
          put(weeklyKey, new LongRange(priorBin, weeklyBin));

          // Construct middle ranges.
          priorBin = weeklyBin;
          for (int i = 1; i < weekly_keys0.size(); i++) {
            weeklyKey = weekly_keys.get(i);
            weeklyBin = weekly_binsArray[i];
            put(weeklyKey, new LongRange(priorBin + 1, weeklyBin));
            priorBin = weeklyBin;
          }

          // Construct last range.
          String lastKey = weekly_keys.get(weekly_keys0.size() - 1) + "+";
          put(lastKey, new LongRange(priorBin + 1, Long.MAX_VALUE));
        }
      };

  private static final Map<String, LongRange> monthlyRanges =
      new HashMap<String, LongRange>() {
        {
          // Construct first range.
          long priorBin = 0L;
          String monthlyKey = monthly_keys.get(0);
          long monthlyBin = monthly_binsArray[0];
          put(monthlyKey, new LongRange(priorBin, monthlyBin));

          // Construct middle ranges.
          priorBin = monthlyBin;
          for (int i = 1; i < monthly_keys0.size(); i++) {
            monthlyKey = monthly_keys.get(i);
            monthlyBin = monthly_binsArray[i];
            put(monthlyKey, new LongRange(priorBin + 1, monthlyBin));
            priorBin = monthlyBin;
          }

          // Construct last range.
          String lastKey = monthly_keys.get(monthly_keys0.size() - 1) + "+";
          put(lastKey, new LongRange(priorBin + 1, Long.MAX_VALUE));
        }
      };

  private static final Map<String, LongRange> yearlyRanges =
      new HashMap<String, LongRange>() {
        {
          // Construct first range.
          long priorBin = 0L;
          String yearlyKey = yearly_keys.get(0);
          long yearlyBin = yearly_binsArray[0];
          put(yearlyKey, new LongRange(priorBin, yearlyBin));

          // Construct middle ranges.
          priorBin = yearlyBin;
          for (int i = 1; i < yearly_keys0.size(); i++) {
            yearlyKey = yearly_keys.get(i);
            yearlyBin = yearly_binsArray[i];
            put(yearlyKey, new LongRange(priorBin + 1, yearlyBin));
            priorBin = yearlyBin;
          }

          // Construct last range.
          String lastKey = yearly_keys.get(yearly_keys0.size() - 1) + "+";
          put(lastKey, new LongRange(priorBin + 1, Long.MAX_VALUE));
        }
      };

  /**
   * Function method for use by the QueryEngine to assign histogram buckets.
   *
   * @param timeRange defined time range
   * @return function mapping input time to bucket string
   */
  public static Function<Long, String> computeBucketFunction(String timeRange) {
    TimeRange timeRangeEnum = TimeRange.valueOf(timeRange);
    switch (timeRangeEnum) {
      case daily:
        return time -> {
          for (Entry<String, LongRange> range : dailyRanges.entrySet()) {
            if (range.getValue().containsLong(time)) {
              return range.getKey();
            }
          }
          return "NO_MAPPING";
        };
      case monthly:
        return time -> {
          for (Entry<String, LongRange> range : monthlyRanges.entrySet()) {
            if (range.getValue().containsLong(time)) {
              return range.getKey();
            }
          }
          return "NO_MAPPING";
        };
      case yearly:
        return time -> {
          for (Entry<String, LongRange> range : yearlyRanges.entrySet()) {
            if (range.getValue().containsLong(time)) {
              return range.getKey();
            }
          }
          return "NO_MAPPING";
        };
      case weekly:
      default:
        return time -> {
          for (Entry<String, LongRange> range : weeklyRanges.entrySet()) {
            if (range.getValue().containsLong(time)) {
              return range.getKey();
            }
          }
          return "NO_MAPPING";
        };
    }
  }
}
