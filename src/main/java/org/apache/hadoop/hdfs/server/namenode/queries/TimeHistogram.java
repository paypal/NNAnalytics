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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TimeHistogram {

  public enum TIME_RANGES {daily, weekly, monthly, yearly}

  public static Long[] getBinsArray(String timeRange) {
    TIME_RANGES timeRangeEnum = TIME_RANGES.valueOf(timeRange);
    switch (timeRangeEnum) {
      case daily:
        return daily_binsArray.clone();
      case weekly:
        return weekly_binsArray.clone();
      case monthly:
        return monthly_binsArray.clone();
      case yearly:
        return yearly_binsArray.clone();
      default:
        return weekly_binsArray.clone();
    }
  }

  public static List<String> getKeys(String timeRange) {
    TIME_RANGES timeRangeEnum = TIME_RANGES.valueOf(timeRange);
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
  private static final List<Long> daily_keys0 = LongStream.range(1, 365).boxed()
      .collect(Collectors.toList());
  private static final List<String> daily_keys = daily_keys0.stream().map(k -> k + " Days")
      .collect(Collectors.toList());
  private static final Long[] daily_binsArray = daily_keys0.stream()
      .mapToLong(TimeUnit.DAYS::toMillis).boxed().toArray(Long[]::new);

  /* We can lower the number of days to improve performance on multi-filter bin'ing. */
  private static final List<Long> weekly_keys0 = LongStream.range(1, 50).map(l -> l * 7).boxed()
      .collect(Collectors.toList());
  private static final List<String> weekly_keys = weekly_keys0.stream().map(k -> k / 7 + " Weeks")
      .collect(Collectors.toList());
  private static final Long[] weekly_binsArray = weekly_keys0.stream()
      .mapToLong(TimeUnit.DAYS::toMillis).boxed().toArray(Long[]::new);

  /* We can lower the number of days to improve performance on multi-filter bin'ing. */
  private static final List<Long> monthly_keys0 = LongStream.range(1, 24).map(l -> l * 30).boxed()
      .collect(Collectors.toList());
  private static final List<String> monthly_keys = monthly_keys0.stream()
      .map(k -> k / 30 + " Months").collect(Collectors.toList());
  private static final Long[] monthly_binsArray = monthly_keys0.stream()
      .mapToLong(TimeUnit.DAYS::toMillis).boxed().toArray(Long[]::new);

  /* We can lower the number of days to improve performance on multi-filter bin'ing. */
  private static final List<Long> yearly_keys0 = LongStream.range(1, 5).map(l -> l * 365).boxed()
      .collect(Collectors.toList());
  private static final List<String> yearly_keys = yearly_keys0.stream().map(k -> k / 365 + " Years")
      .collect(Collectors.toList());
  private static final Long[] yearly_binsArray = yearly_keys0.stream()
      .mapToLong(TimeUnit.DAYS::toMillis).boxed().toArray(Long[]::new);
}
