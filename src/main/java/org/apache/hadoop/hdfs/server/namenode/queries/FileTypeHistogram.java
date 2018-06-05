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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FileTypeHistogram {

  private enum TYPES {
    UNKNOWN,
    PART_R,
    PART_M,
    PARQUET,
    AVRO,
    SNAPPY,
    GZ,
    LOG,
    TXT,
    _SUCCESS,
    JSON,
    XML,
    DAT,
    INDEX,
    ORC,
    JAR,
    ZIP,
    DTDONE,
    JHIST
  }

  private static final Pattern part_r_pattern_internal = Pattern.compile("part-r-\\d\\d\\d\\d\\d");
  private static final Pattern part_m_pattern_internal = Pattern.compile("part-m-\\d\\d\\d\\d\\d");
  private static final Matcher part_r_matcher_internal = part_r_pattern_internal.matcher("");
  private static final Matcher part_m_matcher_internal = part_m_pattern_internal.matcher("");

  private static final Function<String, Boolean> part_r_pattern =
      s -> {
        synchronized (part_r_matcher_internal) {
          part_r_matcher_internal.reset(s);
          return part_r_matcher_internal.matches();
        }
      };
  private static final Function<String, Boolean> part_m_pattern =
      s -> {
        synchronized (part_m_matcher_internal) {
          part_m_matcher_internal.reset(s);
          return part_m_matcher_internal.matches();
        }
      };
  private static final Function<String, Boolean> success_pattern = s -> s.equals("_SUCCESS");
  private static final Function<String, Boolean> txt_pattern = s -> s.endsWith(".txt");
  private static final Function<String, Boolean> log_pattern = s -> s.endsWith(".log");
  private static final Function<String, Boolean> avro_pattern = s -> s.endsWith(".avro");
  private static final Function<String, Boolean> snappy_pattern = s -> s.endsWith(".snappy");
  private static final Function<String, Boolean> parquet_pattern = s -> s.endsWith(".parquet");
  private static final Function<String, Boolean> gz_pattern = s -> s.endsWith(".gz");
  private static final Function<String, Boolean> json_pattern = s -> s.endsWith(".json");
  private static final Function<String, Boolean> xml_pattern = s -> s.endsWith(".xml");
  private static final Function<String, Boolean> dat_pattern = s -> s.endsWith(".dat");
  private static final Function<String, Boolean> index_pattern = s -> s.endsWith(".index");
  private static final Function<String, Boolean> orc_pattern = s -> s.endsWith(".orc");
  private static final Function<String, Boolean> jar_pattern = s -> s.endsWith(".jar");
  private static final Function<String, Boolean> zip_pattern = s -> s.endsWith(".zip");
  private static final Function<String, Boolean> dtdone_pattern = s -> s.endsWith(".dtdone");
  private static final Function<String, Boolean> jhist_pattern = s -> s.endsWith(".jhist");

  private static final Map<String, Function<String, Boolean>> patternMap =
      new TreeMap<String, Function<String, Boolean>>() {
        {
          put(TYPES.PART_R.name(), part_r_pattern);
          put(TYPES.PART_M.name(), part_m_pattern);
          put(TYPES._SUCCESS.name(), success_pattern);
          put(TYPES.TXT.name(), txt_pattern);
          put(TYPES.LOG.name(), log_pattern);
          put(TYPES.AVRO.name(), avro_pattern);
          put(TYPES.SNAPPY.name(), snappy_pattern);
          put(TYPES.PARQUET.name(), parquet_pattern);
          put(TYPES.GZ.name(), gz_pattern);
          put(TYPES.JSON.name(), json_pattern);
          put(TYPES.XML.name(), xml_pattern);
          put(TYPES.DAT.name(), dat_pattern);
          put(TYPES.INDEX.name(), index_pattern);
          put(TYPES.ORC.name(), orc_pattern);
          put(TYPES.JAR.name(), jar_pattern);
          put(TYPES.ZIP.name(), zip_pattern);
          put(TYPES.DTDONE.name(), dtdone_pattern);
          put(TYPES.JHIST.name(), jhist_pattern);
        }
      };

  public static final List<String> keys =
      Collections.unmodifiableList(
          Arrays.stream(TYPES.values()).map(Enum::name).collect(Collectors.toList()));

  public static String determineType(String name) {
    for (Map.Entry<String, Function<String, Boolean>> patternEntry : patternMap.entrySet()) {
      if (patternEntry.getValue().apply(name)) {
        return patternEntry.getKey();
      }
    }
    return TYPES.UNKNOWN.name();
  }
}
