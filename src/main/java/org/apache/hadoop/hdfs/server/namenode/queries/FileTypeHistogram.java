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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileTypeHistogram {

  private enum Types {
    UNKNOWN,
    PART_R,
    PART_M,
    PARQUET,
    AVRO,
    SNAPPY,
    TAR,
    GZ,
    GZIP,
    LOG,
    TXT,
    _SUCCESS,
    _DONE,
    JSON,
    XML,
    DAT,
    DATA,
    INDEX,
    ORC,
    JAR,
    ZIP,
    DTDONE,
    DONE,
    JHIST
  }

  private static final Map<String, String> startsWithMap =
      new HashMap<String, String>() {
        {
          put("part_r", Types.PART_R.name());
          put("part_m", Types.PART_M.name());
        }
      };

  private static final Map<String, String> equalsMap =
      new HashMap<String, String>() {
        {
          put("_SUCCESS", Types._SUCCESS.name());
          put("_DONE", Types._DONE.name());
        }
      };

  private static final Map<String, String> suffixExtMap =
      new HashMap<String, String>() {
        {
          put(".txt", Types.TXT.name());
          put(".log", Types.LOG.name());
          put(".avro", Types.AVRO.name());
          put(".snappy", Types.SNAPPY.name());
          put(".parquet", Types.PARQUET.name());
          put(".gz", Types.GZ.name());
          put(".tar", Types.TAR.name());
          put(".json", Types.JSON.name());
          put(".xml", Types.XML.name());
          put(".dat", Types.DAT.name());
          put(".index", Types.INDEX.name());
          put(".orc", Types.ORC.name());
          put(".jar", Types.JAR.name());
          put(".zip", Types.ZIP.name());
          put(".gzip", Types.GZIP.name());
          put(".dtdone", Types.DTDONE.name());
          put(".done", Types.DONE.name());
          put(".jhist", Types.JHIST.name());
          put(".data", Types.DATA.name());
        }
      };

  public static final List<String> keys =
      Collections.unmodifiableList(
          Arrays.stream(Types.values()).map(Enum::name).collect(Collectors.toList()));

  /**
   * Method for determining the file type based on the file name.
   *
   * @param name the name of the file
   * @return the type of the file
   */
  public static String determineType(String name) {
    String key;
    if ((key = equalsMap.get(name)) != null) {
      return key;
    } else if (name.length() > 6 && (key = startsWithMap.get(name.substring(0, 7))) != null) {
      return key;
    } else {
      int extIndex = name.lastIndexOf(".");
      if (extIndex != -1
          && (key = suffixExtMap.get(name.substring(extIndex, name.length()))) != null) {
        return key;
      }
    }
    return Types.UNKNOWN.name();
  }
}
