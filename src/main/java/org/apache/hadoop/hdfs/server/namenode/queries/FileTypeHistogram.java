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
    C000,
    PART,
    PART_R,
    PART_M,
    PARQUET,
    AVRO,
    SNAPPY,
    TAR,
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
    JHIST,
    PIG_HEADER,
    PIG_SCHEMA,
    BATCH,
    TSV,
    CSV,
    BZ2,
    TODO,
    HTML,
    JS,
    YAML,
    SPLIT,
    SPLITMETAINFO,
    MP3,
    WAV,
    JPEG,
    PNG,
    WAR,
    PYTHON,
    JAVA,
    CLASS,
    LOCK,
    APP_LOG,
    AVSC,
    NAMES,
    NN,
    GBT,
    TEMP,
    LZ4,
    LZO,
    DELTA,
    SNAPSHOT,
    SQL,
    PENDING,
    SUCCESS,
    GIVEUP,
    INPROGRESS,
    ENTITY_LOG,
    DOMAIN_LOG,
    SUMMARY_LOG,
    _MASTERINDEX,
    _INDEX
  }

  private static final Map<String, String> startsWithMap =
      new HashMap<String, String>() {
        {
          put("part-0", Types.PART.name());
          put("part_0", Types.PART.name());
          put("part_r", Types.PART_R.name());
          put("part-r", Types.PART_R.name());
          put("part_m", Types.PART_M.name());
          put("part-m", Types.PART_M.name());
          put("entitylog-", Types.ENTITY_LOG.name());
          put("domainlog-", Types.DOMAIN_LOG.name());
          put("summarylog", Types.SUMMARY_LOG.name());
        }
      };

  private static final Map<String, String> equalsMap =
      new HashMap<String, String>() {
        {
          put(".pig_header", Types.PIG_HEADER.name());
          put(".pig_schema", Types.PIG_SCHEMA.name());
          put("_SUCCESS", Types._SUCCESS.name());
          put("_DONE", Types._DONE.name());
          put("_index", Types._INDEX.name());
          put("_masterindex", Types._MASTERINDEX.name());
        }
      };

  private static final Map<String, String> suffixExtMap =
      new HashMap<String, String>() {
        {
          put("_45454", Types.APP_LOG.name());
          put(".batch", Types.BATCH.name());
          put(".txt", Types.TXT.name());
          put(".tsv", Types.TSV.name());
          put(".bz2", Types.BZ2.name());
          put(".csv", Types.CSV.name());
          put(".log", Types.LOG.name());
          put(".avro", Types.AVRO.name());
          put(".avsc", Types.AVSC.name());
          put(".snappy", Types.SNAPPY.name());
          put(".parquet", Types.PARQUET.name());
          put(".gz", Types.GZIP.name());
          put(".tar", Types.TAR.name());
          put(".json", Types.JSON.name());
          put(".xml", Types.XML.name());
          put(".index", Types.INDEX.name());
          put(".todo", Types.TODO.name());
          put(".html", Types.HTML.name());
          put(".js", Types.JS.name());
          put(".orc", Types.ORC.name());
          put(".jar", Types.JAR.name());
          put(".zip", Types.ZIP.name());
          put(".gzip", Types.GZIP.name());
          put(".names", Types.NAMES.name());
          put(".nn", Types.NN.name());
          put(".gbt", Types.GBT.name());
          put(".tmp", Types.TEMP.name());
          put(".temp", Types.TEMP.name());
          put(".lz4", Types.LZ4.name());
          put(".lzo", Types.LZO.name());
          put(".delta", Types.DELTA.name());
          put(".snapshot", Types.SNAPSHOT.name());
          put(".sql", Types.SQL.name());
          put(".dtdone", Types.DTDONE.name());
          put(".done", Types.DONE.name());
          put(".jhist", Types.JHIST.name());
          put(".dat", Types.DAT.name());
          put(".data", Types.DATA.name());
          put(".yaml", Types.YAML.name());
          put(".yml", Types.YAML.name());
          put(".split", Types.SPLIT.name());
          put(".splitmetainfo", Types.SPLITMETAINFO.name());
          put(".mp3", Types.MP3.name());
          put(".wav", Types.WAV.name());
          put(".jpg", Types.JPEG.name());
          put(".jpeg", Types.JPEG.name());
          put(".png", Types.PNG.name());
          put(".war", Types.WAR.name());
          put(".py", Types.PYTHON.name());
          put(".java", Types.JAVA.name());
          put(".class", Types.CLASS.name());
          put(".lock", Types.LOCK.name());
          put(".pending", Types.PENDING.name());
          put(".PENDING", Types.PENDING.name());
          put(".success", Types.SUCCESS.name());
          put(".SUCCESS", Types.SUCCESS.name());
          put(".giveup", Types.GIVEUP.name());
          put(".GIVEUP", Types.GIVEUP.name());
          put(".inprogress", Types.INPROGRESS.name());
          put(".INPROGRESS", Types.INPROGRESS.name());
        }
      };

  public static final List<String> keys =
      Collections.unmodifiableList(
          Arrays.stream(Types.values()).map(Enum::name).collect(Collectors.toList()));

  public static final Map<String, Integer> typeToIdMap =
      keys.stream().mapToInt(keys::indexOf).boxed().collect(Collectors.toMap(keys::get, v -> v));

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
    } else if (name.length() > 6 && (key = startsWithMap.get(name.substring(0, 6))) != null) {
      return key;
    } else if (name.length() > 10 && (key = startsWithMap.get(name.substring(0, 10))) != null) {
      return key;
    } else {
      int extIndex = name.lastIndexOf(".");
      if (extIndex != -1 && (key = suffixExtMap.get(name.substring(extIndex))) != null) {
        return key;
      }
      extIndex = name.lastIndexOf("_");
      if (extIndex != -1 && (key = suffixExtMap.get(name.substring(extIndex))) != null) {
        return key;
      }
    }
    return Types.UNKNOWN.name();
  }
}
