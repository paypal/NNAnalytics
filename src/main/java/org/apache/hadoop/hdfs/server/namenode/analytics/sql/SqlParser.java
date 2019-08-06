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

package org.apache.hadoop.hdfs.server.namenode.analytics.sql;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import org.apache.hadoop.hdfs.server.namenode.Constants;

public class SqlParser {

  private String set;
  private String filters;
  private String type;
  private String sum;
  private String find;
  private Integer limit;

  private CCJSqlParserManager parser;

  public SqlParser() {
    parser = new CCJSqlParserManager();
  }

  /**
   * Describes either the files or directories options available.
   *
   * @param describe The entity to describe. Either files or dirs.
   * @return The JSON string dictating the options available.
   */
  public String describeInJson(String describe) {
    Gson gson = new Gson();
    Type type = new TypeToken<Map<String, List<String>>>() {}.getType();
    if (describe.toUpperCase().contains("DESCRIBE")) {
      boolean filesCheck = describe.toUpperCase().contains("FILES");
      boolean dirsCheck = describe.toUpperCase().contains("DIRS");
      if (filesCheck) {
        Map<String, List<String>> filesDescription =
            new LinkedHashMap<String, List<String>>() {
              {
                put(
                    "WHERE_CONDITIONS",
                    Constants.FILTER_FILE.stream().map(Enum::name).collect(Collectors.toList()));
                put(
                    "GROUP_BYS",
                    Constants.TYPE_FILE.stream().map(Enum::name).collect(Collectors.toList()));
                put(
                    "SUM_FIELDS",
                    Constants.SUM_FILE.stream().map(Enum::name).collect(Collectors.toList()));
                put(
                    "MIN_MAX_AVG_FIELDS",
                    Constants.FIND_FILE.stream().map(Enum::name).collect(Collectors.toList()));
              }
            };
        return gson.toJson(filesDescription, type);
      }
      if (dirsCheck) {
        Map<String, List<String>> dirsDescription =
            new LinkedHashMap<String, List<String>>() {
              {
                put(
                    "WHERE_CONDITIONS",
                    Constants.FILTER_DIR.stream().map(Enum::name).collect(Collectors.toList()));
                put(
                    "GROUP_BYS",
                    Constants.TYPE_DIR.stream().map(Enum::name).collect(Collectors.toList()));
                put(
                    "SUM_FIELDS",
                    Constants.SUM_DIR.stream().map(Enum::name).collect(Collectors.toList()));
                put(
                    "MIN_MAX_AVG_FIELDS",
                    Constants.FIND_DIR.stream().map(Enum::name).collect(Collectors.toList()));
              }
            };
        return gson.toJson(dirsDescription, type);
      }
    }
    throw new IllegalArgumentException();
  }

  /**
   * The main method to parse the SQL statement into an NNA-understandable query.
   *
   * @param statement The SQL statement.
   * @throws JSQLParserException - if sql is unreadable
   */
  public void parse(String statement) throws JSQLParserException {
    Statement parse = parser.parse(new StringReader(statement));
    INodeSqlStatementVisitor inodeVisitor = new INodeSqlStatementVisitor();
    parse.accept(inodeVisitor);
    set = inodeVisitor.set.toLowerCase();
    filters = String.join(",", inodeVisitor.filters);
    sum = inodeVisitor.sum;
    find = inodeVisitor.find;
    type = inodeVisitor.type;
    limit = inodeVisitor.limit;
  }

  public String getINodeSet() {
    return set;
  }

  public String getFilters() {
    return filters;
  }

  public String getSum() {
    return sum;
  }

  public String getFind() {
    return find;
  }

  public String getType() {
    return type;
  }

  public Integer getLimit() {
    return limit;
  }

  public boolean getUseRawTimestamps() {
    return false;
  }

  public Integer getParentDirDepth() {
    return 3;
  }

  public String getTimeRange() {
    return "monthly";
  }
}
