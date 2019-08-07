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

package org.apache.hadoop.hdfs.server.namenode.analytics;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import net.sf.jsqlparser.JSQLParserException;
import org.apache.hadoop.hdfs.server.namenode.analytics.sql.SqlParser;
import org.junit.Before;
import org.junit.Test;

public class TestSQL {

  private SqlParser sqlParser;

  @Before
  public void before() {
    sqlParser = new SqlParser();
  }

  @Test
  public void testGetInodeSet() throws JSQLParserException {
    sqlParser.parse("SELECT * FROM files");
    String inodeSet = sqlParser.getINodeSet();
    assertThat(inodeSet, is(equalTo("files")));
  }

  @Test
  public void testSumFileSizeByUser() throws JSQLParserException {
    sqlParser.parse("select user,sum(fileSize) from files group by user");
    String inodeSet = sqlParser.getINodeSet();
    String filters = sqlParser.getFilters();
    String sum = sqlParser.getSum();
    String find = sqlParser.getFind();
    String type = sqlParser.getType();
    assertThat(inodeSet, is(equalTo("files")));
    assertThat(filters, is(""));
    assertThat(sum, is("fileSize"));
    assertThat(type, is("user"));
    assertThat(find, is(nullValue()));
  }

  @Test
  public void testSumDiskspaceConsumedByAccessTime() throws JSQLParserException {
    sqlParser.parse("select accessTime,sum(diskspaceConsumed) from files group by accessTime");
    String inodeSet = sqlParser.getINodeSet();
    String filters = sqlParser.getFilters();
    String sum = sqlParser.getSum();
    String find = sqlParser.getFind();
    String type = sqlParser.getType();
    assertThat(inodeSet, is(equalTo("files")));
    assertThat(filters, is(""));
    assertThat(sum, is("diskspaceConsumed"));
    assertThat(type, is("accessTime"));
    assertThat(find, is(nullValue()));
  }

  @Test
  public void testGetSingleFileFilter() throws JSQLParserException {
    sqlParser.parse("SELECT * FROM files WHERE fileSize = 0");
    String inodeSet = sqlParser.getINodeSet();
    String filters = sqlParser.getFilters();
    assertThat(inodeSet, is(equalTo("files")));
    assertThat(filters, is("fileSize:eq:0"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupportedGetDoubleFileFilterOr() throws JSQLParserException {
    sqlParser.parse("SELECT * FROM files WHERE fileSize = 0 OR user != pjeli");
  }

  @Test
  public void testGetDoubleFileFilterAnd() throws JSQLParserException {
    sqlParser.parse("SELECT * FROM files WHERE fileSize = 0 AND user != pjeli");
    String inodeSet = sqlParser.getINodeSet();
    String filters = sqlParser.getFilters();
    assertThat(inodeSet, is(equalTo("files")));
    assertThat(filters, is("fileSize:eq:0,user:notEq:pjeli"));
  }

  @Test
  public void testGetSumDoubleFileFilterAnd() throws JSQLParserException {
    sqlParser.parse(
        "SELECT SUM(diskspaceConsumed) FROM files WHERE fileSize = 0 AND user != pjeli");
    String inodeSet = sqlParser.getINodeSet();
    String filters = sqlParser.getFilters();
    String sum = sqlParser.getSum();
    assertThat(inodeSet, is(equalTo("files")));
    assertThat(filters, is("fileSize:eq:0,user:notEq:pjeli"));
    assertThat(sum, is(equalTo("diskspaceConsumed")));
  }

  @Test
  public void testGetCountDoubleFileFilterAnd() throws JSQLParserException {
    sqlParser.parse("SELECT COUNT(*) FROM files WHERE fileSize = 0 AND user != pjeli");
    String inodeSet = sqlParser.getINodeSet();
    String filters = sqlParser.getFilters();
    String sum = sqlParser.getSum();
    assertThat(inodeSet, is(equalTo("files")));
    assertThat(filters, is("fileSize:eq:0,user:notEq:pjeli"));
    assertThat(sum, is(equalTo("count")));
  }

  @Test
  public void testGetMaxDoubleFileFilterAnd() throws JSQLParserException {
    sqlParser.parse(
        "SELECT MAX(diskspaceConsumed) FROM files WHERE fileSize > 0 AND user != pjeli");
    String inodeSet = sqlParser.getINodeSet();
    String filters = sqlParser.getFilters();
    String sum = sqlParser.getSum();
    String find = sqlParser.getFind();
    assertThat(inodeSet, is(equalTo("files")));
    assertThat(filters, is("fileSize:gt:0,user:notEq:pjeli"));
    assertThat(sum, is(nullValue()));
    assertThat(find, is(equalTo("max:diskspaceConsumed")));
  }

  @Test
  public void testGetMaxDoubleFileFilterPathStartsWithAnd() throws JSQLParserException {
    sqlParser.parse(
        "SELECT MAX(diskspaceConsumed) FROM files WHERE path LIKE '/tmp/%' AND user != pjeli");
    String inodeSet = sqlParser.getINodeSet();
    String filters = sqlParser.getFilters();
    String sum = sqlParser.getSum();
    String find = sqlParser.getFind();
    assertThat(inodeSet, is(equalTo("files")));
    assertThat(filters, is("path:startsWith:/tmp/,user:notEq:pjeli"));
    assertThat(sum, is(nullValue()));
    assertThat(find, is(equalTo("max:diskspaceConsumed")));
  }

  @Test
  public void testGetSingleFileGroupByUser() throws JSQLParserException {
    sqlParser.parse("SELECT user,COUNT(*) FROM files WHERE fileSize = 0 GROUP BY user");
    String inodeSet = sqlParser.getINodeSet();
    String filters = sqlParser.getFilters();
    String type = sqlParser.getType();
    String sum = sqlParser.getSum();
    assertThat(inodeSet, is(equalTo("files")));
    assertThat(filters, is("fileSize:eq:0"));
    assertThat(type, is("user"));
    assertThat(sum, is(equalTo("count")));
  }

  @Test
  public void testGetCountDoubleFileFilterAndLimit() throws JSQLParserException {
    sqlParser.parse("SELECT COUNT(*) FROM files WHERE fileSize = 0 AND user != pjeli LIMIT 1000");
    String inodeSet = sqlParser.getINodeSet();
    String filters = sqlParser.getFilters();
    String sum = sqlParser.getSum();
    Integer limit = sqlParser.getLimit();
    assertThat(inodeSet, is(equalTo("files")));
    assertThat(filters, is("fileSize:eq:0,user:notEq:pjeli"));
    assertThat(sum, is(equalTo("count")));
    assertThat(limit, is(1000));
  }

  @Test
  public void testDescribeFiles() {
    String describe_files = sqlParser.describeInJson("DESCRIBE files");
    System.out.println(describe_files);
  }

  @Test
  public void testDescribeDirs() {
    String describe_dirs = sqlParser.describeInJson("DESCRIBE dirs");
    System.out.print(describe_dirs);
  }

  @Test
  public void testShowTables() {
    String show_tables = sqlParser.showTables();
    System.out.print(show_tables);
  }
}
