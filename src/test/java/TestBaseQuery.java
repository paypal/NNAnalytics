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

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hdfs.server.namenode.queries.BaseQuery;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBaseQuery {

  private static BaseQuery baseQuery;

  @BeforeClass
  public static void setUp() {
    baseQuery = new BaseQuery("/test", "test");
  }

  @Test
  public void testHashCode() {
    assertThat(baseQuery.hashCode(), is(not(0)));
    assertThat(new BaseQuery(null, null).hashCode(), is(0));
  }

  @Test
  public void testEquals() {
    assertThat(baseQuery.equals(baseQuery), is(true));
    assertThat(baseQuery.equals(new Object()), is(false));
    assertThat(baseQuery.equals(null), is(false));
    assertThat(new BaseQuery(null, null).equals(baseQuery), is(false));
    assertThat(new BaseQuery("/test", "test").equals(baseQuery), is(true));
  }
}
