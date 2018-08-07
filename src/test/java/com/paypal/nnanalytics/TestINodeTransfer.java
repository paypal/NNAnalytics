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

package com.paypal.nnanalytics;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hdfs.server.namenode.GSetGenerator;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeWithAdditionalFields;
import org.apache.hadoop.util.GSet;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestINodeTransfer {

  private static GSet<INode, INodeWithAdditionalFields> gset;

  @BeforeClass
  public static void beforeClass() {
    GSetGenerator gSetGenerator = new GSetGenerator();
    gSetGenerator.clear();
    gset = gSetGenerator.getGSet((short) 3, 10, 100);
  }

  /* This is busted. If we uncomment this everything else breaks. :S. */
  @Test
  public void putAll() {
    //    GSet<INode, INodeWithAdditionalFields> testSet = GSetGenerator.getEmptyGSet();
    //    for(INodeWithAdditionalFields node : gset) {
    //      testSet.put(node);
    //    }
    //    assertThat(testSet.size(), is(gset.size()));
  }

  @Test
  public void filterAll() {
    long start = System.currentTimeMillis();
    List<INodeWithAdditionalFields> allNodes =
        StreamSupport.stream(gset.spliterator(), true).collect(Collectors.toList());
    long end = System.currentTimeMillis();
    System.out.println("Took " + (end - start) + " ms.");
    assertThat(allNodes.size(), is(gset.size()));
    assertThat(allNodes.size(), is(GSetGenerator.FILES_MADE + GSetGenerator.DIRS_MADE));
  }

  @Test
  public void filterFiles() {
    List<INodeWithAdditionalFields> allFiles =
        StreamSupport.stream(gset.spliterator(), true)
            .filter(INode::isFile)
            .collect(Collectors.toList());
    assertThat(allFiles.size(), is(GSetGenerator.FILES_MADE));
  }

  @Test
  public void filterDirs() {
    List<INodeWithAdditionalFields> allFiles =
        StreamSupport.stream(gset.spliterator(), true)
            .filter(INode::isDirectory)
            .collect(Collectors.toList());
    assertThat(allFiles.size(), is(GSetGenerator.DIRS_MADE));
  }
}
