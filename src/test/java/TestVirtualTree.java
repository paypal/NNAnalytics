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

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.util.List;
import org.apache.hadoop.util.VirtualINodeTree;
import org.junit.Before;
import org.junit.Test;

/**
 * NNA utilizes a common ancestory discovery algorithm from {@link VirtualINodeTree} as an
 * optimization for analyzing specific sets of directories.
 *
 * <p>This test is a simple demonstration of finding common ancestors to run queries against so that
 * NNA can quickly re-iterate the query on children rather than start over every time.
 */
public class TestVirtualTree {

  private static VirtualINodeTree tree;

  @Before
  public void before() {
    tree = new VirtualINodeTree();
  }

  @Test
  public void testTree1() {
    String slist[] =
        new String[] {
          "/A/B/1",
          "/A/B/2",
          "/A/B/C/D/1",
          "/A/B/C/D/2",
          "/A/B/C/3",
          "/A/B/C/4",
          "/A/D/E/1",
          "/A/D/E/1/2",
          "/E/C/1",
          "/E/C/2",
          "/E/C/3",
          "/E/H/1",
          "/E/H/2",
        };

    for (String data : slist) {
      tree.addElement(data);
    }

    List<String> commonRoots = tree.getCommonRootsAsStrings();

    System.out.println("COMMON ROOTS::");
    for (String commonRoot : commonRoots) {
      System.out.println(commonRoot);
    }

    assertThat(commonRoots.size(), is(4));
    assertThat(commonRoots, hasItem("/A/B"));
    assertThat(commonRoots, hasItem("/A/D/E"));
    assertThat(commonRoots, hasItem("/E/C"));
    assertThat(commonRoots, hasItem("/E/H"));
  }

  @Test
  public void testTree2() {
    String slist[] =
        new String[] {
          "/A/P/1",
          "/A/P/2",
          "/A/P/3/1",
          "/A/P/3/2",
          "/A/P/3/1/4",
          "/A/Q/T/1",
          "/A/Q/T/2",
          "/H/D/1",
          "/E/F/1",
          "/E/F/2",
        };

    for (String data : slist) {
      tree.addElement(data);
    }

    List<String> commonRoots = tree.getCommonRootsAsStrings();

    System.out.println("COMMON ROOTS::");
    for (String commonRoot : commonRoots) {
      System.out.println(commonRoot);
    }

    assertThat(commonRoots.size(), is(4));
    assertThat(commonRoots, hasItem("/A/P"));
    assertThat(commonRoots, hasItem("/A/Q/T"));
    assertThat(commonRoots, hasItem("/H/D"));
    assertThat(commonRoots, hasItem("/E/F"));
  }
}
