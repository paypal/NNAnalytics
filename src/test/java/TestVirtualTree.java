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
import java.util.Set;
import org.apache.hadoop.util.VirtualINode;
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
  public void testGetINode() {
    String slist[] = new String[] {"/A/B/C/D/1", "/A/B/C/F/1", "/A/B/C/E/1", "/A/B/C/E/2"};

    for (String data : slist) {
      tree.addElement(data);
    }

    VirtualINode element = tree.getElement("/A/B/C/D/1");
    assertThat(element.score(), is(1));
    assertThat(element.path(), is("/A/B/C/D/1"));
  }

  @Test
  public void testScore() {
    String slist[] = new String[] {"/A/B/C/D/1", "/A/B/C/F/1", "/A/B/C/E/1", "/A/B/C/E/2"};

    for (String data : slist) {
      tree.addElement(data);
    }

    VirtualINode element;
    element = tree.getElement("/A");
    assertThat(element.score(), is(1));
    element = tree.getElement("/A/B");
    assertThat(element.score(), is(1));
    element = tree.getElement("/A/B/C");
    assertThat(element.score(), is(3));
    element = tree.getElement("/A/B/C/E");
    assertThat(element.score(), is(2));
  }

  @Test
  public void testOneCommonAncestor() {
    String slist[] = new String[] {"/A/B/C/D/1", "/A/B/C/F/1", "/A/B/C/E/1", "/A/B/C/E/2"};

    for (String data : slist) {
      tree.addElement(data);
    }

    List<String> commonRoots = dumpNodeDetails();
    assertThat(commonRoots.size(), is(1));
    assertThat(commonRoots, hasItem("/A/B/C"));
  }

  @Test
  public void testBigDepth() {
    String slist[] =
        new String[] {
          "/A",
          "/A/Q",
          "/A/Q/P",
          "/A/Q/P/Z",
          "/A/Q/P/Z/D",
          "/A/Q/P/Z/D/G",
          "/A/Q/P/Z/D/G/W",
          "/A/Q/P/Z/D/G/W/L",
          "/A/Q/P/Z/D/G/W/L/U",
          "/A/Q/P/Z/D/G/W/L/U/B",
          "/A/Q/P/Z/D/G/W/L/U/B/C",
          "/A/Q/P/Z/D/G/W/L/U/B/C/A",
        };

    for (String data : slist) {
      tree.addElement(data);
    }

    List<String> commonRoots = dumpNodeDetails();
    assertThat(commonRoots.size(), is(1));
    assertThat(commonRoots, hasItem("/A"));
  }

  @Test
  public void testBigDepthReversed() {
    String slist[] =
        new String[] {
          "/A/Q/P/Z/D/G/W/L/U/B/C/A",
          "/A/Q/P/Z/D/G/W/L/U/B/C",
          "/A/Q/P/Z/D/G/W/L/U/B",
          "/A/Q/P/Z/D/G/W/L/U",
          "/A/Q/P/Z/D/G/W/L",
          "/A/Q/P/Z/D/G/W",
          "/A/Q/P/Z/D/G",
          "/A/Q/P/Z/D",
          "/A/Q/P/Z",
          "/A/Q/P",
          "/A/Q",
          "/A",
        };

    for (String data : slist) {
      tree.addElement(data);
    }

    List<String> commonRoots = dumpNodeDetails();
    assertThat(commonRoots.size(), is(1));
    assertThat(commonRoots, hasItem("/A"));
  }

  @Test
  public void testBigBreadth() {
    String slist[] =
        new String[] {
          "/A/Q/P/1",
          "/A/Q/P/2",
          "/A/Q/P/3/1",
          "/A/Q/P/3/2",
          "/A/Q/P/3/1/4",
          "/A/Q/T/1",
          "/A/Q/T/2",
          "/A/Q/Z/1",
          "/A/Q/Z/31",
          "/A/Q/I/29",
          "/A/Q/G/29",
          "/A/Q/W/29",
          "/A/Q/L/29",
          "/A/Q/M/29",
          "/A/Q/N/29",
        };

    for (String data : slist) {
      tree.addElement(data);
    }

    List<String> commonRoots = dumpNodeDetails();
    assertThat(commonRoots.size(), is(1));
    assertThat(commonRoots, hasItem("/A/Q"));
  }

  @Test
  public void testBreadth1() {
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

    List<String> commonRoots = dumpNodeDetails();
    assertThat(commonRoots.size(), is(4));
    assertThat(commonRoots, hasItem("/A/P"));
    assertThat(commonRoots, hasItem("/A/Q/T"));
    assertThat(commonRoots, hasItem("/H/D/1"));
    assertThat(commonRoots, hasItem("/E/F"));
  }

  @Test
  public void testBreadth2() {
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

    List<String> commonRoots = dumpNodeDetails();
    assertThat(commonRoots.size(), is(3));
    assertThat(commonRoots, hasItem("/A"));
    assertThat(commonRoots, hasItem("/E/C"));
    assertThat(commonRoots, hasItem("/E/H"));
  }

  @Test
  public void testIsolation() {
    String slist[] =
        new String[] {"/A/B/C/D", "/B/C/D/E", "/C/D/E/F", "/E/F/G/H", "/F/G/H/I", "/J/K/L/M"};

    for (String data : slist) {
      tree.addElement(data);
    }

    List<String> commonRoots = dumpNodeDetails();
    assertThat(commonRoots.size(), is(6));
    assertThat(commonRoots, hasItem("/A/B/C/D"));
    assertThat(commonRoots, hasItem("/B/C/D/E"));
    assertThat(commonRoots, hasItem("/C/D/E/F"));
    assertThat(commonRoots, hasItem("/E/F/G/H"));
    assertThat(commonRoots, hasItem("/F/G/H/I"));
    assertThat(commonRoots, hasItem("/J/K/L/M"));
  }

  private List<String> dumpNodeDetails() {
    Set<VirtualINode> nodes = tree.getAllNodes();
    for (VirtualINode node : nodes) {
      System.out.println(node.path());
      System.out.println(node.score());
    }
    List<String> commonAncestors = tree.getCommonAncestorsAsStrings();
    System.out.println("COMMON ANCESTORS:");
    for (String commonRoot : commonAncestors) {
      System.out.println(commonRoot);
    }
    return commonAncestors;
  }
}
