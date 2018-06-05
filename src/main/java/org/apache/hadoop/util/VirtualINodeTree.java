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

package org.apache.hadoop.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class VirtualINodeTree {

  private VirtualINode root;

  public VirtualINodeTree() {
    this.root = new VirtualINode("", "");
  }

  public void addElement(String elementValue) {
    String[] list = elementValue.split("/");

    // latest element of the list is the filename.extrension
    root.addElement(root.path(), list);
  }

  public List<VirtualINode> getCommonRoots() {
    List<VirtualINode> list = new ArrayList<>();

    List<VirtualINode> childs = root.childs;
    List<VirtualINode> leafs = root.leafs;
    list.addAll(leafs);
    for (VirtualINode child1 : childs) {
      List<VirtualINode> childs1 = child1.childs;
      List<VirtualINode> leafs1 = child1.leafs;
      list.addAll(leafs1);
      for (VirtualINode child2 : childs1) {
        if (child2.leafs.size() <= 0 && child2.childs.size() == 0) {
          list.add(child2);
          continue;
        }
        VirtualINode current = child2;
        while (current.leafs.size() <= 0) {
          current = current.childs.get(0);
        }
        list.add(current);
      }
    }

    return list;
  }

  public List<String> getCommonRootsAsStrings() {
    return getCommonRoots().stream().map(VirtualINode::path).collect(Collectors.toList());
  }
}
