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
import java.util.Arrays;
import java.util.List;

public class VirtualINode {

  List<VirtualINode> childs;
  List<VirtualINode> leafs;
  private String data;
  private String incrementalPath;

  public VirtualINode(String nodeValue, String incrementalPath) {
    childs = new ArrayList<>();
    leafs = new ArrayList<>();
    data = nodeValue;
    this.incrementalPath = incrementalPath;
  }

  public String path() {
    return incrementalPath;
  }

  public void addElement(String currentPath, String[] list) {

    // Avoid first element that can be an empty string if you split a string that has a starting
    // slash as /sd/card/
    while (list[0] == null || list[0].equals("")) {
      list = Arrays.copyOfRange(list, 1, list.length);
    }

    VirtualINode currentChild = new VirtualINode(list[0], currentPath + "/" + list[0]);
    if (list.length == 1) {
      leafs.add(currentChild);
      return;
    } else {
      int index = childs.indexOf(currentChild);
      if (index == -1) {
        childs.add(currentChild);
        currentChild.addElement(
            currentChild.incrementalPath, Arrays.copyOfRange(list, 1, list.length));
      } else {
        VirtualINode nextChild = childs.get(index);
        nextChild.addElement(
            currentChild.incrementalPath, Arrays.copyOfRange(list, 1, list.length));
      }
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof VirtualINode)) {
      return false;
    }
    VirtualINode cmpObj = (VirtualINode) obj;
    return incrementalPath.equals(cmpObj.incrementalPath) && data.equals(cmpObj.data);
  }

  @Override
  public int hashCode() {
    int result = childs != null ? childs.hashCode() : 0;
    result = 31 * result + (leafs != null ? leafs.hashCode() : 0);
    result = 31 * result + (data != null ? data.hashCode() : 0);
    result = 31 * result + (incrementalPath != null ? incrementalPath.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return data;
  }
}
