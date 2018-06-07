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

public class VirtualINode {

  private VirtualINode parent;
  private String data;
  private int score;

  List<VirtualINode> children;

  public VirtualINode(VirtualINode parent, String nodeValue) {
    this.parent = parent;
    children = new ArrayList<>();
    data = nodeValue;
    score = 0;
  }

  public VirtualINode parent() {
    return parent;
  }

  public int score() {
    return score;
  }

  public boolean isRoot() {
    return parent == null;
  }

  public String path() {
    if (isRoot()) {
      return "/";
    }

    StringBuilder path = new StringBuilder(data);
    VirtualINode current = this;
    VirtualINode next;
    while ((next = current.parent()) != null) {
      path.insert(0, next.data + "/");
      current = next;
    }
    return path.toString();
  }

  public void incrementScore() {
    score++;
  }

  public void addChild(VirtualINode child) {
    children.add(child);
    child.incrementScore();
  }

  public VirtualINode getChild(String element) {
    for (VirtualINode child : children) {
      if (child.data.equals(element)) {
        return child;
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return data;
  }
}
