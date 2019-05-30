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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class VirtualINodeTree {

  private VirtualINode root;
  private Set<String> paths = new HashSet<>();

  public VirtualINodeTree() {
    this.root = new VirtualINode(null, "");
  }

  private int getNumPathsAdded() {
    return paths.size();
  }

  /**
   * This will add a path to the virtual tree. The path is broken up into its elements by splitting
   * on the '/' separator and adding all non-existing VirtualINodes along the way. If any
   * VirtualINodes along the way already exists, its "score" is incremented.
   *
   * @param path - The full path to add to the tree.
   */
  public void addElement(String path) {
    if (paths.contains(path)) {
      return;
    }

    String[] elements = path.split("/");
    // Avoid first element that can be an empty string if you split a string that has a starting /
    while (elements[0] == null || elements[0].equals("")) {
      elements = Arrays.copyOfRange(elements, 1, elements.length);
    }

    VirtualINode currParent = root;
    for (String element : elements) {
      VirtualINode nextParent = currParent.getChild(element);
      if (nextParent != null) {
        currParent = nextParent;
      } else {
        VirtualINode newChild = new VirtualINode(currParent, element);
        if (!currParent.getChildren().isEmpty()) {
          currParent.incrementScore();
        }
        currParent.addChild(newChild);
        currParent = newChild;
      }
    }

    paths.add(path);
  }

  /**
   * Fetches the VirtualINode that represents the parameter path.
   *
   * @param path - The full path to fetch.
   * @return The VirtualINodes representing the parameter path or null.
   */
  public VirtualINode getElement(String path) {
    String[] elements = path.split("/");

    if (elements.length == 0) {
      return root;
    }

    // Avoid first element that can be an empty string if you split a string that has a starting /
    while (elements[0] == null || elements[0].equals("")) {
      elements = Arrays.copyOfRange(elements, 1, elements.length);
    }

    VirtualINode currParent = root;
    for (String element : elements) {
      VirtualINode nextParent = currParent.getChild(element);
      if (nextParent != null) {
        currParent = nextParent;
      } else {
        return null;
      }
    }
    return currParent;
  }

  /**
   * Get the set of all VirtualINodes in the tree.
   *
   * @return INodeSet of all VirtualINodes in the tree.
   */
  public Set<VirtualINode> getAllNodes() {
    Set<VirtualINode> nodes = new HashSet<>();
    addAllChildren(root, nodes);
    return nodes;
  }

  /**
   * This must return a list of paths that in total will account for all the deepest children of the
   * entire tree. This is vital to NNA directory caching optimization.
   *
   * @see org.apache.hadoop.hdfs.server.namenode.cache.SuggestionsEngine
   * @return set of lowest common ancestor VirtualINodes
   */
  public Set<VirtualINode> getCommonAncestors() {
    Set<VirtualINode> commonAncestors = new HashSet<>();
    for (String path : paths) {
      VirtualINode deepChild = getElement(path);
      VirtualINode currentAncestor = deepChild;
      VirtualINode currentParent = deepChild.parent();
      while (currentParent != null) {
        if (currentParent.isRoot() && getNumPathsAdded() > 1) {
          break;
        }
        if (currentParent.score() > currentAncestor.score()) {
          currentAncestor = currentParent;
        }
        currentParent = currentParent.parent();
      }
      commonAncestors.add(currentAncestor);
    }
    return prunedAncestors(commonAncestors);
  }

  /**
   * This must return a list of paths that in total will account for all the deepest children of the
   * entire tree. This is vital to NNA directory caching optimization.
   *
   * @see org.apache.hadoop.hdfs.server.namenode.cache.SuggestionsEngine
   * @return set of lowest common ancestor String paths
   */
  public List<String> getCommonAncestorsAsStrings() {
    return getCommonAncestors().stream().map(VirtualINode::path).collect(Collectors.toList());
  }

  private void addAllChildren(VirtualINode node, Set<VirtualINode> list) {
    list.add(node);
    if (node.getChildren() != null && !node.getChildren().isEmpty()) {
      list.addAll(node.getChildren());
      for (VirtualINode child : node.getChildren()) {
        addAllChildren(child, list);
      }
    }
  }

  /*
   * O(n^2) implementation for removing longer ancestors.
   * This is used in the case that /A and /A/B show up as ancestors.
   * It is assumed that the total number of added elements for analysis will be small.
   */
  private Set<VirtualINode> prunedAncestors(Set<VirtualINode> commonAncestors) {
    Map<String, VirtualINode> ancestorPaths =
        commonAncestors.stream().collect(Collectors.toMap(VirtualINode::path, Function.identity()));
    Set<String> toRemove = new HashSet<>();
    for (String ancestorPath : ancestorPaths.keySet()) {
      for (String compare : ancestorPaths.keySet()) {
        if (compare.startsWith(ancestorPath) && compare.length() > ancestorPath.length()) {
          toRemove.add(compare);
        }
      }
    }
    toRemove.forEach(ancestorPaths::remove);
    ancestorPaths.values().forEach(VirtualINode::unmark);
    return new HashSet<>(ancestorPaths.values());
  }
}
