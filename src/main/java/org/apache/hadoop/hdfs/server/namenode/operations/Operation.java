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

package org.apache.hadoop.hdfs.server.namenode.operations;

import java.util.List;

interface Operation {

  @Override
  int hashCode();

  @Override
  boolean equals(Object obj);

  void initialize();

  void close();

  /**
   * Get the list of paths last enacted upon.
   *
   * @param numOfLast number of at most paths
   * @return the list of, at most length 'numOfLast', inode paths that operations were attempted on.
   */
  List<String> lastPerformed(int numOfLast);

  /**
   * Returns the next INode path to be operated on.
   *
   * @return the next inode path that operation will be performed on.
   */
  String upNext();

  /**
   * This is the main operation to perform. Always enacts against the inode specified by upNext().
   * Increments to next inode afterwards.
   *
   * @return true if success, false otherwise.
   */
  boolean performOp();

  /**
   * Whether there is a next INode to operate on or not.
   *
   * @return true if has there is a next inode to perform on, false otherwise.
   */
  boolean hasNext();

  /** Aborts the operation. Will not "undo". */
  void abort();

  /**
   * Get the identifier for this operation sequence.
   *
   * @return the identity UUID that represents this operation.
   */
  String identity();

  /**
   * Get the total number of INodes to be operated on.
   *
   * @return the total number of inodes to operate on for this run.
   */
  int totalToPerform();

  /**
   * Get the total number of INodes operated on.
   *
   * @return the number of inodes operated on already.
   */
  int numPerformed();

  /**
   * Return the full query.
   *
   * @return the URI path and query parameters that created this operation.
   */
  String query();

  /**
   * Return the owner of the operation.
   *
   * @return the username that submitted this operation.
   */
  String owner();
}
