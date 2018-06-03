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
   * @param numOfLast number of at most paths
   * @return the list of, at most length 'numOfLast', inode paths that operations were attempted on.
   */
  List<String> lastPerformed(int numOfLast);

  /**
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
   * @return true if has there is a next inode to perform on, false otherwise.
   */
  boolean hasNext();

  /**
   * Aborts the operation. Will not "undo".
   */
  void abort();

  /**
   * @return the identity UUID that represents this operation.
   */
  String identity();

  /**
   * @return the total number of inodes to operate on for this run.
   */
  int totalToPerform();

  /**
   * @return the number of inodes operated on already.
   */
  int numPerformed();

  /**
   * @return the URI path and query parameters that created this operation.
   */
  String query();

  /**
   * @return the username that submitted this operation.
   */
  String owner();
}
