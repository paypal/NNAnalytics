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

import static org.apache.hadoop.hdfs.server.namenode.NNAConstants.CHARSET;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.util.StreamingGZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class OperationLog {

  public static final Logger LOG = LoggerFactory.getLogger(OperationLog.class.getName());

  private static final long TEN_MEGABYTES = 10L * 1024L * 1024L;

  private final String identity;
  private final String owner;
  private final String query;
  private final File log;
  private final boolean gzipLog;

  private OutputStream fileStream;
  private boolean isClosed = false;

  OperationLog(String identity, String logBaseDir, String query, String owner, boolean gzipLog) {
    this.identity = identity;
    this.owner = owner;
    this.query = query;
    this.gzipLog = gzipLog;

    final String logPath = logBaseDir + "/" + identity + ".opLog" + ((gzipLog) ? ".gz" : "");
    this.log = new File(logPath);
  }

  void startLog() {
    // Start with creating new log file.
    if (!log.exists()) {
      try {
        boolean created = log.createNewFile();
        if (!created) {
          throw new IllegalStateException(
              "Could not create new file. Failing op: " + identity + ".");
        }
      } catch (IOException e) {
        throw new IllegalStateException(
            "Issue when creating new file. Failing op: " + identity + ".", e);
      }
    } else {
      throw new IllegalStateException(
          "Log should not already exist. Failing op: " + identity + ".");
    }
    // Ensure file stream is not already open.
    if (fileStream != null) {
      throw new IllegalStateException(
          "Log should not already be open. Failing op: " + identity + ".");
    }
    // Ensure there is enough space for new log.
    try {
      checkSpace();
    } catch (Exception e) {
      try {
        // Delete log if there is not enough space left.
        FileUtils.forceDelete(log);
      } catch (IOException ioE) {
        throw new IllegalStateException(
            "Could not operation log on failure! Failing op: " + identity + ".", ioE);
      }
    }
    try {
      FileOutputStream plainTextStream =
          new FileOutputStream(log) {
            @Override
            public void close() throws IOException {
              isClosed = true;
              super.close();
            }
          };
      if (gzipLog) {
        LOG.info("Will write log for large op: {}, as GZIP.", identity);
        fileStream = new StreamingGZIPOutputStream(plainTextStream);
      } else {
        fileStream = plainTextStream;
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "Could not open file stream. Failing op: " + identity + ".", e);
    }
    try {
      fileStream.write(
          ("Starting log for operation with identity: "
                  + identity
                  + ", by owner: "
                  + owner
                  + ".\n"
                  + "Query: "
                  + query
                  + ".\n")
              .getBytes(CHARSET));
    } catch (IOException e) {
      throw new IllegalStateException(
          "Could not write operation log header. Failing op: " + identity + ".", e);
    }
  }

  void logOp(String path, String inodeType, boolean success) {
    checkSpace();
    if (isClosed()) {
      throw new IllegalStateException("Log is closed. Failing op: " + identity + ".");
    }
    String outcome = (success) ? "SUCCESS" : "FAILURE";
    try {
      fileStream.write((outcome + " |  " + path + " | " + inodeType + "\n").getBytes(CHARSET));
      fileStream.flush();
    } catch (IOException e) {
      throw new IllegalStateException(
          "Could not write operation log header. Failing op: " + identity + ".", e);
    }
  }

  void close() {
    if (isClosed() || fileStream == null) {
      return;
    }
    try {
      fileStream.write("DONE.".getBytes(CHARSET));
    } catch (IOException e) {
      throw new IllegalStateException(
          "Could not write operation log tail. Closing op: " + identity + ".", e);
    }
    IOUtils.closeQuietly(fileStream);
    isClosed = true;
  }

  private void checkSpace() {
    long freeSpace = log.getParentFile().getFreeSpace();
    if (freeSpace <= (TEN_MEGABYTES)) {
      throw new IllegalStateException(
          "Not enough space left to log. Failing op: " + identity + ".");
    }
  }

  private boolean isClosed() {
    return isClosed;
  }
}
