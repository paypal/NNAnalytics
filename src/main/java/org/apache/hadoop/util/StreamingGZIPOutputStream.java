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

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

/**
 * Utility class used to streaming big log files from Deletes to GZIP file to attempt to save on
 * space.
 */
public class StreamingGZIPOutputStream extends GZIPOutputStream {

  public StreamingGZIPOutputStream(OutputStream out) throws IOException {
    super(out);
  }

  @Override
  protected void deflate() throws IOException {
    int len = def.deflate(buf, 0, buf.length, Deflater.SYNC_FLUSH);
    if (len > 0) {
      out.write(buf, 0, len);
    }
  }

}