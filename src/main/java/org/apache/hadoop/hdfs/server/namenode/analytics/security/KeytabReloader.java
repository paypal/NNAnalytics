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

package org.apache.hadoop.hdfs.server.namenode.analytics.security;

import java.io.IOException;
import java.net.InetAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeytabReloader implements Runnable {

  public static final Logger LOG = LoggerFactory.getLogger(KeytabReloader.class.getName());

  private Configuration conf;
  private volatile boolean shutdown = false;

  public KeytabReloader(Configuration conf) {
    this.conf = conf;
  }

  public void shutdown() {
    shutdown = true;
    Thread.currentThread().interrupt();
  }

  @Override
  public void run() {
    while (!shutdown) {
      // Reload Keytab every 10 minutes.
      try {
        Thread.sleep(10 * 60 * 1000L);
      } catch (InterruptedException ex) {
        LOG.debug("Keytab refresh sleep was interrupted.", ex);
      }
      if (UserGroupInformation.isSecurityEnabled()) {
        try {
          SecurityUtil.login(
              conf,
              DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY,
              DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY,
              InetAddress.getLocalHost().getCanonicalHostName());
          UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
        } catch (IOException ex) {
          LOG.error("Keytab refresh was failed.", ex);
          break;
        }
      }
    }
    LOG.error("Keytab reload service exited; keytab will expire.");
  }
}
