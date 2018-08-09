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

package org.apache.hadoop.security.token.delegation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;

/**
 * This class is used to fetch information from the NameNode's Delegation Tokens in order to
 * determine when a user last made use of HDFS.
 *
 * <p>The use case for this is to eventually discover users that have not been active on the
 * cluster.
 */
public class TokenExtractor {

  private final DelegationTokenSecretManager dtsm;
  private final FSNamesystem fsn;

  /**
   * Constructor.
   *
   * @param dtsm tokensecretmanager from FSNamesystem
   * @param fsn the FSNamesystem
   */
  public TokenExtractor(DelegationTokenSecretManager dtsm, FSNamesystem fsn) {
    this.dtsm = dtsm;
    this.fsn = fsn;
  }

  /**
   * Extract the last seen DelegationTokens from FSNamesystem.
   *
   * @return map of user names to last timestamp of token seen
   */
  public Map<String, Long> getTokenLastLogins() {
    if (fsn == null || dtsm == null) {
      return new HashMap<String, Long>() {
        {
          put("hdfs", System.currentTimeMillis());
        }
      };
    }
    Map<String, Long> lastLogins = new HashMap<>();
    fsn.writeLock();
    try {
      Set<Map.Entry<DelegationTokenIdentifier, DelegationTokenInformation>> entries =
          dtsm.currentTokens.entrySet();
      for (Map.Entry<DelegationTokenIdentifier, DelegationTokenInformation> entry : entries) {
        Text owner = entry.getKey().getOwner();
        Text realUser = entry.getKey().getRealUser();
        String ownerStr = new KerberosName(owner.toString()).getServiceName();
        long time = entry.getKey().getIssueDate();
        lastLogins.put(ownerStr, time);
        if ((realUser != null) && (!realUser.toString().isEmpty()) && !realUser.equals(owner)) {
          String realUserStr = new KerberosName(realUser.toString()).getServiceName();
          lastLogins.put(realUserStr, time);
        }
      }
      return lastLogins;
    } finally {
      fsn.writeUnlock();
    }
  }
}
