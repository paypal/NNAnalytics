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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.notNull;

import java.util.Map;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestTokenExtractor {
  
  private FSNamesystem fsn;
  private DelegationTokenSecretManager dtsm;
  
  @Before
  public void setUp() {
    fsn = Mockito.mock(FSNamesystem.class);
    dtsm = new DelegationTokenSecretManager(0L, 0L, 0L, 0L, fsn);
  }
  
  @Test
  public void testSimple() {
    Mockito.doNothing().when(fsn).writeLock();
    Mockito.doNothing().when(fsn).writeUnlock();

    DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(new Text("mockOwner"), 
        new Text("mockGroup"), new Text("mockRealUser/HOST@REALM.COM"));
    DelegationTokenInformation dtInfo = Mockito.mock(DelegationTokenInformation.class);
    dtsm.currentTokens.put(dtId, dtInfo);

    TokenExtractor extractor = new TokenExtractor(dtsm, fsn);
    Map<String, Long> tokenLastLogins = extractor.getTokenLastLogins();

    assertThat(tokenLastLogins.size(), is(2));
    assertThat(tokenLastLogins.get("mockRealUser"), is(notNull()));
    assertThat(tokenLastLogins.get("mockOwner"), is(notNull()));
  }

}
