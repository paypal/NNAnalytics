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

import java.util.Map;

class UserPasswordSet extends UserSet {

  private Map<String, String> usernamePasswords;

  UserPasswordSet(Map<String, String> usernamePasswords) {
    super(usernamePasswords.keySet());
    this.usernamePasswords = usernamePasswords;
    allowAll = false;
  }

  boolean allows(String user) {
    return super.allows(user);
  }

  boolean authenticate(String user, String password) {
    String storedPassword = usernamePasswords.get(user);
    return storedPassword != null && storedPassword.equals(password);
  }
}
