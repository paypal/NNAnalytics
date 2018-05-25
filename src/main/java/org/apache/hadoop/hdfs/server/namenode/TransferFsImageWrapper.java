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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.EnumSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.util.MD5FileUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * In order to facilitate FsImage fetching we need a wrapper class as TransferFsImage's method for
 * fetching the latest image does not return the MD5 and we do not want NNA to be responsible for
 * computing it.
 */
public class TransferFsImageWrapper {

  private final NNLoader nnLoader;

  public TransferFsImageWrapper(NNLoader nnLoader) {
    this.nnLoader = nnLoader;
  }

  /**
   * This is meant to download the latest FSImage without relying on FSNamesystem or other running
   * HDFS classes within NNLoader.
   */
  public void downloadMostRecentImage() throws IOException {
    FileSystem fileSystem = nnLoader.getFileSystem();
    Configuration conf = nnLoader.getConfiguration();
    String namespaceDirPath = conf.get(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY);
    File namespaceDir = new File(namespaceDirPath, "current");
    SecurityUtil.login(conf, DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY,
        DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY,
        InetAddress.getLocalHost().getCanonicalHostName());
    InetSocketAddress addressOfActive = HAUtil.getAddressOfActive(fileSystem);
    URL infoServer = DFSUtil.getInfoServer(addressOfActive, conf, DFSUtil.getHttpClientScheme(conf))
        .toURL();
    SecurityUtil.doAsLoginUser(() -> {
      NamenodeProtocol nnProtocolProxy =
          NameNodeProxies.createNonHAProxy(conf, addressOfActive, NamenodeProtocol.class,
              UserGroupInformation.getLoginUser(), true).getProxy();
      NamespaceInfo namespaceInfo = nnProtocolProxy.versionRequest();
      String fileId = ImageServlet.getParamStringForMostRecentImage();
      NNStorage storage =
          new NNStorage(conf, FSNamesystem.getNamespaceDirs(conf),
              FSNamesystem.getNamespaceEditsDirs(conf));
      storage.format(namespaceInfo);
      MD5Hash md5 = TransferFsImage
          .getFileClient(infoServer, fileId, Lists.newArrayList(namespaceDir),
              storage, true);
      FSImageTransactionalStorageInspector inspector =
          new FSImageTransactionalStorageInspector(EnumSet.of(NNStorage.NameNodeFile.IMAGE));
      storage.inspectStorageDirs(inspector);
      File imageFile = inspector.getLatestImages().get(0).getFile();
      MD5FileUtils.saveMD5File(imageFile, md5);
      return null;
    });
  }
}
