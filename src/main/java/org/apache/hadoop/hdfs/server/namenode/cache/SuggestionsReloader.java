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

package org.apache.hadoop.hdfs.server.namenode.cache;

import org.apache.hadoop.hdfs.server.namenode.Constants.AnalysisState;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLoader;
import org.apache.hadoop.hdfs.server.namenode.analytics.ApplicationConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuggestionsReloader implements Runnable {

  public static final Logger LOG = LoggerFactory.getLogger(SuggestionsReloader.class.getName());

  private ApplicationConfiguration conf;
  private SuggestionsEngine suggestionsEngine;
  private NameNodeLoader nameNodeLoader;
  private volatile boolean shutdown = false;

  /**
   * Constructor.
   *
   * @param conf application config
   * @param suggestionsEngine suggestions engine
   * @param nameNodeLoader namenode loader
   */
  public SuggestionsReloader(
      ApplicationConfiguration conf,
      SuggestionsEngine suggestionsEngine,
      NameNodeLoader nameNodeLoader) {
    this.conf = conf;
    this.suggestionsEngine = suggestionsEngine;
    this.nameNodeLoader = nameNodeLoader;
  }

  public void shutdown() {
    shutdown = true;
    Thread.currentThread().interrupt();
  }

  @Override
  public void run() {
    while (!shutdown) {
      suggestionsEngine.setCurrentState(AnalysisState.sleep);
      try {
        suggestionsEngine.reloadSuggestions(nameNodeLoader);
      } catch (Throwable e) {
        LOG.info("Suggestion reload failed!", e);
        for (StackTraceElement element : e.getStackTrace()) {
          LOG.info(element.toString());
        }
      }
      suggestionsEngine.setCurrentState(AnalysisState.sleep);
      try {
        Thread.sleep(conf.getSuggestionsReloadSleepMs());
      } catch (InterruptedException ex) {
        LOG.debug("Suggestion reload was interrupted.", ex);
      }
    }
    LOG.error("Suggestion reload service exited; suggestions will not update.");
  }
}
