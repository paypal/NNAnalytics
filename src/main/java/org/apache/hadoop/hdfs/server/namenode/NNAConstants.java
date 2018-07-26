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

import java.nio.charset.Charset;
import java.util.EnumSet;
import java.util.function.Predicate;

public interface NNAConstants {

  Charset CHARSET = Charset.defaultCharset();

  int DEFAULT_DELETE_SLEEP_MS = 5000;

  enum SET {
    all,
    files,
    dirs
  }

  enum HISTOGRAM {
    user,
    accessTime,
    modTime,
    fileSize,
    diskspaceConsumed,
    memoryConsumed,
    fileReplica,
    parentDir,
    storageType,
    group,
    fileType,
    dirQuota
  }

  enum HISTOGRAM_OUTPUT {
    chart,
    csv,
    json
  }

  enum FILTER {
    accessTime,
    modTime,
    fileSize,
    diskspaceConsumed,
    memoryConsumed,
    fileReplica,
    blockSize,
    numBlocks,
    numReplicas,
    dirNumChildren,
    dirSubTreeSize,
    dirSubTreeNumFiles,
    dirSubTreeNumDirs,
    storageType,
    depth,
    permission,
    name,
    path,
    user,
    group,
    modDate,
    accessDate,
    isUnderConstruction,
    isWithSnapshot,
    hasAcl,
    hasQuota
  }

  EnumSet<FILTER> FILTER_LONG =
      EnumSet.of(
          FILTER.accessTime,
          FILTER.modTime,
          FILTER.fileSize,
          FILTER.diskspaceConsumed,
          FILTER.memoryConsumed,
          FILTER.fileReplica,
          FILTER.blockSize,
          FILTER.numBlocks,
          FILTER.numReplicas,
          FILTER.dirNumChildren,
          FILTER.dirSubTreeSize,
          FILTER.dirSubTreeNumFiles,
          FILTER.dirSubTreeNumDirs,
          FILTER.storageType,
          FILTER.depth,
          FILTER.permission);

  EnumSet<FILTER> FILTER_STRING =
      EnumSet.of(
          FILTER.name, FILTER.path, FILTER.user, FILTER.group, FILTER.modDate, FILTER.accessDate);

  EnumSet<FILTER> FILTER_BOOLEAN =
      EnumSet.of(FILTER.isUnderConstruction, FILTER.isWithSnapshot, FILTER.hasAcl, FILTER.hasQuota);

  enum FILTER_OP {
    lt,
    gt,
    lte,
    gte,
    eq,
    notEq,
    startsWith,
    notStartsWith,
    endsWith,
    notEndsWith,
    contains,
    notContains,
    minutesAgo,
    hoursAgo,
    daysAgo,
    monthsAgo,
    yearsAgo,
    olderThanMinutes,
    olderThanHours,
    olderThanDays,
    olderThanMonths,
    olderThanYears,
    dateEq,
    dateNotEq,
    dateLt,
    dateLte,
    dateStart,
    dateGt,
    dateGte,
    dateEnd
  }

  EnumSet<FILTER_OP> FILTER_LONG_OPS =
      EnumSet.of(
          FILTER_OP.eq,
          FILTER_OP.lt,
          FILTER_OP.gt,
          FILTER_OP.lte,
          FILTER_OP.gte,
          FILTER_OP.notEq,
          FILTER_OP.minutesAgo,
          FILTER_OP.hoursAgo,
          FILTER_OP.daysAgo,
          FILTER_OP.monthsAgo,
          FILTER_OP.yearsAgo,
          FILTER_OP.olderThanMinutes,
          FILTER_OP.olderThanHours,
          FILTER_OP.olderThanDays,
          FILTER_OP.olderThanMonths,
          FILTER_OP.olderThanYears);

  EnumSet<FILTER_OP> FILTER_STRING_OPS =
      EnumSet.of(
          FILTER_OP.startsWith,
          FILTER_OP.notStartsWith,
          FILTER_OP.endsWith,
          FILTER_OP.notEndsWith,
          FILTER_OP.contains,
          FILTER_OP.notContains,
          FILTER_OP.eq,
          FILTER_OP.notEq,
          FILTER_OP.dateEq,
          FILTER_OP.dateNotEq,
          FILTER_OP.dateLt,
          FILTER_OP.dateLte,
          FILTER_OP.dateStart,
          FILTER_OP.dateGt,
          FILTER_OP.dateGte,
          FILTER_OP.dateEnd);

  EnumSet<FILTER_OP> FILTER_BOOLEAN_OPS = EnumSet.of(FILTER_OP.eq, FILTER_OP.notEq);

  enum TRANSFORM {
    fileReplica
  }

  enum SUM {
    count,
    fileSize,
    diskspaceConsumed,
    memoryConsumed,
    blockSize,
    numBlocks,
    numReplicas,
    nsQuotaRatioUsed,
    dsQuotaRatioUsed,
    nsQuotaUsed,
    dsQuotaUsed,
    nsQuota,
    dsQuota
  }

  enum FIND {
    min,
    max,
    avg
  }

  enum FIND_FIELD {
    accessTime,
    modTime,
    fileSize,
    diskspaceConsumed,
    memoryConsumed
  }

  enum OPERATION {
    delete,
    setReplication,
    setStoragePolicy
  }

  enum ENDPOINT {
    login,
    logout,
    endpoints,
    credentials,
    loadingStatus,
    config,
    system,
    info,
    threads,
    sets,
    filters,
    filterOps,
    histograms,
    histogramOutputs,
    sums,
    transforms,
    finds,
    operations,
    dump,
    filter,
    histogram,
    divide,
    saveNamespace,
    fetchNamespace,
    reloadNamespace,
    log,
    history,
    suggestions,
    users,
    top,
    bottom,
    refresh,
    listOperations,
    submitOperation,
    abortOperation,
    token,
    drop,
    truncate,
    directories,
    addDirectory,
    removeDirectory,
    quotas,
    fileAge
  }

  EnumSet<ENDPOINT> UNSECURED_ENDPOINTS =
      EnumSet.of(
          ENDPOINT.login,
          ENDPOINT.logout,
          ENDPOINT.endpoints,
          ENDPOINT.credentials,
          ENDPOINT.sets,
          ENDPOINT.filters,
          ENDPOINT.filterOps,
          ENDPOINT.histograms,
          ENDPOINT.histogramOutputs,
          ENDPOINT.sums,
          ENDPOINT.transforms,
          ENDPOINT.operations,
          ENDPOINT.finds);

  EnumSet<ENDPOINT> CACHE_READER_ENDPOINTS =
      EnumSet.of(
          ENDPOINT.suggestions,
          ENDPOINT.history,
          ENDPOINT.token,
          ENDPOINT.directories,
          ENDPOINT.users,
          ENDPOINT.quotas,
          ENDPOINT.fileAge,
          ENDPOINT.info,
          ENDPOINT.config);

  EnumSet<ENDPOINT> READER_ENDPOINTS =
      EnumSet.of(
          ENDPOINT.filter,
          ENDPOINT.histogram,
          ENDPOINT.divide,
          ENDPOINT.top,
          ENDPOINT.bottom,
          ENDPOINT.dump);

  EnumSet<ENDPOINT> WRITER_ENDPOINTS =
      EnumSet.of(ENDPOINT.listOperations, ENDPOINT.submitOperation, ENDPOINT.abortOperation);

  EnumSet<ENDPOINT> ADMIN_ENDPOINTS =
      EnumSet.of(
          ENDPOINT.saveNamespace,
          ENDPOINT.fetchNamespace,
          ENDPOINT.reloadNamespace,
          ENDPOINT.log,
          ENDPOINT.loadingStatus,
          ENDPOINT.system,
          ENDPOINT.threads,
          ENDPOINT.refresh,
          ENDPOINT.drop,
          ENDPOINT.truncate,
          ENDPOINT.addDirectory,
          ENDPOINT.removeDirectory);

  EnumSet<FILTER> FILTER_FILE =
      EnumSet.of(
          FILTER.blockSize,
          FILTER.fileSize,
          FILTER.fileReplica,
          FILTER.diskspaceConsumed,
          FILTER.numBlocks,
          FILTER.numReplicas,
          FILTER.isUnderConstruction,
          FILTER.storageType,
          FILTER.accessTime,
          FILTER.modTime,
          FILTER.memoryConsumed,
          FILTER.depth,
          FILTER.permission,
          FILTER.name,
          FILTER.path,
          FILTER.user,
          FILTER.group,
          FILTER.modDate,
          FILTER.accessDate,
          FILTER.isWithSnapshot,
          FILTER.hasAcl,
          FILTER.hasQuota);

  EnumSet<FILTER> FILTER_DIR =
      EnumSet.of(
          FILTER.dirNumChildren,
          FILTER.dirSubTreeSize,
          FILTER.dirSubTreeNumFiles,
          FILTER.dirSubTreeNumDirs,
          FILTER.accessDate,
          FILTER.accessTime,
          FILTER.modDate,
          FILTER.modTime,
          FILTER.memoryConsumed,
          FILTER.depth,
          FILTER.permission,
          FILTER.name,
          FILTER.path,
          FILTER.user,
          FILTER.group,
          FILTER.isWithSnapshot,
          FILTER.hasAcl,
          FILTER.hasQuota,
          FILTER.storageType);

  EnumSet<FILTER> FILTER_ALL = getIntersection(FILTER_FILE, FILTER_DIR);

  EnumSet<HISTOGRAM> TYPE_FILE =
      EnumSet.of(
          HISTOGRAM.fileSize,
          HISTOGRAM.fileReplica,
          HISTOGRAM.diskspaceConsumed,
          HISTOGRAM.storageType,
          HISTOGRAM.user,
          HISTOGRAM.accessTime,
          HISTOGRAM.modTime,
          HISTOGRAM.memoryConsumed,
          HISTOGRAM.parentDir,
          HISTOGRAM.group,
          HISTOGRAM.fileType);

  EnumSet<HISTOGRAM> TYPE_DIR =
      EnumSet.of(
          HISTOGRAM.user,
          HISTOGRAM.accessTime,
          HISTOGRAM.modTime,
          HISTOGRAM.memoryConsumed,
          HISTOGRAM.parentDir,
          HISTOGRAM.group,
          HISTOGRAM.storageType,
          HISTOGRAM.dirQuota);

  EnumSet<HISTOGRAM> TYPE_ALL = getIntersection(TYPE_FILE, TYPE_DIR);

  EnumSet<SUM> SUM_FILE =
      EnumSet.of(
          SUM.fileSize,
          SUM.diskspaceConsumed,
          SUM.blockSize,
          SUM.numBlocks,
          SUM.numReplicas,
          SUM.memoryConsumed,
          SUM.count);

  EnumSet<SUM> SUM_DIR =
      EnumSet.of(
          SUM.count,
          SUM.memoryConsumed,
          SUM.nsQuota,
          SUM.dsQuota,
          SUM.nsQuotaUsed,
          SUM.dsQuotaUsed,
          SUM.nsQuotaRatioUsed,
          SUM.dsQuotaRatioUsed);

  EnumSet<SUM> SUM_ALL = getIntersection(SUM_FILE, SUM_DIR);

  EnumSet<FIND_FIELD> FIND_FILE =
      EnumSet.of(
          FIND_FIELD.accessTime,
          FIND_FIELD.modTime,
          FIND_FIELD.diskspaceConsumed,
          FIND_FIELD.fileSize,
          FIND_FIELD.memoryConsumed);

  EnumSet<FIND_FIELD> FIND_DIR =
      EnumSet.of(FIND_FIELD.accessTime, FIND_FIELD.modTime, FIND_FIELD.memoryConsumed);

  EnumSet<FIND_FIELD> FIND_ALL = getIntersection(FIND_FILE, FIND_DIR);

  enum OPERAND {
    type,
    filter,
    sum,
    find
  }

  static <K extends Enum<K>> EnumSet<K> getIntersection(
      EnumSet<K> firstEnum, EnumSet<K> secondEnum) {
    EnumSet<K> intersection = EnumSet.copyOf(firstEnum);
    firstEnum.stream().filter(not(secondEnum::contains)).forEach(intersection::remove);
    return intersection;
  }

  static <K extends Enum<K>> EnumSet<K> getDifference(EnumSet<K> firstEnum, EnumSet<K> secondEnum) {
    EnumSet<K> difference = EnumSet.copyOf(firstEnum);
    firstEnum.stream().filter(secondEnum::contains).forEach(difference::remove);
    return difference;
  }

  static <R> Predicate<R> not(Predicate<R> predicate) {
    return predicate.negate();
  }
}
