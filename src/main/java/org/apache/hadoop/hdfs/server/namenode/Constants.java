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

public interface Constants {

  Charset CHARSET = Charset.defaultCharset();

  int DEFAULT_DELETE_SLEEP_MS = 5000;

  enum INodeSet {
    all,
    files,
    dirs
  }

  enum Histogram {
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

  enum HistogramOutput {
    chart,
    csv,
    json
  }

  enum Filter {
    id,
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
    hasQuota,
    isUnderNsQuota,
    isUnderDsQuota
  }

  EnumSet<Filter> FILTER_LONG =
      EnumSet.of(
          Filter.id,
          Filter.accessTime,
          Filter.modTime,
          Filter.fileSize,
          Filter.diskspaceConsumed,
          Filter.memoryConsumed,
          Filter.fileReplica,
          Filter.blockSize,
          Filter.numBlocks,
          Filter.numReplicas,
          Filter.dirNumChildren,
          Filter.dirSubTreeSize,
          Filter.dirSubTreeNumFiles,
          Filter.dirSubTreeNumDirs,
          Filter.storageType,
          Filter.depth,
          Filter.permission);

  EnumSet<Filter> FILTER_STRING =
      EnumSet.of(
          Filter.name, Filter.path, Filter.user, Filter.group, Filter.modDate, Filter.accessDate);

  EnumSet<Filter> FILTER_BOOLEAN =
      EnumSet.of(
          Filter.isUnderConstruction,
          Filter.isWithSnapshot,
          Filter.hasAcl,
          Filter.hasQuota,
          Filter.isUnderNsQuota,
          Filter.isUnderDsQuota);

  enum FilterOp {
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

  EnumSet<FilterOp> FILTER_LONG_OPS =
      EnumSet.of(
          FilterOp.eq,
          FilterOp.lt,
          FilterOp.gt,
          FilterOp.lte,
          FilterOp.gte,
          FilterOp.notEq,
          FilterOp.minutesAgo,
          FilterOp.hoursAgo,
          FilterOp.daysAgo,
          FilterOp.monthsAgo,
          FilterOp.yearsAgo,
          FilterOp.olderThanMinutes,
          FilterOp.olderThanHours,
          FilterOp.olderThanDays,
          FilterOp.olderThanMonths,
          FilterOp.olderThanYears);

  EnumSet<FilterOp> FILTER_STRING_OPS =
      EnumSet.of(
          FilterOp.startsWith,
          FilterOp.notStartsWith,
          FilterOp.endsWith,
          FilterOp.notEndsWith,
          FilterOp.contains,
          FilterOp.notContains,
          FilterOp.eq,
          FilterOp.notEq,
          FilterOp.dateEq,
          FilterOp.dateNotEq,
          FilterOp.dateLt,
          FilterOp.dateLte,
          FilterOp.dateStart,
          FilterOp.dateGt,
          FilterOp.dateGte,
          FilterOp.dateEnd);

  EnumSet<FilterOp> FILTER_BOOLEAN_OPS = EnumSet.of(FilterOp.eq, FilterOp.notEq);

  enum Transform {
    fileReplica
  }

  enum Sum {
    count,
    dirNumChildren,
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

  enum Find {
    min,
    max,
    avg
  }

  enum FindField {
    accessTime,
    modTime,
    fileSize,
    blockSize,
    diskspaceConsumed,
    memoryConsumed
  }

  enum Operation {
    delete,
    setReplication,
    setStoragePolicy
  }

  enum Endpoint {
    login,
    logout,
    endpoints,
    contentSummary,
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
    histogram2,
    histogram3,
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
    fileAge,
    metrics,
    setCachedQuery,
    getCachedQuery,
    removeCachedQuery,
    cachedMaps,
    sql,
    fileTypes,
    queryGuard
  }

  EnumSet<Endpoint> UNSECURED_ENDPOINTS =
      EnumSet.of(
          Endpoint.login,
          Endpoint.logout,
          Endpoint.endpoints,
          Endpoint.credentials,
          Endpoint.sets,
          Endpoint.filters,
          Endpoint.filterOps,
          Endpoint.histograms,
          Endpoint.histogramOutputs,
          Endpoint.sums,
          Endpoint.transforms,
          Endpoint.operations,
          Endpoint.finds);

  EnumSet<Endpoint> CACHE_READER_ENDPOINTS =
      EnumSet.of(
          Endpoint.suggestions,
          Endpoint.history,
          Endpoint.token,
          Endpoint.directories,
          Endpoint.users,
          Endpoint.quotas,
          Endpoint.fileAge,
          Endpoint.info,
          Endpoint.config,
          Endpoint.getCachedQuery,
          Endpoint.cachedMaps,
          Endpoint.fileTypes);

  EnumSet<Endpoint> READER_ENDPOINTS =
      EnumSet.of(
          Endpoint.contentSummary,
          Endpoint.filter,
          Endpoint.histogram,
          Endpoint.histogram2,
          Endpoint.histogram3,
          Endpoint.divide,
          Endpoint.top,
          Endpoint.bottom,
          Endpoint.dump,
          Endpoint.sql);

  EnumSet<Endpoint> WRITER_ENDPOINTS =
      EnumSet.of(Endpoint.listOperations, Endpoint.submitOperation, Endpoint.abortOperation);

  EnumSet<Endpoint> ADMIN_ENDPOINTS =
      EnumSet.of(
          Endpoint.saveNamespace,
          Endpoint.fetchNamespace,
          Endpoint.reloadNamespace,
          Endpoint.log,
          Endpoint.loadingStatus,
          Endpoint.system,
          Endpoint.threads,
          Endpoint.refresh,
          Endpoint.drop,
          Endpoint.truncate,
          Endpoint.addDirectory,
          Endpoint.removeDirectory,
          Endpoint.metrics,
          Endpoint.setCachedQuery,
          Endpoint.removeCachedQuery,
          Endpoint.queryGuard);

  EnumSet<Filter> FILTER_FILE =
      EnumSet.of(
          Filter.id,
          Filter.blockSize,
          Filter.fileSize,
          Filter.fileReplica,
          Filter.diskspaceConsumed,
          Filter.numBlocks,
          Filter.numReplicas,
          Filter.isUnderConstruction,
          Filter.storageType,
          Filter.accessTime,
          Filter.modTime,
          Filter.memoryConsumed,
          Filter.depth,
          Filter.permission,
          Filter.name,
          Filter.path,
          Filter.user,
          Filter.group,
          Filter.modDate,
          Filter.accessDate,
          Filter.isWithSnapshot,
          Filter.hasAcl,
          Filter.hasQuota,
          Filter.isUnderNsQuota,
          Filter.isUnderDsQuota);

  EnumSet<Filter> FILTER_DIR =
      EnumSet.of(
          Filter.id,
          Filter.dirNumChildren,
          Filter.dirSubTreeSize,
          Filter.dirSubTreeNumFiles,
          Filter.dirSubTreeNumDirs,
          Filter.accessDate,
          Filter.accessTime,
          Filter.modDate,
          Filter.modTime,
          Filter.memoryConsumed,
          Filter.depth,
          Filter.permission,
          Filter.name,
          Filter.path,
          Filter.user,
          Filter.group,
          Filter.isWithSnapshot,
          Filter.hasAcl,
          Filter.hasQuota,
          Filter.storageType,
          Filter.isUnderNsQuota,
          Filter.isUnderDsQuota);

  EnumSet<Filter> FILTER_ALL = getIntersection(FILTER_FILE, FILTER_DIR);

  EnumSet<Histogram> TYPE_FILE =
      EnumSet.of(
          Histogram.fileSize,
          Histogram.fileReplica,
          Histogram.diskspaceConsumed,
          Histogram.storageType,
          Histogram.user,
          Histogram.accessTime,
          Histogram.modTime,
          Histogram.memoryConsumed,
          Histogram.parentDir,
          Histogram.group,
          Histogram.fileType);

  EnumSet<Histogram> TYPE_DIR =
      EnumSet.of(
          Histogram.user,
          Histogram.accessTime,
          Histogram.modTime,
          Histogram.memoryConsumed,
          Histogram.parentDir,
          Histogram.group,
          Histogram.storageType,
          Histogram.dirQuota);

  EnumSet<Histogram> TYPE_ALL = getIntersection(TYPE_FILE, TYPE_DIR);

  EnumSet<Sum> SUM_FILE =
      EnumSet.of(
          Sum.fileSize,
          Sum.diskspaceConsumed,
          Sum.blockSize,
          Sum.numBlocks,
          Sum.numReplicas,
          Sum.memoryConsumed,
          Sum.count);

  EnumSet<Sum> SUM_DIR =
      EnumSet.of(
          Sum.count,
          Sum.dirNumChildren,
          Sum.memoryConsumed,
          Sum.nsQuota,
          Sum.dsQuota,
          Sum.nsQuotaUsed,
          Sum.dsQuotaUsed,
          Sum.nsQuotaRatioUsed,
          Sum.dsQuotaRatioUsed);

  EnumSet<Sum> SUM_ALL = getIntersection(SUM_FILE, SUM_DIR);

  EnumSet<FindField> FIND_FILE =
      EnumSet.of(
          FindField.accessTime,
          FindField.modTime,
          FindField.blockSize,
          FindField.diskspaceConsumed,
          FindField.fileSize,
          FindField.memoryConsumed);

  EnumSet<FindField> FIND_DIR =
      EnumSet.of(FindField.accessTime, FindField.modTime, FindField.memoryConsumed);

  EnumSet<FindField> FIND_ALL = getIntersection(FIND_FILE, FIND_DIR);

  enum Operand {
    type,
    filter,
    sum,
    find
  }

  enum AnalysisState {
    sleep,
    capacity,
    fileAges,
    users,
    diskspace,
    files24h,
    files1y2y,
    perUserCount,
    perUserFileType,
    directories,
    cachedDirectories,
    systemFilter,
    system24h,
    system1y,
    systemCount,
    perUserFilter,
    perUser24h,
    perUser1y,
    perUserMem,
    perUserDs,
    directories24h,
    cachedDirectories24h,
    cachedQuotas,
    cachedLogins,
    history,
    cachedQueries,
    writeMapDb
  }

  /**
   * Returns intersection of two EnumSets as a new EnumSet.
   *
   * @param firstEnum first enum set
   * @param secondEnum second enum set
   * @param <K> some Enum
   * @return the intersection between the two parameter sets
   */
  static <K extends Enum<K>> EnumSet<K> getIntersection(
      EnumSet<K> firstEnum, EnumSet<K> secondEnum) {
    EnumSet<K> intersection = EnumSet.copyOf(firstEnum);
    firstEnum.stream().filter(not(secondEnum::contains)).forEach(intersection::remove);
    return intersection;
  }

  /**
   * Returns difference of two EnumSets as a new EnumSet.
   *
   * @param firstEnum first enum set
   * @param secondEnum second enum set
   * @param <K> some Enum
   * @return the difference between the two parameter sets
   */
  static <K extends Enum<K>> EnumSet<K> getDifference(EnumSet<K> firstEnum, EnumSet<K> secondEnum) {
    EnumSet<K> difference = EnumSet.copyOf(firstEnum);
    firstEnum.stream().filter(secondEnum::contains).forEach(difference::remove);
    return difference;
  }

  static <R> Predicate<R> not(Predicate<R> predicate) {
    return predicate.negate();
  }
}
