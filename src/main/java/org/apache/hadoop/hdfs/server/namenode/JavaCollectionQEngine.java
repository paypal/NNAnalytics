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

import static com.googlecode.cqengine.query.QueryFactory.and;
import static com.googlecode.cqengine.query.QueryFactory.attribute;
import static com.googlecode.cqengine.query.QueryFactory.contains;
import static com.googlecode.cqengine.query.QueryFactory.endsWith;
import static com.googlecode.cqengine.query.QueryFactory.equal;
import static com.googlecode.cqengine.query.QueryFactory.greaterThan;
import static com.googlecode.cqengine.query.QueryFactory.greaterThanOrEqualTo;
import static com.googlecode.cqengine.query.QueryFactory.lessThan;
import static com.googlecode.cqengine.query.QueryFactory.lessThanOrEqualTo;
import static com.googlecode.cqengine.query.QueryFactory.not;
import static com.googlecode.cqengine.query.QueryFactory.startsWith;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.persistence.wrapping.WrappingPersistence;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.parser.sql.SQLParser;
import com.googlecode.cqengine.resultset.ResultSet;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.http.HttpStatus;

public class JavaCollectionQEngine extends AbstractQueryEngine {

  private final SimpleAttribute<INode, Long> id =
      attribute("id", node -> getFilterFunctionToLongForINode("id").apply(node));
  private final SimpleAttribute<INode, Long> accessTime =
      attribute("accessTime", node -> getFilterFunctionToLongForINode("accessTime").apply(node));
  private final SimpleAttribute<INode, Long> modTime =
      attribute("modTime", node -> getFilterFunctionToLongForINode("modTime").apply(node));
  private final SimpleAttribute<INode, Long> fileSize =
      attribute("fileSize", node -> getFilterFunctionToLongForINode("fileSize").apply(node));
  private final SimpleAttribute<INode, Long> diskspaceConsumed =
      attribute(
          "diskspaceConsumed",
          node -> getFilterFunctionToLongForINode("diskspaceConsumed").apply(node));
  private final SimpleAttribute<INode, Long> memoryConsumed =
      attribute(
          "memoryConsumed", node -> getFilterFunctionToLongForINode("memoryConsumed").apply(node));
  private final SimpleAttribute<INode, Long> fileReplica =
      attribute("fileReplica", node -> getFilterFunctionToLongForINode("fileReplica").apply(node));
  private final SimpleAttribute<INode, Long> blockSize =
      attribute("blockSize", node -> getFilterFunctionToLongForINode("blockSize").apply(node));
  private final SimpleAttribute<INode, Long> numBlocks =
      attribute("numBlocks", node -> getFilterFunctionToLongForINode("numBlocks").apply(node));
  private final SimpleAttribute<INode, Long> numReplicas =
      attribute("numReplicas", node -> getFilterFunctionToLongForINode("numReplicas").apply(node));
  private final SimpleAttribute<INode, Long> depth =
      attribute("depth", node -> getFilterFunctionToLongForINode("depth").apply(node));
  private final SimpleAttribute<INode, Long> permission =
      attribute("permission", node -> getFilterFunctionToLongForINode("permission").apply(node));
  private final SimpleAttribute<INode, String> user =
      attribute("user", node -> getFilterFunctionToStringForINode("user").apply(node));
  private final SimpleAttribute<INode, String> group =
      attribute("group", node -> getFilterFunctionToStringForINode("group").apply(node));
  private final SimpleAttribute<INode, String> name =
      attribute("name", node -> getFilterFunctionToStringForINode("name").apply(node));
  private final SimpleAttribute<INode, String> path =
      attribute("path", node -> getFilterFunctionToStringForINode("path").apply(node));
  private final SimpleAttribute<INode, Date> modDate =
      attribute(
          "modDate",
          node -> {
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
            String modDateInt = getFilterFunctionToStringForINode("modDate").apply(node);
            try {
              return sdf.parse(modDateInt);
            } catch (ParseException e) {
              throw new RuntimeException(e);
            }
          });
  private final SimpleAttribute<INode, Date> accessDate =
      attribute(
          "accessDate",
          node -> {
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
            String modDateInt = getFilterFunctionToStringForINode("accessDate").apply(node);
            try {
              return sdf.parse(modDateInt);
            } catch (ParseException e) {
              throw new RuntimeException(e);
            }
          });
  private final SimpleAttribute<INode, Boolean> isUnderConstruction =
      attribute(
          "isUnderConstruction",
          node -> getFilterFunctionToBooleanForINode("isUnderConstruction").apply(node));
  private final SimpleAttribute<INode, Boolean> isWithSnapshot =
      attribute(
          "isWithSnapshot",
          node -> getFilterFunctionToBooleanForINode("isWithSnapshot").apply(node));
  private final SimpleAttribute<INode, Boolean> hasAcl =
      attribute("hasAcl", node -> getFilterFunctionToBooleanForINode("hasAcl").apply(node));
  private final SimpleAttribute<INode, Boolean> hasQuota =
      attribute("hasQuota", node -> getFilterFunctionToBooleanForINode("hasQuota").apply(node));
  private final SimpleAttribute<INode, Boolean> isUnderNsQuota =
      attribute(
          "isUnderNsQuota", node -> getFilterFunctionToBooleanForINode("isUnderNsQuota").apply(node));
  private final SimpleAttribute<INode, Boolean> isUnderDsQuota =
      attribute(
          "isUnderDsQuota", node -> getFilterFunctionToBooleanForINode("isUnderDsQuota").apply(node));
  private SimpleAttribute<INode, Long> dirNumChildren;
  private SimpleAttribute<INode, Long> dirSubTreeSize;
  private SimpleAttribute<INode, Long> dirSubTreeNumFiles;
  private SimpleAttribute<INode, Long> dirSubTreeNumDirs;
  private SimpleAttribute<INode, Long> storageType;

  private IndexedCollection<INode> indexedFiles;
  private IndexedCollection<INode> indexedDirs;

  @Override // QueryEngine
  public void setContexts(NameNodeLoader loader, VersionInterface versionLoader) {
    this.nameNodeLoader = loader;
    this.versionLoader = versionLoader;

    dirNumChildren =
        attribute(
            "dirNumChildren",
            node -> versionLoader.getFilterFunctionToLongForINode("dirNumChildren").apply(node));
    dirSubTreeSize =
        attribute(
            "dirSubTreeSize",
            node -> versionLoader.getFilterFunctionToLongForINode("dirSubTreeSize").apply(node));
    dirSubTreeNumFiles =
        attribute(
            "dirSubTreeNumFiles",
            node ->
                versionLoader.getFilterFunctionToLongForINode("dirSubTreeNumFiles").apply(node));
    dirSubTreeNumDirs =
        attribute(
            "dirSubTreeNumDirs",
            node -> versionLoader.getFilterFunctionToLongForINode("dirSubTreeNumDirs").apply(node));
    storageType =
        attribute(
            "storageType",
            node -> versionLoader.getFilterFunctionToLongForINode("storageType").apply(node));

    Collection<INode> files = loader.getINodeSetInternal("files");
    Collection<INode> dirs = loader.getINodeSetInternal("dirs");

    indexedFiles =
        new ConcurrentIndexedCollection<>(
            WrappingPersistence.aroundCollectionOnPrimaryKey(files, id));

    indexedDirs =
        new ConcurrentIndexedCollection<>(
            WrappingPersistence.aroundCollectionOnPrimaryKey(dirs, id));
  }

  @Override // QueryEngine
  public Collection<INode> getINodeSet(String set) {
    long start = System.currentTimeMillis();
    Collection<INode> inodes;
    switch (set) {
      case "all":
        inodes =
            new ConcurrentIndexedCollection<>(
                WrappingPersistence.aroundCollectionOnPrimaryKey(
                    nameNodeLoader.getINodeSetInternal("all"), id));
        break;
      case "files":
        inodes = indexedFiles;
        break;
      case "dirs":
        inodes = indexedDirs;
        break;
      default:
        throw new IllegalArgumentException(
            "You did not specify a set to use. Please check /sets for available sets.");
    }
    long end = System.currentTimeMillis();
    LOG.info(
        "Fetching indexed set of: {} had result size: {} and took: {} ms.",
        set,
        inodes.size(),
        (end - start));
    return inodes;
  }

  /**
   * Main filter method for filtering down a set of INodes to a smaller subset.
   *
   * @param inodes the main inode set to work on
   * @param filters set of filters to use
   * @param filterOps matching length set of filter operands and operators
   * @return the filtered set of inodes
   */
  @Override // QueryEngine
  public Collection<INode> combinedFilter(
      Collection<INode> inodes, String[] filters, String[] filterOps) {
    IndexedCollection<INode> indexedINodes;
    if (!(inodes instanceof IndexedCollection)) {
      indexedINodes =
          new ConcurrentIndexedCollection<>(
              WrappingPersistence.aroundCollectionOnPrimaryKey(inodes, id));
    } else {
      indexedINodes = (IndexedCollection<INode>) inodes;
    }

    List<Query<INode>> queries = new ArrayList<>();
    for (int i = 0; i < filters.length; i++) {
      String filter = filters[i];
      String[] filterOp = filterOps[i].split(":");
      Query<INode> filterFunc = getFilter(filter, filterOp);
      queries.add(filterFunc);
    }

    long start = System.currentTimeMillis();
    try {
      ResultSet<INode> result;
      switch (queries.size()) {
        case 0:
          return inodes;
        case 1:
          result = indexedINodes.retrieve(queries.get(0));
          break;
        case 2:
          result = indexedINodes.retrieve(and(queries.get(0), queries.get(1)));
          break;
        default:
          result =
              indexedINodes.retrieve(
                  and(queries.get(0), queries.get(1), queries.subList(2, queries.size())));
          break;
      }
      return result.stream().collect(Collectors.toSet());
    } finally {
      long end = System.currentTimeMillis();
      LOG.info(
          "Performing filters: {} with filterOps: {} took: {} ms.",
          Arrays.asList(filters),
          Arrays.asList(filterOps),
          (end - start));
    }
  }

  private Query<INode> getFilter(String filter, String[] filterOps) {
    long start = System.currentTimeMillis();
    try {
      // Values for all other filters
      String op = filterOps[0];
      String opValue = filterOps[1];

      // Long value filters
      Attribute<INode, Long> longAttribute = getLongAttributeForINode(filter);
      if (longAttribute != null) {
        return getQueryForLong(longAttribute, Long.parseLong(opValue), op);
      }

      // String value filters
      Attribute<INode, String> stringAttribute = getStringAttributeForINode(filter);
      if (stringAttribute != null) {
        return getQueryForString(stringAttribute, opValue, op);
      }

      // Boolean value filters
      Attribute<INode, Boolean> booleanAttribute = getBooleanAttributeForINode(filter);
      if (booleanAttribute != null) {
        return getQueryForBoolean(booleanAttribute, opValue, op);
      }

      // Date value filters
      Attribute<INode, Date> dateAttribute = getDateAttributeForINode(filter);
      if (dateAttribute != null) {
        return getQueryForDate(dateAttribute, opValue, op);
      }

      throw new IllegalArgumentException(
          "Failed to determine filter: "
              + filter
              + ", with operations: "
              + Arrays.asList(filterOps)
              + ".\nCheck your filter arguments."
              + "\nPossible filters and operations available at /filters and /filterOps.");
    } finally {
      long end = System.currentTimeMillis();
      LOG.info(
          "Obtaining filter: {} with filterOps:{} took: {} ms.",
          filter,
          Arrays.asList(filterOps),
          (end - start));
    }
  }

  private Query<INode> getQueryForDate(
      Attribute<INode, Date> dateAttribute, String value, String op) {
    SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
    Date valueDate;
    switch (op) {
      case "dateEq":
        try {
          valueDate = sdf.parse(value);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
        return equal(dateAttribute, valueDate);
      case "dateNotEq":
        try {
          valueDate = sdf.parse(value);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
        return not(equal(dateAttribute, valueDate));
      case "dateLt":
        try {
          valueDate = sdf.parse(value);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
        return lessThan(dateAttribute, valueDate);
      case "dateStart":
      case "dateLte":
        try {
          valueDate = sdf.parse(value);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
        return lessThanOrEqualTo(dateAttribute, valueDate);
      case "dateGt":
        try {
          valueDate = sdf.parse(value);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
        return greaterThan(dateAttribute, valueDate);
      case "dateEnd":
      case "dateGte":
        try {
          valueDate = sdf.parse(value);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
        return greaterThanOrEqualTo(dateAttribute, valueDate);
      default:
        return null;
    }
  }

  private Attribute<INode, Date> getDateAttributeForINode(String filter) {
    switch (filter) {
      case "modDate":
        return modDate;
      case "accessDate":
        return accessDate;
      default:
        return null;
    }
  }

  private Query<INode> getQueryForBoolean(
      Attribute<INode, Boolean> booleanAttribute, String value, String op) {
    switch (op) {
      case "eq":
        return equal(booleanAttribute, Boolean.valueOf(value));
      case "notEq":
        return not(equal(booleanAttribute, Boolean.valueOf(value)));
      default:
        return null;
    }
  }

  private Attribute<INode, Boolean> getBooleanAttributeForINode(String filter) {
    switch (filter) {
      case "isUnderConstruction":
        return isUnderConstruction;
      case "isWithSnapshot":
        return isWithSnapshot;
      case "hasAcl":
        return hasAcl;
      case "hasQuota":
        return hasQuota;
      case "isUnderNsQuota":
        return isUnderNsQuota;
      case "isUnderDsQuota":
        return isUnderDsQuota;
      default:
        return null;
    }
  }

  private Query<INode> getQueryForString(
      Attribute<INode, String> stringAttribute, String value, String op) {
    switch (op) {
      case "eq":
        return equal(stringAttribute, value);
      case "notEq":
        return not(equal(stringAttribute, value));
      case "startsWith":
        return startsWith(stringAttribute, value);
      case "notStartsWith":
        return not(startsWith(stringAttribute, value));
      case "endsWith":
        return endsWith(stringAttribute, value);
      case "notEndsWith":
        return not(endsWith(stringAttribute, value));
      case "contains":
        return contains(stringAttribute, value);
      case "notContains":
        return not(contains(stringAttribute, value));
      default:
        return null;
    }
  }

  private Attribute<INode, String> getStringAttributeForINode(String filter) {
    switch (filter) {
      case "user":
        return user;
      case "group":
        return group;
      case "name":
        return name;
      case "path":
        return path;
      default:
        return null;
    }
  }

  private Query<INode> getQueryForLong(
      Attribute<INode, Long> longAttribute, long value, String op) {
    switch (op) {
      case "eq":
        return equal(longAttribute, value);
      case "gt":
        return greaterThan(longAttribute, value);
      case "gte":
        return greaterThanOrEqualTo(longAttribute, value);
      case "lt":
        return lessThan(longAttribute, value);
      case "lte":
        return lessThanOrEqualTo(longAttribute, value);
      case "minutesAgo":
        return greaterThanOrEqualTo(
            longAttribute, System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(value));
      case "hoursAgo":
        return greaterThanOrEqualTo(
            longAttribute, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(value));
      case "daysAgo":
        return greaterThanOrEqualTo(
            longAttribute, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(value));
      case "monthsAgo":
        return greaterThanOrEqualTo(
            longAttribute, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30 * value));
      case "yearsAgo":
        return greaterThanOrEqualTo(
            longAttribute, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(365 * value));
      case "olderThanMinutes":
        return lessThanOrEqualTo(
            longAttribute, System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(value));
      case "olderThanHours":
        return lessThanOrEqualTo(
            longAttribute, System.currentTimeMillis() - TimeUnit.HOURS.toMillis(value));
      case "olderThanDays":
        return lessThanOrEqualTo(
            longAttribute, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(value));
      case "olderThanMonths":
        return lessThanOrEqualTo(
            longAttribute, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30 * value));
      case "olderThanYears":
        return lessThanOrEqualTo(
            longAttribute, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(365 * value));
      default:
        return null;
    }
  }

  private Attribute<INode, Long> getLongAttributeForINode(String filter) {
    switch (filter) {
      case "id":
        return id;
      case "accessTime":
        return accessTime;
      case "modTime":
        return modTime;
      case "fileSize":
        return fileSize;
      case "diskspaceConsumed":
        return diskspaceConsumed;
      case "memoryConsumed":
        return memoryConsumed;
      case "fileReplica":
        return fileReplica;
      case "blockSize":
        return blockSize;
      case "numBlocks":
        return numBlocks;
      case "numReplicas":
        return numReplicas;
      case "depth":
        return depth;
      case "permission":
        return permission;
      case "dirNumChildren":
        return dirNumChildren;
      case "dirSubTreeSize":
        return dirSubTreeSize;
      case "dirSubTreeNumFiles":
        return dirSubTreeNumFiles;
      case "dirSubTreeNumDirs":
        return dirSubTreeNumDirs;
      case "storageType":
        return storageType;
      default:
        return null;
    }
  }

  public HttpServletResponse sql(HttpServletRequest req, HttpServletResponse res) {
    return sql(req, res, null);
  }

  /**
   * Performs an SQL query against this query engine.
   *
   * @param req the http request
   * @param res the http response
   * @param formData the Jersey form data
   * @return The same HttpServletResponse as param.
   */
  public HttpServletResponse sql(
      HttpServletRequest req, HttpServletResponse res, MultivaluedMap<String, String> formData) {
    Map<String, Attribute<INode, ?>> attributes = new HashMap<>();
    attributes.put("id", id);
    attributes.put("accessTime", accessTime);
    attributes.put("modTime", modTime);
    attributes.put("fileSize", fileSize);
    attributes.put("diskspaceConsumed", diskspaceConsumed);
    attributes.put("memoryConsumed", memoryConsumed);
    attributes.put("fileReplica", fileReplica);
    attributes.put("numBlocks", numBlocks);
    attributes.put("numReplicas", numReplicas);
    attributes.put("dirNumChildren", dirNumChildren);
    attributes.put("dirSubTreeSize", dirSubTreeSize);
    attributes.put("dirSubTreeNumFiles", dirSubTreeNumFiles);
    attributes.put("dirSubTreeNumDirs", dirSubTreeNumDirs);
    attributes.put("storageType", storageType);
    attributes.put("depth", depth);
    attributes.put("permission", permission);
    attributes.put("name", name);
    attributes.put("path", path);
    attributes.put("user", user);
    attributes.put("group", group);
    attributes.put("modDate", modDate);
    attributes.put("accessDate", accessDate);
    attributes.put("isUnderConstruction", isUnderConstruction);
    attributes.put("isWithSnapshot", isWithSnapshot);
    attributes.put("hasAcl", hasAcl);
    attributes.put("hasQuota", hasQuota);

    SQLParser<INode> parser = SQLParser.forPojoWithAttributes(INode.class, attributes);

    long count = 0;
    try (PrintWriter out = res.getWriter()) {
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Content-Type", "text/plain");
      String sql;
      if (formData == null) {
        sql = req.getParameter("sqlStatement");
      } else {
        sql = formData.getFirst("sqlStatement");
      }
      ResultSet<INode> results = parser.retrieve(indexedFiles, sql);
      for (INode inode : results) {
        out.println(inode.getFullPathName());
        count++;
      }
      res.setStatus(HttpStatus.SC_OK);
    } catch (IOException e) {
      LOG.warn("SQL statement failed.", e);
    }
    LOG.info("SQL statement produced result of: {} inodes.", count);

    return res;
  }
}
