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

package org.apache.hadoop.hdfs.server.namenode.queries;

// import org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLoader;
import org.slf4j.Logger;

public class Transforms {

  public static final Logger LOG = NameNodeLoader.LOG;

  private static class Transform {

    Function<INode, Boolean> conditions;
    Function<INode, Long> toLongFunc;

    Transform(Function<INode, Boolean> conditions, Function<INode, Long> toLongFunc) {
      this.conditions = conditions;
      this.toLongFunc = toLongFunc;
    }
  }

  public static Map<String, Function<INode, Long>> getAttributeTransforms(
      String transformConditionArray,
      String transformFields,
      String transformOutputs,
      NameNodeLoader loader) {
    if (transformConditionArray == null
        || transformFields == null
        || transformOutputs == null
        || !loader.isInit()) {
      return Collections.emptyMap();
    }

    Map<String, List<Transform>> transformMap = new HashMap<>();
    long start = System.currentTimeMillis();
    try {
      String[] conditionArray = transformConditionArray.split(";");
      String[] fieldArray = transformFields.split(",");
      String[] outputArray = transformOutputs.split(",");

      assert conditionArray.length == fieldArray.length;
      assert conditionArray.length == outputArray.length;

      for (int i = 0; i < conditionArray.length; i++) {
        Map<String, List<Transform>> transformFunctions =
            transformINodeToLongFunction(conditionArray[i], fieldArray[i], outputArray[i], loader);
        for (Map.Entry<String, List<Transform>> transform : transformFunctions.entrySet()) {
          List<Transform> inMapFunction = transformMap.get(transform.getKey());
          if (inMapFunction != null) {
            List<Transform> toAppendTransforms = transform.getValue();
            inMapFunction.addAll(toAppendTransforms);
          } else {
            transformMap.put(transform.getKey(), transform.getValue());
          }
        }
      }

      return compoundMethods(transformMap, loader);
    } finally {
      long end = System.currentTimeMillis();
      LOG.info("Fetching transform map took: {} ms.", (end - start));
    }
  }

  private static Map<String, List<Transform>> transformINodeToLongFunction(
      String transformConditions,
      String transformField,
      String transformOutput,
      NameNodeLoader loader) {
    Map<String, List<Transform>> transformMap = new HashMap<>(2);
    String[] conditionTriplets = transformConditions.split(",");
    String[][] conditions = new String[conditionTriplets.length][3];
    List<Function<INode, Boolean>> comparisons = new ArrayList<>(conditionTriplets.length);
    for (int i = 0; i < conditionTriplets.length; i++) {
      String triplet = conditionTriplets[i];
      conditions[i] = triplet.split(":");
    }

    // Create comparisons.
    for (String[] condition : conditions) {
      // Long value filters
      Function<INode, Long> longFunction = loader.getFilterFunctionToLongForINode(condition[0]);
      if (longFunction != null) {
        Function<Long, Boolean> longCompFunction =
            loader.getFilterFunctionForLong(Long.parseLong(condition[2]), condition[1]);
        Function<INode, Boolean> comparisonFunction = longCompFunction.compose(longFunction);
        comparisons.add(comparisonFunction);
        continue;
      }

      // String value filters
      Function<INode, String> strFunction = loader.getFilterFunctionToStringForINode(condition[0]);
      if (strFunction != null) {
        Function<String, Boolean> strCompFunction =
            loader.getFilterFunctionForString(condition[2], condition[1]);
        Function<INode, Boolean> comparisonFunction = strCompFunction.compose(strFunction);
        comparisons.add(comparisonFunction);
        continue;
      }

      // Boolean value filters
      Function<INode, Boolean> boolFunction =
          loader.getFilterFunctionToBooleanForINode(condition[0]);
      if (boolFunction != null) {
        Function<Boolean, Boolean> boolCompFunction =
            loader.getFilterFunctionForBoolean(Boolean.parseBoolean(condition[2]), condition[1]);
        Function<INode, Boolean> comparisonFunction = boolCompFunction.compose(boolFunction);
        comparisons.add(comparisonFunction);
        continue;
      }

      throw new IllegalArgumentException("Your transform sucks.");
    }

    // And the functions.
    Function<INode, Boolean> andedComparisons =
        nodeInternal -> {
          for (Function<INode, Boolean> functionInternal : comparisons) {
            if (!functionInternal.apply(nodeInternal)) {
              return false;
            }
          }
          return true;
        };

    switch (transformField) {
      case "fileReplica":
        addFunctionToTransformMap(
            "fileReplica", andedComparisons, node -> Long.parseLong(transformOutput), transformMap);
        addFunctionToTransformMap(
            "numReplicas",
            andedComparisons,
            node -> node.asFile().getBlocks().length * Long.parseLong(transformOutput),
            transformMap);
        addFunctionToTransformMap(
            "diskspaceConsumed",
            andedComparisons,
            node -> Long.parseLong(transformOutput) * node.asFile().computeFileSize(),
            transformMap);
        return transformMap;
        //        addFunctionToTransformMap("storagePolicy", andedComparisons, node -> (long)
        // BlockStoragePolicySuite.createDefaultSuite().getPolicy(transformOutput).getId(),
        // transformMap);
      default:
        throw new IllegalArgumentException("Your transform arguments suck.");
    }
  }

  private static void addFunctionToTransformMap(
      String transformFuncName,
      Function<INode, Boolean> conditionsFunc,
      Function<INode, Long> toLongFunc,
      Map<String, List<Transform>> transformMap) {
    List<Transform> transforms = transformMap.get(transformFuncName);
    if (transforms != null) {
      transforms.add(new Transform(conditionsFunc, toLongFunc));
    } else {
      transformMap.put(
          transformFuncName,
          new ArrayList<Transform>() {
            {
              add(new Transform(conditionsFunc, toLongFunc));
            }
          });
    }
  }

  private static Map<String, Function<INode, Long>> compoundMethods(
      Map<String, List<Transform>> transformMap, NameNodeLoader loader) {
    if (transformMap.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, Function<INode, Long>> compoundedTransforms = new HashMap<>(transformMap.size());
    for (Map.Entry<String, List<Transform>> entry : transformMap.entrySet()) {
      List<Transform> transforms = entry.getValue();
      Function<INode, Long> compoundedFunction =
          node -> {
            for (Transform transform : transforms) {
              if (transform.conditions.apply(node)) {
                return transform.toLongFunc.apply(node);
              }
            }
            return loader.getFilterFunctionToLongForINode(entry.getKey()).apply(node);
          };
      compoundedTransforms.put(entry.getKey(), compoundedFunction);
    }

    return compoundedTransforms;
  }
}
