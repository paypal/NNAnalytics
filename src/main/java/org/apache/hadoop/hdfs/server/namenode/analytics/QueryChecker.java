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

package org.apache.hadoop.hdfs.server.namenode.analytics;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.hdfs.server.namenode.Constants;
import org.apache.hadoop.hdfs.server.namenode.Constants.Filter;
import org.apache.hadoop.hdfs.server.namenode.Constants.FilterOp;
import org.apache.hadoop.hdfs.server.namenode.Constants.Find;
import org.apache.hadoop.hdfs.server.namenode.Constants.FindField;
import org.apache.hadoop.hdfs.server.namenode.Constants.Histogram;
import org.apache.hadoop.hdfs.server.namenode.Constants.INodeSet;
import org.apache.hadoop.hdfs.server.namenode.Constants.Operand;
import org.apache.hadoop.hdfs.server.namenode.Constants.Sum;

/** Responsible for failing early bad NNA queries. */
public class QueryChecker {

  private static Map<String, EnumSet> filterMap = new HashMap<>();
  private static Map<String, EnumSet> sumMap = new HashMap<>();
  private static Map<String, EnumSet> typeMap = new HashMap<>();
  private static Map<String, EnumSet> findMap = new HashMap<>();
  private static ArrayList<String> errorMessages = new ArrayList<>();

  static {
    {
      filterMap.put(INodeSet.files.toString(), Constants.FILTER_FILE);
      filterMap.put(INodeSet.dirs.toString(), Constants.FILTER_DIR);
      filterMap.put(INodeSet.all.toString(), Constants.FILTER_ALL);

      sumMap.put(INodeSet.files.toString(), Constants.SUM_FILE);
      sumMap.put(INodeSet.dirs.toString(), Constants.SUM_DIR);
      sumMap.put(INodeSet.all.toString(), Constants.SUM_ALL);

      typeMap.put(INodeSet.files.toString(), Constants.TYPE_FILE);
      typeMap.put(INodeSet.dirs.toString(), Constants.TYPE_DIR);
      typeMap.put(INodeSet.all.toString(), Constants.TYPE_ALL);

      findMap.put(INodeSet.files.toString(), Constants.FIND_FILE);
      findMap.put(INodeSet.dirs.toString(), Constants.FIND_DIR);
      findMap.put(INodeSet.all.toString(), Constants.FIND_ALL);
    }
  }

  /**
   * Determines whether or not this query should work on NNA.
   *
   * @param setType inode type, files or dirs
   * @param filters the set of filters to pass
   * @param type the histogram type to group by
   * @param sum the summation type
   * @param filterOps the set of filter operations
   * @param find find a min, max, or avg
   * @throws MalformedURLException if it fails to pass the QueryChecker
   */
  public static void isValidQuery(
      String setType, String[] filters, String type, String sum, String[] filterOps, String find)
      throws MalformedURLException {
    errorMessages.clear();

    final EnumSet validSumOperands = sumMap.get(setType);
    final EnumSet validFilterOperands = filterMap.get(setType);
    final EnumSet validTypeOperands = typeMap.get(setType);
    final EnumSet validFindOperands = findMap.get(setType);

    Pair<Boolean, Pair<String, String>> typeCheck =
        isValidOperand(type, null, validTypeOperands, Operand.type.toString());
    if (!typeCheck.getFirst()) {
      errorMessages.add(
          "Please check /types. Your histogram type: "
              + typeCheck.getSecond().getFirst()
              + " is not valid for set type: "
              + setType
              + ".");
    }

    Pair<Boolean, Pair<String, String>> sumCheck =
        isValidOperand(sum, null, validSumOperands, Operand.sum.toString());
    if (!sumCheck.getFirst()) {
      errorMessages.add(
          "Please check /sums. Your sum type: "
              + sumCheck.getSecond().getFirst()
              + " is not valid for set type: "
              + setType
              + ".");
    }

    Pair<Boolean, Pair<String, String>> filterCheck =
        isValidOperand(filters, filterOps, validFilterOperands, Operand.filter.toString());
    if (!filterCheck.getFirst()) {
      Pair<String, String> result = filterCheck.getSecond();
      if (result.getFirst().length() > 0) {
        errorMessages.add(
            "Please check /filters. Your filter type: "
                + result.getFirst()
                + " is not valid for set type: "
                + setType
                + ".");
      }
      if (result.getSecond().length() > 0) {
        errorMessages.add("Please check /filterOps. " + "\n" + result.getSecond());
      }
    }

    Pair<Boolean, Pair<String, String>> findCheck =
        isValidOperand(find, null, validFindOperands, Operand.find.toString());
    if (!findCheck.getFirst()) {
      errorMessages.add(
          "Please check /finds. Your find type: "
              + findCheck.getSecond().getFirst()
              + " is not valid for set type: "
              + setType
              + ".");
    }

    if (!errorMessages.isEmpty()) {
      throw new MalformedURLException(getMessageText(errorMessages));
    }
  }

  private static Pair<Boolean, Pair<String, String>> isValidOperand(
      String operand, String[] filterOps, EnumSet validOperands, String operandType) {
    String[] operands = (operand == null || operand.length() == 0) ? null : new String[] {operand};
    return isValidOperand(operands, filterOps, validOperands, operandType);
  }

  private static Pair<Boolean, Pair<String, String>> isValidOperand(
      String[] operands, String[] filterOps, EnumSet validOperands, String operandType) {
    StringBuilder invalidOpString = new StringBuilder();
    StringBuilder invalidFilterOpString = new StringBuilder();
    boolean isOverallValid = true;
    if (operands != null && operands.length != 0) {
      Enum opEnum;
      boolean isFilter = false;
      boolean isFind = true;
      String[] find = null;
      int i = 0;
      for (String op : operands) {
        switch (operandType) {
          case "filter":
            opEnum = Filter.valueOf(Filter.class, op);
            isFilter = true;
            break;
          case "type":
            opEnum = Histogram.valueOf(Histogram.class, op);
            break;
          case "sum":
            opEnum = Sum.valueOf(Sum.class, op);
            break;
          case "find":
            find = op.split(":");
            opEnum = Find.valueOf(Find.class, find[0]);
            break;
          default:
            throw new IllegalArgumentException("Operand is not valid: " + op);
        }
        boolean isValid;
        if (find != null && find.length == 2) {
          Enum opOperand = FindField.valueOf(FindField.class, find[1]);
          isValid = validOperands.contains(opOperand);
        } else {
          isValid = validOperands.contains(opEnum);
        }
        isOverallValid &= isValid;
        if (isFilter && filterOps != null) {
          String filterOp = filterOps[i];
          Pair<Boolean, String> result = isValidFilterOp(filterOp, opEnum);
          if (!result.getFirst()) {
            invalidFilterOpString
                .append("'")
                .append(filterOp)
                .append("'")
                .append(" is not applicable for ")
                .append("'")
                .append(opEnum.toString())
                .append("'.")
                .append("\n")
                .append(result.getSecond());
            isOverallValid = false;
          }
        }
        if (!isValid) {
          invalidOpString.append(op).append(',');
        }
        i++;
      }
    }
    if (!isOverallValid) {
      if (invalidOpString.length() > 0) {
        invalidOpString.setLength(invalidOpString.length() - 1);
      }
    }
    return new Pair<>(
        isOverallValid, new Pair<>(invalidOpString.toString(), invalidFilterOpString.toString()));
  }

  private static Pair<Boolean, String> isValidFilterOp(String op, Enum opEnum) {
    String[] values = op.split(":");
    String operand = values[0];
    String valueToCompare = values[1];
    Enum operandEnum = FilterOp.valueOf(FilterOp.class, operand);
    if (Constants.FILTER_BOOLEAN.contains(opEnum)) {
      if ((Constants.FILTER_BOOLEAN_OPS.contains(operandEnum))
          && (Objects.equals(valueToCompare, "true") || Objects.equals(valueToCompare, "false"))) {
        return new Pair<>(true, null);
      } else {
        return new Pair<>(false, opEnum.toString() + " requires a Boolean value.");
      }
    } else if (Constants.FILTER_LONG.contains(opEnum)) {
      if (Constants.FILTER_LONG_OPS.contains(operandEnum)) {
        try {
          Long.parseLong(valueToCompare);
        } catch (NumberFormatException e) {
          return new Pair<>(false, opEnum.toString() + " requires a Long value.");
        }
        return new Pair<>(true, null);
      }
    } else if (Constants.FILTER_STRING.contains(opEnum)) {
      if (Constants.FILTER_STRING_OPS.contains(operandEnum)) {
        return new Pair<>(true, null);
      } else {
        return new Pair<>(false, opEnum.toString() + " requires a String value.");
      }
    }

    return new Pair<>(false, null);
  }

  private static String getMessageText(ArrayList<String> errorMessages) {
    StringBuilder messageBuilder = new StringBuilder();
    for (String s : errorMessages) {
      messageBuilder.append(s);
      messageBuilder.append("\n");
    }
    return messageBuilder.toString();
  }
}
