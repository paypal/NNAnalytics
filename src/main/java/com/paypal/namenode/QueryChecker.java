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
package com.paypal.namenode;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.hdfs.server.namenode.NNAConstants;

class QueryChecker {

  private static Map<String, EnumSet> filterMap = new HashMap<>();
  private static Map<String, EnumSet> sumMap = new HashMap<>();
  private static Map<String, EnumSet> typeMap = new HashMap<>();
  private static Map<String, EnumSet> findMap = new HashMap<>();
  private static ArrayList<String> errorMessages = new ArrayList<>();

  static {
    {
      filterMap.put(NNAConstants.SET.files.toString(), NNAConstants.FILTER_FILE);
      filterMap.put(NNAConstants.SET.dirs.toString(), NNAConstants.FILTER_DIR);
      filterMap.put(NNAConstants.SET.all.toString(), NNAConstants.FILTER_ALL);

      sumMap.put(NNAConstants.SET.files.toString(), NNAConstants.SUM_FILE);
      sumMap.put(NNAConstants.SET.dirs.toString(), NNAConstants.SUM_DIR);
      sumMap.put(NNAConstants.SET.all.toString(), NNAConstants.SUM_ALL);

      typeMap.put(NNAConstants.SET.files.toString(), NNAConstants.TYPE_FILE);
      typeMap.put(NNAConstants.SET.dirs.toString(), NNAConstants.TYPE_DIR);
      typeMap.put(NNAConstants.SET.all.toString(), NNAConstants.TYPE_ALL);

      findMap.put(NNAConstants.SET.files.toString(), NNAConstants.FIND_FILE);
      findMap.put(NNAConstants.SET.dirs.toString(), NNAConstants.FIND_DIR);
      findMap.put(NNAConstants.SET.all.toString(), NNAConstants.FIND_ALL);
    }
  }

  static void isValidQuery(String setType,
      String[] filters,
      String type,
      String sum,
      String[] filterOps,
      String find)
      throws MalformedURLException {
    errorMessages.clear();

    EnumSet validSumOperands = sumMap.get(setType);
    EnumSet validFilterOperands = filterMap.get(setType);
    EnumSet validTypeOperands = typeMap.get(setType);
    EnumSet validFindOperands = findMap.get(setType);

    Pair<Boolean, Pair<String, String>> typeCheck = isValidOperand(type, null, validTypeOperands,
        NNAConstants.OPERAND.type.toString());
    if (!typeCheck.getFirst()) {
      errorMessages.add(
          "Please check /types. Your histogram type: " + typeCheck.getSecond().getFirst()
              + " is not valid for set type: " + setType + ".");
    }

    Pair<Boolean, Pair<String, String>> sumCheck = isValidOperand(sum, null, validSumOperands,
        NNAConstants.OPERAND.sum.toString());
    if (!sumCheck.getFirst()) {
      errorMessages.add("Please check /sums. Your sum type: " + sumCheck.getSecond().getFirst()
          + " is not valid for set type: " + setType + ".");
    }

    Pair<Boolean, Pair<String, String>> filterCheck = isValidOperand(filters, filterOps,
        validFilterOperands, NNAConstants.OPERAND.filter.toString());
    if (!filterCheck.getFirst()) {
      Pair<String, String> result = filterCheck.getSecond();
      if (result.getFirst().length() > 0) {
        errorMessages.add("Please check /filters. Your filter type: " + result.getFirst()
            + " is not valid for set type: " + setType + ".");
      }
      if (result.getSecond().length() > 0) {
        errorMessages.add("Please check /filterOps. " + "\n" + result.getSecond());
      }
    }

    Pair<Boolean, Pair<String, String>> findCheck = isValidOperand(find, null, validFindOperands,
        NNAConstants.OPERAND.find.toString());
    if (!findCheck.getFirst()) {
      errorMessages.add("Please check /finds. Your find type: " + findCheck.getSecond().getFirst()
          + " is not valid for set type: " + setType + ".");
    }

    if (!errorMessages.isEmpty()) {
      throw new MalformedURLException(getMessageText(errorMessages));
    }
  }

  private static Pair<Boolean, Pair<String, String>> isValidOperand(String operand,
      String[] filterOps,
      EnumSet validOperands,
      String operandType) {
    String[] operands = (operand == null || operand.length() == 0) ? null : new String[]{operand};
    return isValidOperand(operands, filterOps, validOperands, operandType);
  }

  private static Pair<Boolean, Pair<String, String>> isValidOperand(String[] operands,
      String[] filterOps,
      EnumSet validOperands,
      String operandType) {
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
            opEnum = NNAConstants.FILTER.valueOf(NNAConstants.FILTER.class, op);
            isFilter = true;
            break;
          case "type":
            opEnum = NNAConstants.HISTOGRAM.valueOf(NNAConstants.HISTOGRAM.class, op);
            break;
          case "sum":
            opEnum = NNAConstants.SUM.valueOf(NNAConstants.SUM.class, op);
            break;
          case "find":
            find = op.split(":");
            opEnum = NNAConstants.FIND.valueOf(NNAConstants.FIND.class, find[0]);
            break;
          default:
            throw new IllegalArgumentException("Operand is not valid: " + op);
        }
        boolean isValid;
        if (find != null && find.length == 2) {
          Enum opOperand = NNAConstants.FIND_FIELD.valueOf(NNAConstants.FIND_FIELD.class, find[1]);
          isValid = validOperands.contains(opOperand);
        } else {
          isValid = validOperands.contains(opEnum);
        }
        isOverallValid &= isValid;
        if (isFilter && filterOps != null) {
          String filterOp = filterOps[i];
          Pair<Boolean, String> result = isValidFilterOp(filterOp, opEnum);
          if (!result.getFirst()) {
            invalidFilterOpString.append("'").append(filterOp).append("'")
                .append(" is not applicable for ").append("'").append(opEnum.toString())
                .append("'.").append("\n").
                append(result.getSecond());
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
    return new Pair<>(isOverallValid,
        new Pair<>(invalidOpString.toString(), invalidFilterOpString.toString()));
  }

  private static Pair<Boolean, String> isValidFilterOp(String op, Enum opEnum) {
    String[] values = op.split(":");
    String operand = values[0];
    String valueToCompare = values[1];
    Enum operandEnum = NNAConstants.FILTER_OP.valueOf(NNAConstants.FILTER_OP.class, operand);
    if (NNAConstants.FILTER_BOOLEAN.contains(opEnum)) {
      if ((NNAConstants.FILTER_BOOLEAN_OPS.contains(operandEnum)) && (
          Objects.equals(valueToCompare, "true") || Objects.equals(valueToCompare, "false"))) {
        return new Pair<>(true, null);
      } else {
        return new Pair<>(false, opEnum.toString() + " requires a Boolean value.");
      }
    } else if (NNAConstants.FILTER_LONG.contains(opEnum)) {
      if (NNAConstants.FILTER_LONG_OPS.contains(operandEnum)) {
        try {
          Long.parseLong(valueToCompare);
        } catch (NumberFormatException e) {
          return new Pair<>(false, opEnum.toString() + " requires a Long value.");
        }
        return new Pair<>(true, null);
      }
    } else if (NNAConstants.FILTER_STRING.contains(opEnum)) {
      if (NNAConstants.FILTER_STRING_OPS.contains(operandEnum)) {
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