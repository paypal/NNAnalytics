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

package org.apache.hadoop.hdfs.server.namenode.analytics.sql;

import java.util.LinkedList;
import java.util.List;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;

/**
 * Main SQL token traversal implementation. Goal is to translate a SQL query into NNA query fields.
 */
public class INodeSqlStatementVisitor extends StatementVisitorAdapter {

  protected String set;
  protected List<String> filters;
  protected String sum;
  protected String find;
  protected String type;
  protected Integer limit;
  protected Boolean sortAscending;
  protected Boolean sortDescending;
  protected Integer parentDirDepth;
  protected String timeRange;

  INodeSqlStatementVisitor() {
    filters = new LinkedList<>();
  }

  @Override
  public void visit(SetStatement setStatement) {
    String name = setStatement.getName();
    switch (name) {
      case "parentDirDepth":
        parentDirDepth =
            Integer.parseInt(setStatement.getExpression().getASTNode().jjtGetValue().toString());
        return;
      case "timeRange":
        timeRange = setStatement.getExpression().getASTNode().jjtGetValue().toString();
        return;
      default:
        throw new IllegalArgumentException(
            "Tried to set unknown variable: "
                + name
                + ".\n"
                + "Available options are parentDirDepth = <int> and "
                + "timeRange = <daily|weekly|monthly|yearly>.");
    }
  }

  @Override
  public void visit(Select select) {
    select
        .getSelectBody()
        .accept(
            new SelectVisitorAdapter() {
              @Override
              public void visit(PlainSelect plainSelect) {
                plainSelect
                    .getFromItem()
                    .accept(
                        new FromItemVisitorAdapter() {
                          @Override
                          public void visit(Table table) {
                            set = table.getName();
                          }
                        });
                if (plainSelect.getWhere() != null) {
                  plainSelect
                      .getWhere()
                      .accept(
                          new ExpressionVisitorAdapter() {
                            @Override
                            public void visit(OrExpression expr) {
                              throw new UnsupportedOperationException();
                            }

                            @Override
                            public void visit(Between expr) {
                              String field =
                                  expr.getLeftExpression().getASTNode().jjtGetValue().toString();
                              String date1 =
                                  expr.getBetweenExpressionStart()
                                      .getASTNode()
                                      .jjtGetValue()
                                      .toString();
                              String date2 =
                                  expr.getBetweenExpressionEnd()
                                      .getASTNode()
                                      .jjtGetValue()
                                      .toString();
                              long time1;
                              long time2;
                              try {
                                time1 = new DateValue(date1).getValue().getTime();
                                time2 = new DateValue(date2).getValue().getTime();
                              } catch (IllegalArgumentException ignored) {
                                time1 = new TimestampValue(date1).getValue().getTime();
                                time2 = new TimestampValue(date2).getValue().getTime();
                              }
                              if (time1 > time2) {
                                filters.add(field + ":gte:" + time2);
                                filters.add(field + ":lte:" + time1);
                              } else if (time1 < time2) {
                                filters.add(field + ":gte:" + time1);
                                filters.add(field + ":lte:" + time2);
                              } else {
                                filters.add(field + ":eq:" + time1);
                              }
                            }

                            @Override
                            public void visit(LikeExpression expr) {
                              String field =
                                  expr.getLeftExpression().getASTNode().jjtGetValue().toString();
                              String value = expr.getRightExpression().toString();
                              String op;
                              int firstIndex = value.indexOf('%');
                              int lastIndex = value.lastIndexOf('%');
                              if (firstIndex == -1 && lastIndex == -1) {
                                op = "eq";
                              } else if ((firstIndex == 1 || firstIndex == 0)
                                  && (lastIndex == value.length() - 1
                                      || lastIndex == value.length() - 2)) {
                                op = "contains";
                              } else if ((firstIndex == 1 || firstIndex == 0)) {
                                op = "endsWith";
                              } else {
                                op = "startsWith";
                              }
                              filters.add(
                                  field
                                      + ":"
                                      + op
                                      + ":"
                                      + value.replaceAll("%", "").replaceAll("'", ""));
                            }

                            @Override
                            public void visit(EqualsTo expr) {
                              String field =
                                  expr.getLeftExpression().getASTNode().jjtGetValue().toString();
                              String number = expr.getRightExpression().toString();
                              filters.add(field + ":eq:" + number);
                            }

                            @Override
                            public void visit(NotEqualsTo expr) {
                              String field =
                                  expr.getLeftExpression().getASTNode().jjtGetValue().toString();
                              String number = expr.getRightExpression().toString();
                              filters.add(field + ":notEq:" + number);
                            }

                            @Override
                            public void visit(GreaterThan expr) {
                              String field =
                                  expr.getLeftExpression().getASTNode().jjtGetValue().toString();
                              String number = expr.getRightExpression().toString();
                              filters.add(field + ":gt:" + number);
                            }

                            @Override
                            public void visit(GreaterThanEquals expr) {
                              String field =
                                  expr.getLeftExpression().getASTNode().jjtGetValue().toString();
                              String number = expr.getRightExpression().toString();
                              filters.add(field + ":gte:" + number);
                            }

                            @Override
                            public void visit(MinorThan expr) {
                              String field =
                                  expr.getLeftExpression().getASTNode().jjtGetValue().toString();
                              String number = expr.getRightExpression().toString();
                              filters.add(field + ":lt:" + number);
                            }

                            @Override
                            public void visit(MinorThanEquals expr) {
                              String field =
                                  expr.getLeftExpression().getASTNode().jjtGetValue().toString();
                              String number = expr.getRightExpression().toString();
                              filters.add(field + ":lte:" + number);
                            }
                          });
                }
                GroupByElement groupBy = plainSelect.getGroupBy();
                String aggregateFunction;
                String aggregateField;
                if (groupBy != null) {
                  type = plainSelect.getGroupBy().getGroupByExpressions().get(0).toString();
                  aggregateFunction =
                      plainSelect
                          .getSelectItems()
                          .get(1)
                          .getASTNode()
                          .jjtGetFirstToken()
                          .toString();
                  aggregateField =
                      plainSelect
                          .getSelectItems()
                          .get(1)
                          .getASTNode()
                          .jjtGetValue()
                          .toString()
                          .replace(aggregateFunction + "(", "")
                          .replace(")", "");
                } else {
                  aggregateFunction =
                      plainSelect
                          .getSelectItems()
                          .get(0)
                          .getASTNode()
                          .jjtGetFirstToken()
                          .toString();
                  aggregateField =
                      plainSelect
                          .getSelectItems()
                          .get(0)
                          .getASTNode()
                          .jjtGetValue()
                          .toString()
                          .replace(aggregateFunction + "(", "")
                          .replace(")", "");
                }

                switch (aggregateFunction.toUpperCase()) {
                  case "COUNT":
                    sum = "count";
                    break;
                  case "SUM":
                    sum = aggregateField;
                    break;
                  case "MAX":
                  case "MIN":
                  case "AVG":
                    find = aggregateFunction.toLowerCase() + ":" + aggregateField;
                    break;
                  default:
                }
                Limit limitExor = plainSelect.getLimit();
                if (limitExor != null) {
                  limit = Integer.parseInt(limitExor.getRowCount().toString());
                } else {
                  limit = Integer.MAX_VALUE;
                }
                List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
                if (orderByElements != null && !orderByElements.isEmpty()) {
                  OrderByElement orderByElement = orderByElements.get(0);
                  boolean ascDesc = orderByElement.isAscDescPresent();
                  if (ascDesc) {
                    if (orderByElement.isAsc()) {
                      sortAscending = true;
                    } else {
                      sortDescending = true;
                    }
                  }
                }
              }
            });
  }
}
