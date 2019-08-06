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
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Block;
import net.sf.jsqlparser.statement.Commit;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.ShowColumnsStatement;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.UseStatement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.comment.Comment;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.statement.values.ValuesStatement;

public class INodeSqlStatementVisitor implements StatementVisitor {

  String set;
  List<String> filters;
  String sum;
  String find;
  String type;
  Integer limit;

  INodeSqlStatementVisitor() {
    filters = new LinkedList<>();
  }

  @Override
  public void visit(Select select) {
    select
        .getSelectBody()
        .accept(
            new SelectVisitor() {
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
                if (groupBy != null) {
                  type = plainSelect.getGroupBy().getGroupByExpressions().get(0).toString();
                  aggregateFunction =
                      plainSelect
                          .getSelectItems()
                          .get(1)
                          .getASTNode()
                          .jjtGetFirstToken()
                          .toString();
                } else {
                  aggregateFunction =
                      plainSelect
                          .getSelectItems()
                          .get(0)
                          .getASTNode()
                          .jjtGetFirstToken()
                          .toString();
                }
                String aggregateField =
                    plainSelect
                        .getSelectItems()
                        .get(0)
                        .getASTNode()
                        .jjtGetValue()
                        .toString()
                        .replace(aggregateFunction + "(", "")
                        .replace(")", "");
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
              }

              @Override
              public void visit(SetOperationList setOpList) {}

              @Override
              public void visit(WithItem withItem) {}

              @Override
              public void visit(ValuesStatement values) {}
            });
  }

  @Override
  public void visit(Comment comment) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(Commit commit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(Delete delete) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(Update update) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(Insert insert) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(Replace replace) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(Drop drop) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(Truncate truncate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(CreateIndex createIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(CreateTable createTable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(CreateView createView) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(AlterView alterView) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(Alter alter) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(Statements stmts) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(Execute execute) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(SetStatement set) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(ShowColumnsStatement set) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(Merge merge) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(Upsert upsert) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(UseStatement use) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(Block block) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(ValuesStatement values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(DescribeStatement describe) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(ExplainStatement explain) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void visit(ShowStatement show) {
    throw new UnsupportedOperationException();
  }
}
