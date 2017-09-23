/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api.batch.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import org.apache.flink.table.utils.{BatchTableTestUtil, TableTestBase}
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.types.Row
import org.junit.Test

class MyTest extends TableTestBase {

  def addSpecialTypeTable[T](util: BatchTableTestUtil, typeInfo: TypeInformation[T], name: String, fields: Expression*): Table = {
    util.addTable[T](name, fields: _*)(typeInfo)
  }

  @Test
  def testCompositeTypeTableFunction(): Unit = {
    val util = batchTestUtil()
    val typeInfo: TypeInformation[Row] = new RowTypeInfo(
      Types.INT,
      Types.ROW(Types.INT, Types.STRING)
    )
    addSpecialTypeTable(util, typeInfo, "MyTable", 'a, 'b)

    val func = new TableFunc
    util.addFunction("func", func)

    val sqlQuery = "SELECT s FROM MyTable, LATERAL TABLE(func(b)) AS T(s)"

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", "func($cor0.b)"),
        term("function", func.getClass.getCanonicalName),
        term("rowType",
          "RecordType(INTEGER a, RecordType(BIGINT f0, VARCHAR(65536) f1))"),
        term("joinType", "INNER")
      ),
      term("select", "f0 AS s")
    )

    util.verifySql(sqlQuery, expected)
  }

  @Test
  def testCompositeTypeScalaFunction(): Unit = {
    val util = batchTestUtil()
    val typeInfo: TypeInformation[Row] = new RowTypeInfo(
      Types.INT,
      Types.ROW(Types.INT, Types.STRING)
    )
    addSpecialTypeTable(util, typeInfo, "MyTable", 'a, 'b)

    val func = new ScalaFunc()
    util.addFunction("func", func)

    val sqlQuery = "SELECT a, func(b) FROM MyTable"

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(0),
      term("select", "a", "func(b) AS EXPR$1")
    )

    util.verifySql(sqlQuery, expected)
  }
}

class TableFunc extends TableFunction[String] {
  def eval(o: CompositeType[(Long, String)]): Unit = {
    val row: Row = o.asInstanceOf[Row]
    (0 until row.getArity).map((idx: Int) => row.getField(idx).toString).foreach(collect)
  }
}

class ScalaFunc extends ScalarFunction {
  def eval(row: Row): Int = {
    row.getArity
  }
}
