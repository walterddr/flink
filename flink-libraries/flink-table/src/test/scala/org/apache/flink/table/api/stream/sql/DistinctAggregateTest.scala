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

package org.apache.flink.table.api.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.logical.{SessionGroupWindow, SlidingGroupWindow, TumblingGroupWindow}
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.{Ignore, Test}

class DistinctAggregateTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)](
    "MyTable",
    'a, 'b, 'c,
    'proctime.proctime, 'rowtime.rowtime)

  @Test
  def testDistinct(): Unit = {
    val sql = "SELECT DISTINCT a, b, c FROM MyTable"

    val expected =
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a, b, c")
        ),
        term("groupBy", "a, b, c"),
        term("select", "a, b, c")
      )
    streamUtil.verifySql(sql, expected)
  }

  // TODO: this query should be optimized to only have a single DataStreamGroupAggregate
  // TODO: reopen this until FLINK-7144 fixed
  @Ignore
  @Test
  def testDistinctAfterAggregate(): Unit = {
    val sql = "SELECT DISTINCT a FROM MyTable GROUP BY a, b, c"

    val expected =
      unaryNode(
        "DataStreamGroupAggregate",
        unaryNode(
          "DataStreamCalc",
          streamTableNode(0),
          term("select", "a")
        ),
        term("groupBy", "a"),
        term("select", "a")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testDistinctAggregateOnTumbleWindow(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), " +
      "  SUM(a) " +
      "FROM MyTable " +
      "GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE) "

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "rowtime", "a")
      ),
      term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select", "COUNT(DISTINCT a) AS EXPR$0", "SUM(a) AS EXPR$1")
    )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testMultiDistinctAggregateSameFieldOnHopWindow(): Unit = {
    val sqlQuery = "SELECT COUNT(DISTINCT a), " +
      "  SUM(DISTINCT a), " +
      "  MAX(DISTINCT a) " +
      "FROM MyTable " +
      "GROUP BY HOP(rowtime, INTERVAL '15' MINUTE, INTERVAL '1' HOUR) "

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "rowtime", "a")
      ),
      term("window", SlidingGroupWindow('w$, 'rowtime, 3600000.millis, 900000.millis)),
      term("select", "COUNT(DISTINCT a) AS EXPR$0", "SUM(DISTINCT a) AS EXPR$1",
        "MAX(DISTINCT a) AS EXPR$2")
    )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testDistinctAggregateWithGroupingOnSessionWindow(): Unit = {
    val sqlQuery = "SELECT a, " +
      "  COUNT(a), " +
      "  SUM(DISTINCT c) " +
      "FROM MyTable " +
      "GROUP BY a, SESSION(rowtime, INTERVAL '15' MINUTE) "

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "a", "rowtime", "c")
      ),
      term("groupBy", "a"),
      term("window", SessionGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select", "a", "COUNT(a) AS EXPR$1", "SUM(DISTINCT c) AS EXPR$2")
    )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testDistinctAggregateWithNonDistinctAndGrouping(): Unit = {
    val sqlQuery = "SELECT a, " +
      "  COUNT(*), " +
      "  SUM(DISTINCT c), " +
      "  COUNT(DISTINCT b), " +
      "  SUM(c), " +
      "  COUNT(b) " +
      "FROM MyTable " +
      "GROUP BY a, TUMBLE(rowtime, INTERVAL '15' MINUTE) "

    val expected = unaryNode(
      "DataStreamGroupWindowAggregate",
      unaryNode(
        "DataStreamCalc",
        streamTableNode(0),
        term("select", "a", "rowtime", "c", "b")
      ),
      term("groupBy", "a"),
      term("window", TumblingGroupWindow('w$, 'rowtime, 900000.millis)),
      term("select",
        "a",
        "COUNT(*) AS EXPR$1",
        "SUM(DISTINCT c) AS EXPR$2",
        "COUNT(DISTINCT b) AS EXPR$3",
        "SUM(c) AS EXPR$4",
        "COUNT(b) AS EXPR$5"
      )
    )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testProctimeBoundedDistinctWithNonDistinctPartitionedRowOver() = {
    val sql = "SELECT " +
      "b, " +
      "count(a) OVER (PARTITION BY b ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as cnt1, " +
      "sum(a) OVER (PARTITION BY b ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as sum1, " +
      "count(DISTINCT a) OVER (PARTITION BY b ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as cnt2, " +
      "sum(DISTINCT c) OVER (PARTITION BY b ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as sum2 " +
      "from MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "b", "c", "proctime")
          ),
          term("partitionBy", "b"),
          term("orderBy", "proctime"),
          term("rows", "BETWEEN 2 PRECEDING AND CURRENT ROW"),
          term("select", "a", "b", "c", "proctime", "COUNT(a) AS w0$o0, $SUM0(a) AS w0$o1, " +
            "COUNT(DISTINCT a) AS w0$o2, COUNT(DISTINCT c) AS w0$o3, $SUM0(DISTINCT c) AS w0$o4")
        ),
        term("select", "b", "w0$o0 AS cnt1, CASE(>(w0$o0, 0), CAST(w0$o1), null) AS sum1, " +
          "w0$o2 AS cnt2, CASE(>(w0$o3, 0), CAST(w0$o4), null) AS sum2")
      )
    streamUtil.verifySql(sql, expected)
  }

  @Test
  def testProctimeBoundedDistinctPartitionedRowOver() = {
    val sql = "SELECT " +
      "c, " +
      "count(DISTINCT a) OVER (PARTITION BY c ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as cnt1, " +
      "sum(DISTINCT a) OVER (PARTITION BY c ORDER BY proctime ROWS BETWEEN 2 preceding AND " +
      "CURRENT ROW) as sum1 " +
      "from MyTable"

    val expected =
      unaryNode(
        "DataStreamCalc",
        unaryNode(
          "DataStreamOverAggregate",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c", "proctime")
          ),
          term("partitionBy", "c"),
          term("orderBy", "proctime"),
          term("rows", "BETWEEN 2 PRECEDING AND CURRENT ROW"),
          term("select", "a", "c", "proctime",
            "COUNT(DISTINCT a) AS w0$o0, $SUM0(DISTINCT a) AS w0$o1")
        ),
        term("select", "c", "w0$o0 AS cnt1, CASE(>(w0$o0, 0), CAST(w0$o1), null) AS sum1")
      )
    streamUtil.verifySql(sql, expected)
  }
}
