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

package org.apache.flink.streaming.api.scala

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.FoldFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.testutils.{CheckingIdentityRichAllWindowFunction, CheckingIdentityRichProcessAllWindowFunction, CheckingIdentityRichProcessWindowFunction, CheckingIdentityRichWindowFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, VoidWindow}
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.util.Collector
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

/**
 * Tests for Folds over windows. These also test whether OutputTypeConfigurable functions
 * work for windows, because FoldWindowFunction is OutputTypeConfigurable.
 */
class SameWindowITCase extends AbstractTestBase {

  @Test
  def testSameWindowReduce(): Unit = {
    SameWindowITCase.testResults = mutable.MutableList()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val source1 = env.addSource(new SourceFunction[(String, Int)]() {
      def run(ctx: SourceFunction.SourceContext[(String, Int)]) {
        ctx.collect(("a", 0))
        ctx.collect(("a", 100))
        ctx.collect(("a", 200))
        ctx.collect(("b", 300))
        ctx.collect(("b", 400))
        ctx.collect(("b", 500))
        ctx.collect(("a", 600))
        ctx.collect(("a", 700))
        ctx.collect(("a", 800))

        // source is finite, so it will have an implicit MAX watermark when it finishes
      }

      def cancel() {
      }
    }).assignTimestampsAndWatermarks(new SameWindowITCase.Tuple2TimestampExtractor)

    source1
      .keyBy(0)
      .countOver(3)
      .apply(new WindowFunction[(String, Int), (String, Int), Tuple, VoidWindow]() {
        override def apply(key: Tuple, window: VoidWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          out.collect(input.reduce((l, r) => (l._1, l._2 + r._2)))
        }
      })
      .addSink(new SinkFunction[(String, Int)]() {
        override def invoke(value: (String, Int)) {
        SameWindowITCase.testResults += value.toString
        }
      })

    env.execute("Reduce Same Window Test")

    val expectedResult = mutable.MutableList(
      "(a,0)",
      "(a,100)",
      "(a,300)",
      "(a,900)",
      "(a,1500)",
      "(a,2100)",
      "(b,300)",
      "(b,700)",
      "(b,1200)")

    assertEquals(expectedResult.sorted, SameWindowITCase.testResults.sorted)
  }

//  @Test
//  def testFoldWithWindowFunction(): Unit = {
//    SameWindowITCase.testResults = mutable.MutableList()
//    CheckingIdentityRichWindowFunction.reset()
//
//    val foldFunc = new FoldFunction[(String, Int), (String, Int)] {
//      override def fold(accumulator: (String, Int), value: (String, Int)): (String, Int) = {
//        (accumulator._1 + value._1, accumulator._2 + value._2)
//      }
//    }
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)
//
//    val source1 = env.addSource(new SourceFunction[(String, Int)]() {
//      def run(ctx: SourceFunction.SourceContext[(String, Int)]) {
//        ctx.collect(("a", 0))
//        ctx.collect(("a", 1))
//        ctx.collect(("a", 2))
//        ctx.collect(("b", 3))
//        ctx.collect(("b", 4))
//        ctx.collect(("b", 5))
//        ctx.collect(("a", 6))
//        ctx.collect(("a", 7))
//        ctx.collect(("a", 8))
//
//        // source is finite, so it will have an implicit MAX watermark when it finishes
//      }
//
//      def cancel() {
//      }
//    }).assignTimestampsAndWatermarks(new SameWindowITCase.Tuple2TimestampExtractor)
//
//    source1
//      .keyBy(0)
//      .window(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
//      .fold(
//        ("R:", 0),
//        foldFunc,
//        new CheckingIdentityRichWindowFunction[(String, Int), Tuple, TimeWindow]())
//      .addSink(new SinkFunction[(String, Int)]() {
//        override def invoke(value: (String, Int)) {
//          SameWindowITCase.testResults += value.toString
//        }
//      })
//
//    env.execute("Fold Window Test")
//
//    val expectedResult = mutable.MutableList(
//      "(R:aaa,3)",
//      "(R:aaa,21)",
//      "(R:bbb,12)")
//
//    assertEquals(expectedResult.sorted, SameWindowITCase.testResults.sorted)
//
//    CheckingIdentityRichWindowFunction.checkRichMethodCalls()
//  }
//
//  @Test
//  def testFoldWithProcessWindowFunction(): Unit = {
//    SameWindowITCase.testResults = mutable.MutableList()
//    CheckingIdentityRichProcessWindowFunction.reset()
//
//    val foldFunc = new FoldFunction[(String, Int), (Int, String)] {
//      override def fold(accumulator: (Int, String), value: (String, Int)): (Int, String) = {
//        (accumulator._1 + value._2, accumulator._2 + value._1)
//      }
//    }
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)
//
//    val source1 = env.addSource(new SourceFunction[(String, Int)]() {
//      def run(ctx: SourceFunction.SourceContext[(String, Int)]) {
//        ctx.collect(("a", 0))
//        ctx.collect(("a", 1))
//        ctx.collect(("a", 2))
//        ctx.collect(("b", 3))
//        ctx.collect(("b", 4))
//        ctx.collect(("b", 5))
//        ctx.collect(("a", 6))
//        ctx.collect(("a", 7))
//        ctx.collect(("a", 8))
//
//        // source is finite, so it will have an implicit MAX watermark when it finishes
//      }
//
//      def cancel() {
//      }
//    }).assignTimestampsAndWatermarks(new SameWindowITCase.Tuple2TimestampExtractor)
//
//    source1
//      .keyBy(0)
//      .window(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
//      .fold(
//        (0, "R:"),
//        foldFunc,
//        new CheckingIdentityRichProcessWindowFunction[(Int, String), Tuple, TimeWindow]())
//      .addSink(new SinkFunction[(Int, String)]() {
//        override def invoke(value: (Int, String)) {
//          SameWindowITCase.testResults += value.toString
//        }
//      })
//
//    env.execute("Fold Process Window Test")
//
//    val expectedResult = mutable.MutableList(
//      "(3,R:aaa)",
//      "(21,R:aaa)",
//      "(12,R:bbb)")
//
//    assertEquals(expectedResult.sorted, SameWindowITCase.testResults.sorted)
//
//    CheckingIdentityRichProcessWindowFunction.checkRichMethodCalls()
//  }
//
//  @Test
//  def testFoldAllWindow(): Unit = {
//    SameWindowITCase.testResults = mutable.MutableList()
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)
//
//    val source1 = env.addSource(new SourceFunction[(String, Int)]() {
//      def run(ctx: SourceFunction.SourceContext[(String, Int)]) {
//        ctx.collect(("a", 0))
//        ctx.collect(("a", 1))
//        ctx.collect(("a", 2))
//        ctx.collect(("b", 3))
//        ctx.collect(("a", 3))
//        ctx.collect(("b", 4))
//        ctx.collect(("a", 4))
//        ctx.collect(("b", 5))
//        ctx.collect(("a", 5))
//
//        // source is finite, so it will have an implicit MAX watermark when it finishes
//      }
//
//      def cancel() {
//      }
//    }).assignTimestampsAndWatermarks(new SameWindowITCase.Tuple2TimestampExtractor)
//
//    source1
//      .windowAll(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
//      .fold(("R:", 0)) { (acc: (String, Int), v: (String, Int)) => (acc._1 + v._1, acc._2 + v._2) }
//      .addSink(new SinkFunction[(String, Int)]() {
//      override def invoke(value: (String, Int)) {
//        SameWindowITCase.testResults += value.toString
//      }
//    })
//
//    env.execute("Fold All-Window Test")
//
//    val expectedResult = mutable.MutableList(
//      "(R:aaa,3)",
//      "(R:bababa,24)")
//
//    assertEquals(expectedResult.sorted, SameWindowITCase.testResults.sorted)
//  }
//
//  @Test
//  def testFoldAllWithWindowFunction(): Unit = {
//    SameWindowITCase.testResults = mutable.MutableList()
//    CheckingIdentityRichAllWindowFunction.reset()
//
//    val foldFunc = new FoldFunction[(String, Int), (String, Int)] {
//      override def fold(accumulator: (String, Int), value: (String, Int)): (String, Int) = {
//        (accumulator._1 + value._1, accumulator._2 + value._2)
//      }
//    }
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)
//
//    val source1 = env.addSource(new SourceFunction[(String, Int)]() {
//      def run(ctx: SourceFunction.SourceContext[(String, Int)]) {
//        ctx.collect(("a", 0))
//        ctx.collect(("a", 1))
//        ctx.collect(("a", 2))
//        ctx.collect(("b", 3))
//        ctx.collect(("a", 3))
//        ctx.collect(("b", 4))
//        ctx.collect(("a", 4))
//        ctx.collect(("b", 5))
//        ctx.collect(("a", 5))
//
//        // source is finite, so it will have an implicit MAX watermark when it finishes
//      }
//
//      def cancel() {
//      }
//    }).assignTimestampsAndWatermarks(new SameWindowITCase.Tuple2TimestampExtractor)
//
//    source1
//      .windowAll(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
//      .fold(
//        ("R:", 0),
//        foldFunc,
//        new CheckingIdentityRichAllWindowFunction[(String, Int), TimeWindow]())
//      .addSink(new SinkFunction[(String, Int)]() {
//        override def invoke(value: (String, Int)) {
//          SameWindowITCase.testResults += value.toString
//        }
//      })
//
//    env.execute("Fold All-Window Test")
//
//    val expectedResult = mutable.MutableList(
//      "(R:aaa,3)",
//      "(R:bababa,24)")
//
//    assertEquals(expectedResult.sorted, SameWindowITCase.testResults.sorted)
//
//    CheckingIdentityRichAllWindowFunction.checkRichMethodCalls()
//  }
//
//  @Test
//  def testFoldAllWithProcessWindowFunction(): Unit = {
//    SameWindowITCase.testResults = mutable.MutableList()
//    CheckingIdentityRichProcessAllWindowFunction.reset()
//
//    val foldFunc = new FoldFunction[(String, Int), (String, Int)] {
//      override def fold(accumulator: (String, Int), value: (String, Int)): (String, Int) = {
//        (accumulator._1 + value._1, accumulator._2 + value._2)
//      }
//    }
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.setParallelism(1)
//
//    val source1 = env.addSource(new SourceFunction[(String, Int)]() {
//      def run(ctx: SourceFunction.SourceContext[(String, Int)]) {
//        ctx.collect(("a", 0))
//        ctx.collect(("a", 1))
//        ctx.collect(("a", 2))
//        ctx.collect(("b", 3))
//        ctx.collect(("a", 3))
//        ctx.collect(("b", 4))
//        ctx.collect(("a", 4))
//        ctx.collect(("b", 5))
//        ctx.collect(("a", 5))
//
//        // source is finite, so it will have an implicit MAX watermark when it finishes
//      }
//
//      def cancel() {
//      }
//    }).assignTimestampsAndWatermarks(new SameWindowITCase.Tuple2TimestampExtractor)
//
//    source1
//      .windowAll(TumblingEventTimeWindows.of(Time.of(3, TimeUnit.MILLISECONDS)))
//      .fold(
//        ("R:", 0),
//        foldFunc,
//        new CheckingIdentityRichProcessAllWindowFunction[(String, Int), TimeWindow]())
//      .addSink(new SinkFunction[(String, Int)]() {
//        override def invoke(value: (String, Int)) {
//          SameWindowITCase.testResults += value.toString
//        }
//      })
//
//    env.execute("Fold All-Window Test")
//
//    val expectedResult = mutable.MutableList(
//      "(R:aaa,3)",
//      "(R:bababa,24)")
//
//    assertEquals(expectedResult.sorted, SameWindowITCase.testResults.sorted)
//
//    CheckingIdentityRichProcessAllWindowFunction.checkRichMethodCalls()
//  }
}


object SameWindowITCase {
  private var testResults: mutable.MutableList[String] = null

  private class Tuple2TimestampExtractor extends AssignerWithPunctuatedWatermarks[(String, Int)] {

    private var currentTimestamp = -1L

    override def extractTimestamp(element: (String, Int), previousTimestamp: Long): Long = {
      currentTimestamp = element._2
      currentTimestamp
    }

    def checkAndGetNextWatermark(
        lastElement: (String, Int),
        extractedTimestamp: Long): Watermark = {
      new Watermark(lastElement._2 - 1)
    }
  }
}

