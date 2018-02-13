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
package org.apache.flink.table.functions.aggfunctions

import java.lang.{Iterable => JIterable}
import java.lang.{Long => JLong}
import java.util
import java.util.function.Consumer

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.functions.AggregateFunction

/** The initial accumulator for count aggregate function with distinct mapping */
class CountDistinctAccumulator[T](var map: MapView[T, Integer]) extends JTuple1[Long] {
  f0 = 0L //count
}

/**
  * built-in count aggregate function
  */
class CountWithDistinctAggFunction[T]
  extends AggregateFunction[JLong, CountDistinctAccumulator[T]] {

  // process argument is optimized by Calcite.
  // For instance count(42) or count(*) will be optimized to count().
  def accumulate(acc: CountDistinctAccumulator[T]): Unit = {
    acc.f0 += 1L
  }

  def retract(acc: CountDistinctAccumulator[T]): Unit = {
    acc.f0 -= 1L
  }

  def accumulate(acc: CountDistinctAccumulator[T], value: Any): Unit = {
    if (value != null && value.isInstanceOf[T]) {
      val v = value.asInstanceOf[T]
      if (acc.map.contains(v)) {
        acc.map.put(v, acc.map.get(v) + 1)
      } else {
        acc.map.put(v, 1)
        acc.f0 += 1L
      }
    }
  }

  def retract(acc: CountDistinctAccumulator[T], value: Any): Unit = {
    if (value != null && value.isInstanceOf[T]) {
      val v = value.asInstanceOf[T]
      if (acc.map.contains(v)) {
        acc.map.put(v, acc.map.get(v) - 1)
        if (acc.map.get(v) == 0) {
          acc.map.remove(v)
          acc.f0 -= 1L
        }
      }
    }
  }

  override def getValue(acc: CountDistinctAccumulator[T]): JLong = {
    acc.f0
  }

  def merge(acc: CountDistinctAccumulator[T],
            its: JIterable[CountDistinctAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      iter.next().map.entries.forEach(new Consumer[util.Map.Entry[T, Integer]] {
        override def accept(t: util.Map.Entry[T, Integer]): Unit =
          if (acc.map.contains(t.getKey)) {
            acc.map.put(t.getKey, acc.map.get(t.getKey) + t.getValue)
          } else {
            acc.map.put(t.getKey, t.getValue)
            acc.f0 += 1L
          }
      })
    }
  }

  override def createAccumulator(): CountDistinctAccumulator[T] = {
    new CountDistinctAccumulator[T](new MapView[T, Integer]())
  }

  def resetAccumulator(acc: CountDistinctAccumulator[T]): Unit = {
    acc.map.clear()
    acc.f0 = 0L
  }

  override def getAccumulatorType: TypeInformation[CountDistinctAccumulator[T]] = {
    new TupleTypeInfo(classOf[CountDistinctAccumulator[T]], BasicTypeInfo.LONG_TYPE_INFO)
  }

  override def getResultType: TypeInformation[JLong] =
    BasicTypeInfo.LONG_TYPE_INFO
}
