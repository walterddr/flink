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

import java.math.BigDecimal
import java.lang.{Iterable => JIterable}
import java.util
import java.util.Map
import java.util.function.Consumer

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.{PojoField, PojoTypeInfo}
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.dataview.MapViewTypeInfo
import org.apache.flink.table.functions.AggregateFunction

/** The initial accumulator for Sum aggregate distinct function */
class SumWithDistinctAccumulator[T](var map: MapView[T, Integer]) {
  def this() {
    this(new MapView[T, Integer]())
  }
}


/**
  * Base class for built-in Sum distinct aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class SumWithDistinctAggFunction[T: Numeric]
  extends AggregateFunction[T, SumWithDistinctAccumulator[T]] {

  protected val numeric: Numeric[T] = implicitly[Numeric[T]]

  override def createAccumulator(): SumWithDistinctAccumulator[T] = {
    new SumWithDistinctAccumulator[T](
      new MapView[T, Integer]()
    )
  }

  def accumulate(acc: SumWithDistinctAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      if (!acc.map.contains(v)) {
        acc.map.put(v, 1)
      } else {
        acc.map.put(v, acc.map.get(v) + 1)
      }
    }
  }

  def retract(acc: SumWithDistinctAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      if (acc.map.contains(v)) {
        acc.map.put(v, acc.map.get(v) - 1)
        if (acc.map.get(v) <= 0) {
          acc.map.remove(v)
        }
      }
    }
  }

  override def getValue(acc: SumWithDistinctAccumulator[T]): T = {
    if (acc.map.map.size() > 0) {
      var sum = numeric.zero
      acc.map.entries.forEach(new Consumer[Map.Entry[T, Integer]] {
        override def accept(t: Map.Entry[T, Integer]): Unit = {
          sum = numeric.plus(sum, t.getKey)
        }
      })
      sum
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(acc: SumWithDistinctAccumulator[T],
            its: JIterable[SumWithDistinctAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      iter.next.map.entries.forEach(new Consumer[util.Map.Entry[T, Integer]] {
        override def accept(t: util.Map.Entry[T, Integer]): Unit = {
          if (acc.map.contains(t.getKey)) {
            acc.map.put(t.getKey, acc.map.get(t.getKey) + t.getValue)
          } else {
            acc.map.put(t.getKey, t.getValue)
          }
        }
      })
    }
  }

  def resetAccumulator(acc: SumWithDistinctAccumulator[T]): Unit = {
    acc.map.map.clear()
  }

  override def getAccumulatorType: TypeInformation[SumWithDistinctAccumulator[T]] = {
    val clazz = classOf[SumWithDistinctAccumulator[T]]
    val pojoFields = new util.ArrayList[PojoField]
    pojoFields.add(new PojoField(clazz.getDeclaredField("map"),
      new MapViewTypeInfo[T, Integer](
        getValueTypeInfo.asInstanceOf[TypeInformation[T]], BasicTypeInfo.INT_TYPE_INFO)))
    new PojoTypeInfo[SumWithDistinctAccumulator[T]](clazz, pojoFields)
  }

  def getValueTypeInfo: TypeInformation[_]
}

/**
  * Built-in Byte Sum with Distinct aggregate function
  */
class ByteSumWithDistinctAggFunction extends SumWithDistinctAggFunction[Byte] {
  override def getValueTypeInfo = BasicTypeInfo.BYTE_TYPE_INFO
}

/**
  * Built-in Short Sum with Distinct aggregate function
  */
class ShortSumWithDistinctAggFunction extends SumWithDistinctAggFunction[Short] {
  override def getValueTypeInfo = BasicTypeInfo.SHORT_TYPE_INFO
}

/**
  * Built-in Int Sum with Distinct aggregate function
  */
class IntSumWithDistinctAggFunction extends SumWithDistinctAggFunction[Int] {
  override def getValueTypeInfo = BasicTypeInfo.INT_TYPE_INFO
}

/**
  * Built-in Long Sum with Distinct aggregate function
  */
class LongSumWithDistinctAggFunction extends SumWithDistinctAggFunction[Long] {
  override def getValueTypeInfo = BasicTypeInfo.LONG_TYPE_INFO
}

/**
  * Built-in Float Sum with Distinct aggregate function
  */
class FloatSumWithDistinctAggFunction extends SumWithDistinctAggFunction[Float] {
  override def getValueTypeInfo = BasicTypeInfo.FLOAT_TYPE_INFO
}

/**
  * Built-in Double Sum with Distinct aggregate function
  */
class DoubleSumWithDistinctAggFunction extends SumWithDistinctAggFunction[Double] {
  override def getValueTypeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO
}

/** The initial accumulator for Big Decimal Sum with Distinct aggregate function */
class DecimalSumWithDistinctAccumulator(var map: MapView[BigDecimal, Integer]) {
  def this() {
    this(new MapView[BigDecimal, Integer])
  }
}

/**
  * Built-in Big Decimal Sum with distinct aggregate function
  */
class DecimalSumWithDistinctAggFunction
  extends AggregateFunction[BigDecimal, DecimalSumWithDistinctAccumulator] {

  override def createAccumulator(): DecimalSumWithDistinctAccumulator = {
    new DecimalSumWithDistinctAccumulator(new MapView[BigDecimal, Integer]())
  }

  def accumulate(acc: DecimalSumWithDistinctAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      if (!acc.map.contains(v)) {
        acc.map.put(v, 1)
      } else {
        acc.map.put(v, acc.map.get(v) + 1)
      }
    }
  }

  def retract(acc: DecimalSumWithDistinctAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[BigDecimal]
      if (acc.map.contains(v)) {
        acc.map.put(v, acc.map.get(v) - 1)
        if (acc.map.get(v) <= 0) {
          acc.map.remove(v)
        }
      }
    }
  }

  override def getValue(acc: DecimalSumWithDistinctAccumulator): BigDecimal = {
    if (acc.map.map.size >= 0) {
      var sum = BigDecimal.ZERO
      acc.map.entries.forEach(new Consumer[Map.Entry[BigDecimal, Integer]] {
        override def accept(t: Map.Entry[BigDecimal, Integer]): Unit = {
          sum = sum.add(t.getKey)
        }
      })
      sum
    } else {
      null.asInstanceOf[BigDecimal]
    }
  }

  def merge(acc: DecimalSumWithDistinctAccumulator,
            its: JIterable[DecimalSumWithDistinctAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      iter.next.map.entries.forEach(new Consumer[util.Map.Entry[BigDecimal, Integer]] {
        override def accept(t: util.Map.Entry[BigDecimal, Integer]): Unit = {
          if (acc.map.contains(t.getKey)) {
            acc.map.put(t.getKey, acc.map.get(t.getKey) + t.getValue)
          } else {
            acc.map.put(t.getKey, t.getValue)
          }
        }
      })
    }
  }

  def resetAccumulator(acc: DecimalSumWithDistinctAccumulator): Unit = {
    acc.map.clear()
  }

  override def getAccumulatorType: TypeInformation[DecimalSumWithDistinctAccumulator] = {
    val clazz = classOf[DecimalSumWithDistinctAccumulator]
    val pojoFields = new util.ArrayList[PojoField]
    pojoFields.add(new PojoField(clazz.getDeclaredField("map"),
      new MapViewTypeInfo[BigDecimal, Integer](
        BasicTypeInfo.BIG_DEC_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO)))
    new PojoTypeInfo[DecimalSumWithDistinctAccumulator](clazz, pojoFields)
  }
}
