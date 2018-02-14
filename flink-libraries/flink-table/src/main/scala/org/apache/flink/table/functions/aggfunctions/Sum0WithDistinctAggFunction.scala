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
import java.util.Map
import java.util.function.Consumer

import org.apache.flink.api.common.typeinfo.BasicTypeInfo

/**
  * Base class for built-in Sum0 with retract aggregate function.
  * If all values are null, 0 is returned.
  *
  * @tparam T the type for the aggregation result
  */
abstract class Sum0WithDistinctAggFunction[T: Numeric] extends SumWithDistinctAggFunction[T] {

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
      0.asInstanceOf[T]
    }
  }
}

/**
  * Built-in Byte Sum0 with retract aggregate function
  */
class ByteSum0WithDistinctAggFunction extends Sum0WithDistinctAggFunction[Byte] {
  override def getValueTypeInfo = BasicTypeInfo.BYTE_TYPE_INFO
}

/**
  * Built-in Short Sum0 with retract aggregate function
  */
class ShortSum0WithDistinctAggFunction extends Sum0WithDistinctAggFunction[Short] {
  override def getValueTypeInfo = BasicTypeInfo.SHORT_TYPE_INFO
}

/**
  * Built-in Int Sum0 with retract aggregate function
  */
class IntSum0WithDistinctAggFunction extends Sum0WithDistinctAggFunction[Int] {
  override def getValueTypeInfo = BasicTypeInfo.INT_TYPE_INFO
}

/**
  * Built-in Long Sum0 with retract aggregate function
  */
class LongSum0WithDistinctAggFunction extends Sum0WithDistinctAggFunction[Long] {
  override def getValueTypeInfo = BasicTypeInfo.LONG_TYPE_INFO
}

/**
  * Built-in Float Sum0 with retract aggregate function
  */
class FloatSum0WithDistinctAggFunction extends Sum0WithDistinctAggFunction[Float] {
  override def getValueTypeInfo = BasicTypeInfo.FLOAT_TYPE_INFO
}

/**
  * Built-in Double Sum0 with retract aggregate function
  */
class DoubleSum0WithDistinctAggFunction extends Sum0WithDistinctAggFunction[Double] {
  override def getValueTypeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO
}

/**
  * Built-in Big Decimal Sum0 with retract aggregate function
  */
class DecimalSum0WithDistinctAggFunction extends DecimalSumWithDistinctAggFunction {

  override def getValue(acc: DecimalSumWithDistinctAccumulator): BigDecimal = {
    if (acc.map.map.size > 0) {
      var sum = BigDecimal.ZERO
      acc.map.entries.forEach(new Consumer[Map.Entry[BigDecimal, Integer]] {
        override def accept(t: Map.Entry[BigDecimal, Integer]): Unit = {
          sum = sum.add(t.getKey)
        }
      })
      sum
    } else {
      BigDecimal.ZERO
    }
  }
}
