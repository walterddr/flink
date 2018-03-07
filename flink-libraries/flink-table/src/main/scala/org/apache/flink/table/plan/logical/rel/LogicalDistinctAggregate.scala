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

package org.apache.flink.table.plan.logical.rel

import java.util

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelShuttle}
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.table.calcite.FlinkTypeFactory


class LogicalDistinctAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode,
    indicatorFlag: Boolean,
    groupSet: ImmutableBitSet,
    groupSets: util.List[ImmutableBitSet],
    aggCalls: util.List[AggregateCall])
  extends Aggregate(cluster, traitSet, child, indicatorFlag, groupSet, groupSets, aggCalls) {

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      indicator: Boolean,
      groupSet: ImmutableBitSet,
      groupSets: util.List[ImmutableBitSet],
      aggCalls: util.List[AggregateCall])
    : Aggregate = {

    new LogicalDistinctAggregate(
      cluster,
      traitSet,
      input,
      indicator,
      groupSet,
      groupSets,
      aggCalls)
  }

  override def accept(shuttle: RelShuttle): RelNode = shuttle.visit(this)

  override def deriveRowType(): RelDataType = {
    val aggregateRowType = super.deriveRowType()
    val typeFactory = getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val builder = typeFactory.builder
    builder.addAll(aggregateRowType.getFieldList)
    builder.build()
  }
}

object LogicalDistinctAggregate {
  def create(aggregate: Aggregate): LogicalDistinctAggregate = {

    val cluster: RelOptCluster = aggregate.getCluster
    val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
    new LogicalDistinctAggregate(
      cluster,
      traitSet,
      aggregate.getInput,
      aggregate.indicator,
      aggregate.getGroupSet,
      aggregate.getGroupSets,
      aggregate.getAggCallList)
  }
}
