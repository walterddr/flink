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
package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.logical.{LogicalAggregate, LogicalProject}
import org.apache.flink.table.plan.logical.rel.LogicalDistinctAggregate

class DataStreamLogicalDistinctAggregateRule
  extends RelOptRule(
    RelOptRule.operand(classOf[LogicalAggregate],
      RelOptRule.operand(classOf[LogicalProject], RelOptRule.none())),
    "DataStreamLogicalDistinctAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg = call.rel(0).asInstanceOf[LogicalAggregate]

    val groupSets = agg.getGroupSets.size() != 1 || agg.getGroupSets.get(0) != agg.getGroupSet

    !groupSets && !agg.indicator && agg.containsDistinctCall()
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val agg = call.rel[LogicalAggregate](0)
    val project = agg.getInput.asInstanceOf[HepRelVertex].getCurrentRel.asInstanceOf[LogicalProject]

    val rexBuilder = call.builder().getRexBuilder

    val builder = call.builder()

    val transformed = call.builder()
    transformed.push(LogicalDistinctAggregate.create(agg))
      .project(transformed.fields())

    call.transformTo(transformed.build())
  }
}

object DataStreamLogicalDistinctAggregateRule {
  val INSTANCE = new DataStreamLogicalDistinctAggregateRule
}
