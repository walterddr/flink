/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.over;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.streaming.api.TimerService;

public abstract class OverProcessFunction<IN, OUT> extends AbstractRichFunction {


	/**
	 *
	 */
	public abstract class Context {

		/**
		 * Timestamp of the element currently being processed or timestamp of a firing timer.
		 *
		 * <p>This might be {@code null}, for example if the time characteristic of your program
		 * is set to {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}.
		 */
		public abstract Long timestamp();

		/**
		 * A {@link TimerService} for querying time and registering timers.
		 */
		public abstract TimerService timerService();
	}
}

// TODO: Move to base OverOperator
//	protected final long minRetentionTime;
//	protected final long maxRetentionTime;
//	protected final boolean stateCleaningEnabled;
//
//	protected OverProcessFunction(
//		long minRetentionTime,
//		long maxRetentionTime
//	) {
//		this.maxRetentionTime = maxRetentionTime;
//		this.minRetentionTime = minRetentionTime;
//		this.stateCleaningEnabled = minRetentionTime > 1;
//	}
//
//	// holds the latest registered cleanup timer
//	protected ValueState<Long> cleanupTimeState;
//
//	protected void initCleanupTimeState(String stateName) {
//		if (stateCleaningEnabled) {
//			ValueStateDescriptor<Long> cleanupTimeDescriptor =
//				new ValueStateDescriptor<>(stateName, Types.LONG);
//			cleanupTimeState = getRuntimeContext().getState(cleanupTimeDescriptor);
//		}
//	}
//
//	protected void processCleanupTimer(
//		ProcessFunction<IN, OUT>.Context ctx,
//		long currentTime) throws IOException {
//		if (stateCleaningEnabled) {
//			registerProcessingCleanupTimer(
//				cleanupTimeState,
//				currentTime,
//				minRetentionTime,
//				maxRetentionTime,
//				ctx.timerService()
//			);
//		}
//	}
//
//	protected boolean isProcessingTimeTimer(OnTimerContext ctx) {
//		return ctx.timeDomain() == TimeDomain.PROCESSING_TIME;
//	}
//
//	protected void cleanupState(State... states) {
//		// clear all state
//		for (State state: states) {
//			state.clear();
//		}
//		this.cleanupTimeState.clear();
//	}
//
//	private void registerProcessingCleanupTimer(
//		ValueState<Long> cleanupTimeState,
//		long currentTime,
//		long minRetentionTime,
//		long maxRetentionTime,
//		TimerService timerService) throws IOException {
//
//		// last registered timer
//		long curCleanupTime = cleanupTimeState.value();
//
//		// check if a cleanup timer is registered and
//		// that the current cleanup timer won't delete state we need to keep
//		if (curCleanupTime == -1 || (currentTime + minRetentionTime) > curCleanupTime) {
//			// we need to register a new (later) timer
//			long cleanupTime = currentTime + maxRetentionTime;
//			// register timer and remember clean-up time
//			timerService.registerProcessingTimeTimer(cleanupTime);
//			// delete expired timer
//			if (curCleanupTime != -1) {
//				timerService.deleteProcessingTimeTimer(curCleanupTime);
//			}
//			cleanupTimeState.update(cleanupTime);
//		}
//	}
//}
