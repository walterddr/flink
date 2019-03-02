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

package org.apache.flink.streaming.api.windowing.triggers;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.windows.VoidWindow;

/**
 *
 */
@PublicEvolving
public class EventTimeSameWindowTrigger extends Trigger<Object, VoidWindow> {
	private static final long serialVersionUID = 1L;

	private EventTimeSameWindowTrigger() {}

	@Override
	public TriggerResult onElement(Object element, long timestamp, VoidWindow window, TriggerContext ctx) throws Exception {
		ctx.registerEventTimeTimer(timestamp + 1); // Fire on next millisecond
		return TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onEventTime(long time, VoidWindow window, TriggerContext ctx) {
		return TriggerResult.FIRE;
	}

	@Override
	public TriggerResult onProcessingTime(long time, VoidWindow window, TriggerContext ctx) throws Exception {
		return TriggerResult.CONTINUE;
	}

	@Override
	public void clear(VoidWindow window, TriggerContext ctx) throws Exception {
		// Continue, this cannot be supported as of now
	}

	@Override
	public boolean canMerge() {
		return false;
	}

	@Override
	public void onMerge(VoidWindow window,
			OnMergeContext ctx) {
		throw new RuntimeException("same window trigger does not support merge!");
	}

	@Override
	public String toString() {
		return "EventTimeSameWindowTrigger()";
	}

	/**
	 * Creates an event-time same window trigger that fires once the watermark passes
	 * the element time.
	 */
	public static EventTimeSameWindowTrigger create() {
		return new EventTimeSameWindowTrigger();
	}
}
