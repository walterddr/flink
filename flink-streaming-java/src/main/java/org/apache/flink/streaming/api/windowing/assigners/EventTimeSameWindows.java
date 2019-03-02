/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeSameWindowTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.VoidWindow;

import java.util.Collection;
import java.util.Collections;

public class EventTimeSameWindows extends SameWindowAssigner<Object>{

	@Override
	public Collection<VoidWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		return Collections.singletonList(VoidWindow.INSTANCE);
	}

	@Override
	public Trigger<Object, VoidWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
		return EventTimeSameWindowTrigger.create();
	}

	@Override
	public String toString() {
		return "EventTimeSameWindows()";
	}

	/**
	 * Creates a new {@code EventTimeSameWindows} {@link WindowAssigner} that assigns
	 * elements to void window.
	 *
	 * @return The time policy.
	 */
	public static EventTimeSameWindows create() {
		return new EventTimeSameWindows();
	}

	@Override
	public TypeSerializer<VoidWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new VoidWindow.Serializer();
	}

	@Override
	public boolean isEventTime() {
		return true;
	}
}
