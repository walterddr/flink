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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.internal.InternalAppendingState;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowOverSliceAssigner;
import org.apache.flink.streaming.api.windowing.slices.Slice;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.Collection;

/**
 * A {@link WindowOperator} that only allows non-overlapping window assigner.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class WindowOverSliceOperator<K, IN, ACC, OUT, W extends Window>
		extends WindowOperator<K, Slice<IN, K, W>, ACC, OUT, W> {
	private static final long serialVersionUID = 1L;

	public WindowOverSliceOperator(
		WindowOverSliceAssigner<IN, K, W> windowAssigner,
		TypeSerializer<W> windowSerializer,
		KeySelector<Slice<IN, K, W>, K> keySelector,
		TypeSerializer<K> keySerializer,
		StateDescriptor<? extends AppendingState<Slice<IN, K, W>, ACC>, ?> windowStateDescriptor,
		InternalWindowFunction<ACC, OUT, K, W> windowFunction,
		Trigger<? super Slice<IN, K, W>, ? super W> trigger,
		long allowedLateness,
		OutputTag<Slice<IN, K, W>> lateDataOutputTag) {

		super(windowAssigner,
			windowSerializer,
			keySelector,
			keySerializer,
			windowStateDescriptor,
			windowFunction,
			trigger,
			allowedLateness,
			lateDataOutputTag);
	}

	// ------------------------------------------------------------------------
	// Further optimization
	// 1. optimize processElement / emitWindowContent
	// 2. optimize trigger ruling
	// 3. additional API to convert based on slice results
	// ------------------------------------------------------------------------
}
