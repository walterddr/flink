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
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.slices.Slice;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link WindowOperator} that only allows non-overlapping window assigner.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class SliceOperator<K, IN, ACC, W extends Window>
		extends WindowOperator<K, IN, ACC, Slice<ACC, K, W>, W> {

	public SliceOperator(
		WindowAssigner<? super IN, W> windowAssigner,
		TypeSerializer<W> windowSerializer,
		KeySelector<IN, K> keySelector,
		TypeSerializer<K> keySerializer,
		StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor,
		InternalWindowFunction<ACC, ACC, K, W> windowFunction,
		Trigger<? super IN, ? super W> trigger,
		long allowedLateness,
		OutputTag<IN> lateDataOutputTag) {

		super(windowAssigner,
			windowSerializer,
			keySelector,
			keySerializer,
			windowStateDescriptor,
			wrapFunction(windowFunction),
			trigger,
			allowedLateness,
			lateDataOutputTag);
	}

	// Process Element needs to punch correct timestamp / watermark for downstream processing.

	private static <ACC, W extends Window, K> InternalWindowFunction<ACC, Slice<ACC, K, W>, K, W> wrapFunction(InternalWindowFunction<ACC, ACC, K, W> windowFunction) {
		return new InternalWindowFunction<ACC, Slice<ACC, K, W>, K, W>() {
			@Override
			public void process(K k, W window, InternalWindowContext context, ACC input, Collector<Slice<ACC, K, W>> out) throws Exception {

			}

			@Override
			public void clear(W window, InternalWindowContext context) throws Exception {

			}
		};
	}

	// ------------------------------------------------------------------------
    // Further optimization
	// 1. optimize processElement / emitWindowContent
	// 2. optimize trigger ruling
	// 3. additional API to convert based on slice assigner
	// ------------------------------------------------------------------------
}
