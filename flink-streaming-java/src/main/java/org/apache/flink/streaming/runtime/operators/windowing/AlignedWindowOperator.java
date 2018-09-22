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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalAppendingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.windowing.assigners.AlignedWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An operator that implements the logic for windowing based on a {@link AlignedWindowAssigner}
 * utilizing the basic {@link WindowOperator}
 *
 * <p>In addition to getting assigned a key using a {@link KeySelector} and zero or more windows
 * using a {@link WindowAssigner}, it also use {@link AlignedWindowAssigner} to assign zero or one
 * unique pane slice associated with the particular element. A pane is the bucket of elements that
 * have the same key and same {@code Window}. Pane slices are non-overlapping pieces of windows.
 * One particular element can only belong to zero or one pane slice.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class AlignedWindowOperator<K, IN, ACC, OUT, W extends Window>
	extends WindowOperator<K, IN, ACC, OUT, W> {

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------
	// these fields are set by the API stream graph builder to configure the operator

	private final AlignedWindowAssigner<? super IN, W> alignedWindowAssigner;

	private final StateDescriptor<? extends AppendingState<IN, ACC>, ?> sliceStateDescriptor;

	private final MergeFunction<ACC> sliceStateMergingFunction;

	private final StateDescriptor<? extends ListState<W>, ?> windowStateDescriptor;

	// ------------------------------------------------------------------------
	// the fields below are instantiated once the operator runs in the runtime

	private transient InternalMergingState<K, W, IN, ACC, ACC> sliceState;

	private transient InternalListState<K, W, W> windowState;

	// ------------------------------------------------------------------------

	public AlignedWindowOperator(AlignedWindowAssigner<? super IN, W> windowAssigner,
								 TypeSerializer<W> windowSerializer,
								 KeySelector<IN, K> keySelector,
								 TypeSerializer<K> keySerializer,
								 StateDescriptor<? extends ListState<W>, ?> windowStateDescriptor,
								 StateDescriptor<? extends AppendingState<IN, ACC>, ?> sliceStateDescriptor,
								 InternalWindowFunction<ACC, OUT, K, W> windowFunction,
								 MergeFunction<ACC> sliceStateMergingFunction,
								 Trigger<? super IN, ? super W> trigger,
								 long allowedLateness,
								 OutputTag<IN> lateDataOutputTag) {

		super(windowAssigner, windowSerializer, keySelector,
			keySerializer, null, windowFunction, trigger, allowedLateness, lateDataOutputTag);

		this.alignedWindowAssigner = checkNotNull(windowAssigner);
		this.sliceStateDescriptor = checkNotNull(sliceStateDescriptor);
		this.windowStateDescriptor = checkNotNull(windowStateDescriptor);
		this.sliceStateMergingFunction = checkNotNull(sliceStateMergingFunction);
	}

	@Override
	public void open() throws Exception {
		super.open();

		sliceState = (InternalMergingState<K, W, IN, ACC, ACC>)
			getOrCreateKeyedState(windowSerializer, sliceStateDescriptor);
		windowState = (InternalListState<K, W, W>)
			getOrCreateKeyedState(windowSerializer, windowStateDescriptor);
	}

	@Override
	public void close() throws Exception {
		super.close();
		// evictorContext = null;
	}

	@Override
	public void dispose() throws Exception{
		super.dispose();
		// evictorContext = null;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		final Collection<W> elementWindows = alignedWindowAssigner.assignWindows(
				element.getValue(), element.getTimestamp(), windowAssignerContext);

		final Collection<W> elementSlices = alignedWindowAssigner.assignSlices(
			element.getValue(), element.getTimestamp(), windowAssignerContext);

		//if element belongs to none of the window slices
		boolean isNoneAssignedElement = true;

		//if element is handled by none of assigned elementWindows
		boolean isSkippedElement = true;

		final K key = this.<K>getKeyedStateBackend().getCurrentKey();

		// check if the slice assignment had failed
		if (elementSlices.size() == 1) {
			isNoneAssignedElement = false;
			// Process Element - Slice Relationship
			W slice = elementSlices.iterator().next();

			sliceState.setCurrentNamespace(slice);
			ACC previousSliceState = sliceState.get();
			sliceState.add(element.getValue());

			// Process Slice - Window Relationship
			for (W window : elementWindows) {

				// check if the window is already inactive
				if (isWindowLate(window)) {
					continue;
				}
				isSkippedElement = false;

				windowState.setCurrentNamespace(window);
				// append slice only if it was newly created.
				if (previousSliceState == null) {
					windowState.add(slice);
				}

				triggerContext.key = key;
				triggerContext.window = window;

				TriggerResult triggerResult = triggerContext.onElement(element);

				if (triggerResult.isFire()) {
					Iterable<W> contents = windowState.get();
					if (contents == null) {
						// if we have no state, there is nothing to do
						continue;
					}
					emitWindowContents(window, contents, sliceState);
				}

				if (triggerResult.isPurge()) {
					Iterable<W> slices = windowState.get();
					for (W sliceElement: slices) {
						sliceState.setCurrentNamespace(sliceElement);
						sliceState.clear();
					}
					windowState.clear();
					// TODO: Need a way to purge the slice that no longer maps to any window
					// for now just purge all slices within the window
				}
				registerCleanupTimer(window);
			}
			// TODO: register slide cleanup timer
		}

		// side output input event if
		// element not handled by any window
		// late arriving tag has been set
		// windowAssigner is event time and current timestamp + allowed lateness no less than element timestamp
		if ((isSkippedElement || isNoneAssignedElement) && isElementLate(element)) {
			if (lateDataOutputTag != null){
				sideOutput(element);
			} else {
				this.numLateRecordsDropped.inc();
			}
		}
	}

	@Override
	public void onEventTime(InternalTimer<K, W> timer) throws Exception {

		triggerContext.key = timer.getKey();
		triggerContext.window = timer.getNamespace();
//		evictorContext.key = timer.getKey();
//		evictorContext.window = timer.getNamespace();

		MergingWindowSet<W> mergingWindows = null;

		windowState.setCurrentNamespace(triggerContext.window);

		TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

		if (triggerResult.isFire()) {
			Iterable<W> contents = windowState.get();
			if (contents != null) {
				emitWindowContents(triggerContext.window, contents, sliceState);
			}
		}

		if (triggerResult.isPurge()) {
			Iterable<W> slices = windowState.get();
			for (W sliceElement: slices) {
				sliceState.setCurrentNamespace(sliceElement);
				sliceState.clear();
			}
			windowState.clear();
			// TODO: Need a way to purge the slice that no longer maps to any window
			// for now just purge all slices within the window
		}

		if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
			clearAllState(triggerContext.window, windowState, mergingWindows);
			// TODO slice needs to be clear too.
		}

//		if (mergingWindows != null) {
//			// need to make sure to update the merging state in state
//			mergingWindows.persist();
//		}
	}

	@Override
	public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
		triggerContext.key = timer.getKey();
		triggerContext.window = timer.getNamespace();
//		evictorContext.key = timer.getKey();
//		evictorContext.window = timer.getNamespace();

		MergingWindowSet<W> mergingWindows = null;

		windowState.setCurrentNamespace(triggerContext.window);

		TriggerResult triggerResult = triggerContext.onProcessingTime(timer.getTimestamp());

		if (triggerResult.isFire()) {
			Iterable<W> contents = windowState.get();
			if (contents != null) {
				emitWindowContents(triggerContext.window, contents, sliceState);
			}
		}

		if (triggerResult.isPurge()) {
			Iterable<W> slices = windowState.get();
			for (W sliceElement: slices) {
				sliceState.setCurrentNamespace(sliceElement);
				sliceState.clear();
			}
			windowState.clear();
			// TODO: Need a way to purge the slice that no longer maps to any window
			// for now just purge all slices within the window
		}

		if (!windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
			clearAllState(triggerContext.window, windowState, mergingWindows);
		}

		if (mergingWindows != null) {
			// need to make sure to update the merging state in state
			mergingWindows.persist();
		}
	}

	private void emitWindowContents(
		W window,
		Iterable<W> contents,
		InternalAppendingState<K, W, IN, ACC, ACC> sliceState) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());

		// Pre-process and merge sliceStates
		// TODO this is problematic since we are directly accessing AggregateFunction
		// Some type of wrapping is needed here.
		ACC mergedContents = null;
		for (W slice: contents) {
			sliceState.setCurrentNamespace(slice);
			ACC sliceContent = sliceState.get();
			if (mergedContents == null) {
				mergedContents = sliceContent;
			} else {
				mergedContents = sliceStateMergingFunction.merge(mergedContents, sliceContent);
			}
		}

		if (mergedContents != null) {
			processContext.window = triggerContext.window;
			userFunction.process(triggerContext.key, triggerContext.window, processContext, mergedContents, timestampedCollector);
		}

	}

	private void clearAllState(
			W window,
			ListState<W> windowState,
			MergingWindowSet<W> mergingWindows) throws Exception {
		windowState.clear();
		triggerContext.clear();
		processContext.window = window;
		processContext.clear();
		if (mergingWindows != null) {
			mergingWindows.retireWindow(window);
			mergingWindows.persist();
		}
	}

	/**
	 * Define merge method for merging slice states
	 * @param <ACC> slice state type
	 */
	public interface MergeFunction<ACC> {

		/**
		 * trying to merge two intermediate slice results
		 * @param a left element
		 * @param b right element
		 * @return merged result
		 * @throws Exception when merge fails
		 */
		ACC merge(ACC a, ACC b) throws Exception;
	}

//	/**
//	 * {@code EvictorContext} is a utility for handling {@code Evictor} invocations. It can be reused
//	 * by setting the {@code key} and {@code window} fields. No internal state must be kept in
//	 * the {@code EvictorContext}.
//	 */
//
//	class EvictorContext implements Evictor.EvictorContext {
//
//		protected K key;
//		protected W window;
//
//		public EvictorContext(K key, W window) {
//			this.key = key;
//			this.window = window;
//		}
//
//		@Override
//		public long getCurrentProcessingTime() {
//			return internalTimerService.currentProcessingTime();
//		}
//
//		@Override
//		public long getCurrentWatermark() {
//			return internalTimerService.currentWatermark();
//		}
//
//		@Override
//		public MetricGroup getMetricGroup() {
//			return AlignedWindowOperator.this.getMetricGroup();
//		}
//
//		public K getKey() {
//			return key;
//		}
//
//		void evictBefore(Iterable<TimestampedValue<IN>> elements, int size) {
//			evictor.evictBefore((Iterable) elements, size, window, this);
//		}
//
//		void evictAfter(Iterable<TimestampedValue<IN>>  elements, int size) {
//			evictor.evictAfter((Iterable) elements, size, window, this);
//		}
//	}

	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@Override
	@VisibleForTesting
	@SuppressWarnings("unchecked, rawtypes")
	public StateDescriptor<? extends AppendingState<IN, ACC>, ?> getStateDescriptor() {
		return (StateDescriptor<? extends AppendingState<IN, ACC>, ?>) sliceStateDescriptor;
	}

	@VisibleForTesting
	@SuppressWarnings("unchecked, rawtypes")
	public StateDescriptor<? extends ListState<W>, ?> getWindowStateDescriptor() {
		return (StateDescriptor<? extends ListState<W>, ?>) windowStateDescriptor;
	}
}
