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
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
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
 * @param <ACC> The type of elements emitted by the {@code AppendingState}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class SliceOperator<K, IN, ACC, W extends Window>
		extends WindowOperator<K, IN, ACC, Slice<ACC, K, W>, W> {


	private static final long serialVersionUID = 1L;

	private final StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor;

	// ------------------------------------------------------------------------
	// State that is not checkpointed
	// ------------------------------------------------------------------------

	/** The state in which the window contents is stored. Each window is a namespace */
	private transient InternalAppendingState<K, W, IN, ACC, ACC> windowState;

	/**
	 * The {@link #windowState}, typed to merging state for merging windows.
	 * Null if the window state is not mergeable.
	 */
	private transient InternalMergingState<K, W, IN, ACC, ACC> windowMergingState;

	/** The state that holds the merging window metadata (the sets that describe what is merged). */
	private transient InternalListState<K, VoidNamespace, Tuple2<W, W>> mergingSetsState;

	/**
	 * This is given to the {@code InternalWindowFunction} for emitting elements with a given
	 * timestamp.
	 */
	private transient TimestampedCollector<Slice<ACC, K, W>> timestampedCollector;

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
			null,
			new InternalSingleValueSliceWindowFunction<>(windowFunction),
			trigger,
			allowedLateness,
			lateDataOutputTag);

		this.windowStateDescriptor = checkNotNull(windowStateDescriptor);
	}

	@Override
	public void open() throws Exception {
		super.open();
		timestampedCollector = new TimestampedCollector<>(output);

		// create (or restore) the state that hold the actual window contents
		// NOTE - the state may be null in the case of the overriding evicting window operator
		if (windowStateDescriptor != null) {
			windowState = (InternalAppendingState<K, W, IN, ACC, ACC>) getOrCreateKeyedState(windowSerializer, windowStateDescriptor);
		}

		// create the typed and helper states for merging windows
		if (windowAssigner instanceof MergingWindowAssigner) {

			// store a typed reference for the state of merging windows - sanity check
			if (windowState instanceof InternalMergingState) {
				windowMergingState = (InternalMergingState<K, W, IN, ACC, ACC>) windowState;
			}
			// TODO this sanity check should be here, but is prevented by an incorrect test (pending validation)
			// TODO see WindowOperatorTest.testCleanupTimerWithEmptyFoldingStateForSessionWindows()
			// TODO activate the sanity check once resolved
//			else if (windowState != null) {
//				throw new IllegalStateException(
//						"The window uses a merging assigner, but the window state is not mergeable.");
//			}

			@SuppressWarnings("unchecked")
			final Class<Tuple2<W, W>> typedTuple = (Class<Tuple2<W, W>>) (Class<?>) Tuple2.class;

			final TupleSerializer<Tuple2<W, W>> tupleSerializer = new TupleSerializer<>(
				typedTuple,
				new TypeSerializer[] {windowSerializer, windowSerializer});

			final ListStateDescriptor<Tuple2<W, W>> mergingSetsStateDescriptor =
				new ListStateDescriptor<>("merging-window-set", tupleSerializer);

			// get the state that stores the merging sets
			mergingSetsState = (InternalListState<K, VoidNamespace, Tuple2<W, W>>)
				getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, mergingSetsStateDescriptor);
			mergingSetsState.setCurrentNamespace(VoidNamespace.INSTANCE);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		timestampedCollector = null;
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		timestampedCollector = null;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		final Collection<W> elementWindows = windowAssigner.assignWindows(
			element.getValue(), element.getTimestamp(), windowAssignerContext);

		//if element is handled by none of assigned elementWindows
		boolean isSkippedElement = true;

		final K key = this.<K>getKeyedStateBackend().getCurrentKey();

		if (windowAssigner instanceof MergingWindowAssigner) {
			MergingWindowSet<W> mergingWindows = getMergingWindowSet();

			for (W window: elementWindows) {

				// adding the new window might result in a merge, in that case the actualWindow
				// is the merged window and we work with that. If we don't merge then
				// actualWindow == window
				W actualWindow = mergingWindows.addWindow(window, new MergingWindowSet.MergeFunction<W>() {
					@Override
					public void merge(W mergeResult,
									  Collection<W> mergedWindows, W stateWindowResult,
									  Collection<W> mergedStateWindows) throws Exception {

						if ((windowAssigner.isEventTime() && mergeResult.maxTimestamp() + allowedLateness <= internalTimerService.currentWatermark())) {
							throw new UnsupportedOperationException("The end timestamp of an " +
								"event-time window cannot become earlier than the current watermark " +
								"by merging. Current watermark: " + internalTimerService.currentWatermark() +
								" window: " + mergeResult);
						} else if (!windowAssigner.isEventTime() && mergeResult.maxTimestamp() <= internalTimerService.currentProcessingTime()) {
							throw new UnsupportedOperationException("The end timestamp of a " +
								"processing-time window cannot become earlier than the current processing time " +
								"by merging. Current processing time: " + internalTimerService.currentProcessingTime() +
								" window: " + mergeResult);
						}

						triggerContext.key = key;
						triggerContext.window = mergeResult;

						triggerContext.onMerge(mergedWindows);

						for (W m: mergedWindows) {
							triggerContext.window = m;
							triggerContext.clear();
							deleteCleanupTimer(m);
						}

						// merge the merged state windows into the newly resulting state window
						windowMergingState.mergeNamespaces(stateWindowResult, mergedStateWindows);
					}
				});

				// drop if the window is already late
				if (isWindowLate(actualWindow)) {
					mergingWindows.retireWindow(actualWindow);
					continue;
				}
				isSkippedElement = false;

				W stateWindow = mergingWindows.getStateWindow(actualWindow);
				if (stateWindow == null) {
					throw new IllegalStateException("Window " + window + " is not in in-flight window set.");
				}

				windowState.setCurrentNamespace(stateWindow);
				windowState.add(element.getValue());

				triggerContext.key = key;
				triggerContext.window = actualWindow;

				TriggerResult triggerResult = triggerContext.onElement(element);

				if (triggerResult.isFire()) {
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					emitWindowContents(actualWindow, contents);
				}

				if (triggerResult.isPurge()) {
					windowState.clear();
				}
				registerCleanupTimer(actualWindow);
			}

			// need to make sure to update the merging state in state
			mergingWindows.persist();
		} else {
			for (W window: elementWindows) {

				// drop if the window is already late
				if (isWindowLate(window)) {
					continue;
				}
				isSkippedElement = false;

				windowState.setCurrentNamespace(window);
				windowState.add(element.getValue());

				triggerContext.key = key;
				triggerContext.window = window;

				TriggerResult triggerResult = triggerContext.onElement(element);

				if (triggerResult.isFire()) {
					ACC contents = windowState.get();
					if (contents == null) {
						continue;
					}
					emitWindowContents(window, contents);
				}

				if (triggerResult.isPurge()) {
					windowState.clear();
				}
				registerCleanupTimer(window);
			}
		}

		// side output input event if
		// element not handled by any window
		// late arriving tag has been set
		// windowAssigner is event time and current timestamp + allowed lateness no less than element timestamp
		if (isSkippedElement && isElementLate(element)) {
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

		MergingWindowSet<W> mergingWindows;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
			if (stateWindow == null) {
				// Timer firing for non-existent window, this can only happen if a
				// trigger did not clean up timers. We have already cleared the merging
				// window and therefore the Trigger state, however, so nothing to do.
				return;
			} else {
				windowState.setCurrentNamespace(stateWindow);
			}
		} else {
			windowState.setCurrentNamespace(triggerContext.window);
			mergingWindows = null;
		}

		TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

		if (triggerResult.isFire()) {
			ACC contents = windowState.get();
			if (contents != null) {
				emitWindowContents(triggerContext.window, contents);
			}
		}

		if (triggerResult.isPurge()) {
			windowState.clear();
		}

		if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
			clearAllState(triggerContext.window, windowState, mergingWindows);
		}

		if (mergingWindows != null) {
			// need to make sure to update the merging state in state
			mergingWindows.persist();
		}
	}

	@Override
	public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
		triggerContext.key = timer.getKey();
		triggerContext.window = timer.getNamespace();

		MergingWindowSet<W> mergingWindows;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
			if (stateWindow == null) {
				// Timer firing for non-existent window, this can only happen if a
				// trigger did not clean up timers. We have already cleared the merging
				// window and therefore the Trigger state, however, so nothing to do.
				return;
			} else {
				windowState.setCurrentNamespace(stateWindow);
			}
		} else {
			windowState.setCurrentNamespace(triggerContext.window);
			mergingWindows = null;
		}

		TriggerResult triggerResult = triggerContext.onProcessingTime(timer.getTimestamp());

		if (triggerResult.isFire()) {
			ACC contents = windowState.get();
			if (contents != null) {
				emitWindowContents(triggerContext.window, contents);
			}
		}

		if (triggerResult.isPurge()) {
			windowState.clear();
		}

		if (!windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
			clearAllState(triggerContext.window, windowState, mergingWindows);
		}

		if (mergingWindows != null) {
			// need to make sure to update the merging state in state
			mergingWindows.persist();
		}
	}

	/**
	 * Drops all state for the given window and calls
	 * {@link Trigger#clear(Window, Trigger.TriggerContext)}.
	 *
	 * <p>The caller must ensure that the
	 * correct key is set in the state backend and the triggerContext object.
	 */
	private void clearAllState(
		W window,
		AppendingState<IN, ACC> windowState,
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
	 * Emits the contents of the given window using the {@link InternalWindowFunction}.
	 */
	@SuppressWarnings("unchecked")
	private void emitWindowContents(W window, ACC contents) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp()); // should be set to maximum element timestamp actually, but this also works.
		processContext.window = window;
		userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);
	}

	private static class InternalSingleValueSliceWindowFunction<ACC, K, W extends Window>
		implements InternalWindowFunction<ACC, Slice<ACC,K,W>,K,W> {

		private final InternalWindowFunction<ACC, ACC, K, W> internalWindowFunction;

		private SingleValueCollector collector;

		private class SingleValueCollector implements Collector<ACC> {
			private ACC record;

			@Override
			public void collect(ACC record) {
				this.record = record;
			}

			@Override
			public void close() {
			}
		}

		public InternalSingleValueSliceWindowFunction(InternalWindowFunction<ACC, ACC, K, W> internalWindowFunction) {
			this.collector = new SingleValueCollector();
			this.internalWindowFunction = internalWindowFunction;
		}

		@Override
		public void process(K k, W window, InternalWindowContext context, ACC input, Collector<Slice<ACC, K, W>> out) throws Exception {
			internalWindowFunction.process(k, window, context, input, collector);
			out.collect(new Slice<>(collector.record, k, window));
		}

		@Override
		public void clear(W window, InternalWindowContext context) throws Exception {

		}
	}

	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@Override
	@VisibleForTesting
	@SuppressWarnings("unchecked, rawtypes")
	public StateDescriptor<? extends AppendingState<IN, ACC>, ?> getStateDescriptor() {
		return (StateDescriptor<? extends AppendingState<IN, ACC>, ?>) windowStateDescriptor;
	}

	// ------------------------------------------------------------------------
    // Further optimization
	// 1. optimize processElement / emitWindowContent
	// 2. optimize trigger ruling
	// 3. additional API to convert based on slice assigner
	// ------------------------------------------------------------------------
}
