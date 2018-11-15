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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IterableSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializerSnapshot;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.datastream.OverSliceStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowOverSliceAssigner;
import org.apache.flink.streaming.api.windowing.slices.Slice;
import org.apache.flink.streaming.api.windowing.slices.SliceTypeInfo;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.fail;

/**
 * Tests for {@link WindowOperator}.
 */
@SuppressWarnings("serial")
public class WindowOperatorOverSliceTest extends TestLogger {

	private static final TypeInformation<Tuple2<String, Integer>> STRING_INT_TUPLE =
		TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){});

	// For counting if close() is called the correct number of times on the SumReducer
	private static AtomicInteger closeCalled = new AtomicInteger(0);

	// late arriving event OutputTag<StreamRecord<IN>>
	private static final OutputTag<Tuple2<String, Integer>> lateOutputTag = new OutputTag<Tuple2<String, Integer>>("late-output") {};

	private static <OUT> OneInputStreamOperatorTestHarness<Slice<Tuple2<String, Integer>, String, TimeWindow>, OUT> createTestHarness(OneInputStreamOperator<Slice<Tuple2<String, Integer>, String, TimeWindow>, OUT> operator) throws Exception {
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);
	}

	private void testEventTimeSlicing(OneInputStreamOperator<Slice<Tuple2<String, Integer>, String, TimeWindow>, Tuple2<String, Integer>> operator) throws Exception {
		OneInputStreamOperatorTestHarness<Slice<Tuple2<String, Integer>, String, TimeWindow>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Slice<>(new Tuple2<>("key2", 2), "key2", new TimeWindow(3000, 6000)), 5999));
		testHarness.processElement(new StreamRecord<>(new Slice<>(new Tuple2<>("key1", 3), "key1", new TimeWindow(0, 3000)), 2999));
		testHarness.processElement(new StreamRecord<>(new Slice<>(new Tuple2<>("key2", 3), "key2", new TimeWindow(0, 3000)), 2999));

		testHarness.processWatermark(new Watermark(999));
		expectedOutput.add(new Watermark(999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(1999));
		expectedOutput.add(new Watermark(1999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());
		testHarness.close();

		testHarness = createTestHarness(operator);
		expectedOutput.clear();
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processWatermark(new Watermark(2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3),2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3),2999));
		expectedOutput.add(new Watermark(2999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(3999));
		expectedOutput.add(new Watermark(3999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(new Watermark(4999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3),5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5),5999));
		expectedOutput.add(new Watermark(5999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		// those don't have any effect...
		testHarness.processWatermark(new Watermark(6999));
		testHarness.processWatermark(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));
		expectedOutput.add(new Watermark(7999));

		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	public void testEventTimeSlicingReduce() throws Exception {
		closeCalled.set(0);

		final int windowSize = 6;
		final int windowSlide = 3;

		SlidingOverTumbleWindowAssigner<Tuple2<String, Integer>> windowOverSliceAssigner =
			new SlidingOverTumbleWindowAssigner<>(windowSize * 1000L, windowSlide * 1000L, 0L);

		AggregatingStateDescriptor<Slice<Tuple2<String, Integer>, String, TimeWindow>, Tuple2<String, Integer>, Tuple2<String, Integer>> stateDesc =
			new AggregatingStateDescriptor<>("window-contents",
				OverSliceStream.wrapSliceFunction(new SumReducer()),
				STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Slice<Tuple2<String, Integer>, String, TimeWindow>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new WindowOperator<>(
				windowOverSliceAssigner,
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);
		testEventTimeSlicing(operator);
	}

	private static <OUT> OneInputStreamOperatorTestHarness<Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow>, OUT> createIterableTestHarness(OneInputStreamOperator<Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow>, OUT> operator) throws Exception {
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleIterableKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);
	}

	private void testEventTimeSlicingIterable(OneInputStreamOperator<Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow>, Tuple2<String, Integer>> operator) throws Exception {
		OneInputStreamOperatorTestHarness<Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow>, Tuple2<String, Integer>> testHarness =
			createIterableTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.setup();
		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Slice<>(Arrays.asList(
			new Tuple2<>("key2", 1), new Tuple2<>("key2", 1)), "key2", new TimeWindow(3000, 6000)), 5999));
		testHarness.processElement(new StreamRecord<>(new Slice<>(Arrays.asList(
			new Tuple2<>("key1", 1), new Tuple2<>("key1", 1), new Tuple2<>("key1", 1)), "key1", new TimeWindow(0, 3000)), 2999));
		testHarness.processElement(new StreamRecord<>(new Slice<>(Arrays.asList(
			new Tuple2<>("key2", 1), new Tuple2<>("key2", 1), new Tuple2<>("key2", 1)), "key2", new TimeWindow(0, 3000)), 2999));

		testHarness.processWatermark(new Watermark(999));
		expectedOutput.add(new Watermark(999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(1999));
		expectedOutput.add(new Watermark(1999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());
		testHarness.close();

		testHarness = createIterableTestHarness(operator);
		expectedOutput.clear();
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processWatermark(new Watermark(2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3),2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3),2999));
		expectedOutput.add(new Watermark(2999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(3999));
		expectedOutput.add(new Watermark(3999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(new Watermark(4999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3),5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5),5999));
		expectedOutput.add(new Watermark(5999));
		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		// those don't have any effect...
		testHarness.processWatermark(new Watermark(6999));
		testHarness.processWatermark(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));
		expectedOutput.add(new Watermark(7999));

		TestHarnessUtil.assertObjectOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	public void testEventTimeSlicingIterable() throws Exception {
		closeCalled.set(0);

		final int windowSize = 6;
		final int windowSlide = 3;

		SlidingOverTumbleWindowAssigner<Iterable<Tuple2<String, Integer>>> windowOverSliceAssigner =
			new SlidingOverTumbleWindowAssigner<>(windowSize * 1000L, windowSlide * 1000L, 0L);

		TypeInformation<Iterable<Tuple2<String, Integer>>> iterableElementType =
			new IterableElementTypeInfo<>(STRING_INT_TUPLE);

		SliceTypeInfo<Iterable<Tuple2<String, Integer>>, String, TimeWindow> sliceTypeInfo = new SliceTypeInfo<>(
			iterableElementType,
			BasicTypeInfo.STRING_TYPE_INFO,
			windowOverSliceAssigner.getSliceType(),
			new TimeWindow.Serializer()
		);

		ListStateDescriptor<Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow>> stateDesc = new ListStateDescriptor<>("window-contents",
			sliceTypeInfo.createSerializer(new ExecutionConfig()));

		WindowOperator<String,
			Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow>,
			Iterable<Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow>>,
			Tuple2<String, Integer>, TimeWindow> operator =
			new WindowOperator<>(
				windowOverSliceAssigner,
				new TimeWindow.Serializer(),
				new TupleIterableKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(OverSliceStream.wrapSliceElementFunction(new RichSumReducer<TimeWindow>())),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);
		testEventTimeSlicingIterable(operator);
	}

	@SuppressWarnings("unchecked")
	private static class Tuple2ResultSortComparator implements Comparator<Object>, Serializable {
		@Override
		public int compare(Object o1, Object o2) {
			if (o1 instanceof Watermark || o2 instanceof Watermark) {
				return 0;
			} else {
				StreamRecord<Tuple2<String, Integer>> sr0 = (StreamRecord<Tuple2<String, Integer>>) o1;
				StreamRecord<Tuple2<String, Integer>> sr1 = (StreamRecord<Tuple2<String, Integer>>) o2;
				if (sr0.getTimestamp() != sr1.getTimestamp()) {
					return (int) (sr0.getTimestamp() - sr1.getTimestamp());
				}
				int comparison = sr0.getValue().f0.compareTo(sr1.getValue().f0);
				if (comparison != 0) {
					return comparison;
				} else {
					return sr0.getValue().f1 - sr1.getValue().f1;
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	//  UDFs
	// ------------------------------------------------------------------------

	private static class SumReducer implements ReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
											  Tuple2<String, Integer> value2) throws Exception {
			return new Tuple2<>(value2.f0, value1.f1 + value2.f1);
		}
	}
	private static class RichSumReducer<W extends Window> extends RichWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, W> {
		private static final long serialVersionUID = 1L;

		private boolean openCalled = false;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			openCalled = true;
		}

		@Override
		public void close() throws Exception {
			super.close();
			closeCalled.incrementAndGet();
		}

		@Override
		public void apply(String key,
						  W window,
						  Iterable<Tuple2<String, Integer>> input,
						  Collector<Tuple2<String, Integer>> out) throws Exception {

			if (!openCalled) {
				fail("Open was not called");
			}
			int sum = 0;

			for (Tuple2<String, Integer> t: input) {
				sum += t.f1;
			}
			out.collect(new Tuple2<>(key, sum));
		}
	}

	private static class TupleKeySelector implements KeySelector<Slice<Tuple2<String, Integer>, String, TimeWindow>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Slice<Tuple2<String, Integer>, String, TimeWindow> value) throws Exception {
			return value.getKey();
		}
	}

	private static class TupleIterableKeySelector implements KeySelector<Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Slice<Iterable<Tuple2<String, Integer>>, String, TimeWindow> value) throws Exception {
			return value.getKey();
		}
	}

	private class SlidingOverTumbleWindowAssigner<T> extends WindowOverSliceAssigner<T, String, TimeWindow> {

		private long size;
		private long slide;
		private long offset;

		protected SlidingOverTumbleWindowAssigner(long size, long slide, long offset) {
			this.size = size;
			this.slide = slide;
			this.offset = offset;
		}

		@Override
		public Collection<TimeWindow> assignWindowsFromSlice(Slice<T, String, TimeWindow> element, long timestamp, WindowAssignerContext context) {
			TimeWindow slice = element.getWindow();
			if (slice.getEnd() > Long.MIN_VALUE) {
				List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
				long lastStart = TimeWindow.getWindowStartWithOffset(
					slice.getStart(), offset, slide);
				for (long start = lastStart;
					 start >= slice.getEnd() - size;
					 start -= slide) {
					windows.add(new TimeWindow(start, start + size));
				}
				return windows;
			} else {
				throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
					"Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
					"'DataStream.assignTimestampsAndWatermarks(...)'?");
			}
		}

		@Override
		protected Collection<TimeWindow> assignWindowsFromTimestamp(long timestamp, WindowAssignerContext context) {
			throw new RuntimeException("This window over slice assigner cannot assign from timestamp only!");
		}

		public long getSize() {
			return size;
		}

		public long getSlide() {
			return slide;
		}

		@Override
		public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
			return EventTimeTrigger.create();
		}

		@Override
		public String toString() {
			return "SlidingEventTimeWindows(" + size + ", " + slide + ")";
		}

		@Override
		public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
			return new TimeWindow.Serializer();
		}

		@Override
		public boolean isEventTime() {
			return true;
		}

		@Override
		public TypeInformation<TimeWindow> getSliceType() {
			return TypeInformation.of(TimeWindow.class);
		}
	}

	private class IterableElementTypeInfo<T> extends TypeInformation<Iterable<T>> {

		private static final long serialVersionUID = 1L;

		private final TypeInformation<T> elementTypeInfo;


		public IterableElementTypeInfo(Class<T> elementTypeClass) {
			this.elementTypeInfo = of(checkNotNull(elementTypeClass, "elementTypeClass"));
		}

		public IterableElementTypeInfo(TypeInformation<T> elementTypeInfo) {
			this.elementTypeInfo = checkNotNull(elementTypeInfo, "elementTypeInfo");
		}

		// ------------------------------------------------------------------------
		//  ListTypeInfo specific properties
		// ------------------------------------------------------------------------

		/**
		 * Gets the type information for the elements contained in the list
		 */
		public TypeInformation<T> getElementTypeInfo() {
			return elementTypeInfo;
		}

		// ------------------------------------------------------------------------
		//  TypeInformation implementation
		// ------------------------------------------------------------------------

		@Override
		public boolean isBasicType() {
			return false;
		}

		@Override
		public boolean isTupleType() {
			return false;
		}

		@Override
		public int getArity() {
			return 0;
		}

		@Override
		public int getTotalFields() {
			// similar as arrays, the lists are "opaque" to the direct field addressing logic
			// since the list's elements are not addressable, we do not expose them
			return 1;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Class<Iterable<T>> getTypeClass() {
			return (Class<Iterable<T>>)(Class<?>)List.class;
		}

		@Override
		public boolean isKeyType() {
			return false;
		}

		@Override
		public TypeSerializer<Iterable<T>> createSerializer(ExecutionConfig config) {
			TypeSerializer<T> elementTypeSerializer = elementTypeInfo.createSerializer(config);
			return new IterableSerializer<>(elementTypeSerializer);
		}

		// ------------------------------------------------------------------------

		@Override
		public String toString() {
			return "List<" + elementTypeInfo + '>';
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}
			else if (obj instanceof ListTypeInfo) {
				final IterableElementTypeInfo<?> other = (IterableElementTypeInfo<?>) obj;
				return other.canEqual(this) && elementTypeInfo.equals(other.elementTypeInfo);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return 31 * elementTypeInfo.hashCode() + 1;
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj != null && obj.getClass() == getClass();
		}
	}
}
