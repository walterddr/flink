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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.AlignedSlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.AlignedSlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
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
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link WindowOperator}.
 */
@SuppressWarnings("serial")
public class AlignedWindowOperatorTest extends TestLogger {

	private static final TypeInformation<Tuple2<String, Integer>> STRING_INT_TUPLE =
			TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){});

	// For counting if close() is called the correct number of times on the SumReducer
	private static AtomicInteger closeCalled = new AtomicInteger(0);

	// late arriving event OutputTag<StreamRecord<IN>>
	private static final OutputTag<Tuple2<String, Integer>> lateOutputTag = new OutputTag<Tuple2<String, Integer>>("late-output") {};

	private void testSlidingEventTimeWindows(OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator) throws Exception {

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);
		testHarness.setup();
		testHarness.open();

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3000));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 0));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));

		testHarness.processWatermark(new Watermark(999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 999));
		expectedOutput.add(new Watermark(999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(1999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 1999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 1999));
		expectedOutput.add(new Watermark(1999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 2999));
		expectedOutput.add(new Watermark(2999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		// do a snapshot, close and restore again
		OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();

		expectedOutput.clear();
		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processWatermark(new Watermark(3999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5), 3999));
		expectedOutput.add(new Watermark(3999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 4999));
		expectedOutput.add(new Watermark(4999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 5999));
		expectedOutput.add(new Watermark(5999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		// those don't have any effect...
		testHarness.processWatermark(new Watermark(6999));
		testHarness.processWatermark(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));
		expectedOutput.add(new Watermark(7999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	public void testAlignedSlidingEventTimeWindowsAggregate() throws Exception {
		closeCalled.set(0);

		final int windowSize = 3;
		final int windowSlide = 1;

		ListStateDescriptor<TimeWindow> windowStateDesc =
			new ListStateDescriptor<>("window-slice-mappings", new TimeWindow.Serializer());

		AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> aggFunction = new LengthSumAggregate();
		AggregatingStateDescriptor<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>
			sliceStateDesc = new AggregatingStateDescriptor<>("slice-contents",
			aggFunction,
			STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		AlignedWindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new AlignedWindowOperator<>(
				AlignedSlidingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS), Time.of(windowSlide, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				windowStateDesc,
				sliceStateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				aggFunction::merge,
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		testSlidingEventTimeWindows(operator);
	}

	@Test
	public void testAlignedSlidingEventTimeWindowsReduce() throws Exception {
		closeCalled.set(0);

		final int windowSize = 3;
		final int windowSlide = 1;

		ReduceFunction<Tuple2<String, Integer>> sumReducer = new SumReducer();
		ListStateDescriptor<TimeWindow> windowStateDesc =
			new ListStateDescriptor<>("window-slice-mappings", new TimeWindow.Serializer());

		ReducingStateDescriptor<Tuple2<String, Integer>> sliceStateDesc = new ReducingStateDescriptor<>("slice-contents",
			sumReducer,
			STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		AlignedWindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new AlignedWindowOperator<>(
				AlignedSlidingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS), Time.of(windowSlide, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				windowStateDesc,
				sliceStateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				sumReducer::reduce,
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		testSlidingEventTimeWindows(operator);
	}

	@Test
	public void testSlidingEventTimeWindowsApply() throws Exception {
		closeCalled.set(0);

		final int windowSize = 3;
		final int windowSlide = 1;

		ListStateDescriptor<TimeWindow> windowStateDesc =
			new ListStateDescriptor<>("window-slice-mappings", new TimeWindow.Serializer());

		ListStateDescriptor<Tuple2<String, Integer>> sliceStateDesc = new ListStateDescriptor<>("slice-contents",
				STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		RichWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> timeWindowRichSumReducer = new RichSumReducer<>();

		AlignedWindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, TimeWindow> operator =
			new AlignedWindowOperator<>(
				AlignedSlidingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS), Time.of(windowSlide, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				windowStateDesc,
				sliceStateDesc,
				new InternalIterableWindowFunction<>(timeWindowRichSumReducer),
				(a, b) -> {
					List<Tuple2<String, Integer>> mergeList = new ArrayList<>();
					for (Tuple2<String, Integer> i : a) {
						mergeList.add(i);
					}
					for (Tuple2<String, Integer> i : b) {
						mergeList.add(i);
					}
					return mergeList;
				},
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		testSlidingEventTimeWindows(operator);

		// we close once in the rest...
		Assert.assertEquals("Close was not called.", 2, closeCalled.get());
	}

	@Test
	public void testProcessingTimeSlidingWindows() throws Throwable {
		final int windowSize = 3;
		final int windowSlide = 1;


		ReduceFunction<Tuple2<String, Integer>> sumReducer = new SumReducer();
		ListStateDescriptor<TimeWindow> windowStateDesc =
			new ListStateDescriptor<>("window-slice-mappings", new TimeWindow.Serializer());

		ReducingStateDescriptor<Tuple2<String, Integer>> sliceStateDesc = new ReducingStateDescriptor<>("slice-contents",
			sumReducer,
			STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		AlignedWindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new AlignedWindowOperator<>(
				AlignedSlidingProcessingTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS), Time.of(windowSlide, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				windowStateDesc,
				sliceStateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				sumReducer::reduce,
				ProcessingTimeTrigger.create(),
				0,
				null /* late data output tag */);


		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// timestamp is ignored in processing time
		testHarness.setProcessingTime(3);
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), Long.MAX_VALUE));

		testHarness.setProcessingTime(1000);

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 1), 999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), Long.MAX_VALUE));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), Long.MAX_VALUE));

		testHarness.setProcessingTime(2000);

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 1999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));

		testHarness.setProcessingTime(3000);

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 2), 2999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));

		testHarness.setProcessingTime(7000);

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 3999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 5), 3999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 5), 4999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 5999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	public void testLateness() throws Exception {
		final int windowSize = 2;
		final long lateness = 500;

		ReduceFunction<Tuple2<String, Integer>> sumReducer = new SumReducer();
		ListStateDescriptor<TimeWindow> windowStateDesc =
			new ListStateDescriptor<>("window-slice-mappings", new TimeWindow.Serializer());

		ReducingStateDescriptor<Tuple2<String, Integer>> sliceStateDesc = new ReducingStateDescriptor<>("slice-contents",
			sumReducer,
			STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		AlignedWindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new AlignedWindowOperator<>(
				AlignedSlidingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS), Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				windowStateDesc,
				sliceStateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				sumReducer::reduce,
				PurgingTrigger.of(EventTimeTrigger.create()),
				lateness,
				lateOutputTag);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
		ConcurrentLinkedQueue<Object> lateExpected = new ConcurrentLinkedQueue<>();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 500));
		testHarness.processWatermark(new Watermark(1500));

		expected.add(new Watermark(1500));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1300));
		testHarness.processWatermark(new Watermark(2300));

		expected.add(new StreamRecord<>(new Tuple2<>("key2", 2), 1999));
		expected.add(new Watermark(2300));

		// this will not be sideoutput because window.maxTimestamp() + allowedLateness > currentWatermark
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1997));
		testHarness.processWatermark(new Watermark(6000));

		// this is 1 and not 3 because the trigger fires and purges
		expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
		expected.add(new Watermark(6000));

		// this will be side output because window.maxTimestamp() + allowedLateness < currentWatermark
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
		testHarness.processWatermark(new Watermark(7000));

		lateExpected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
		expected.add(new Watermark(7000));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput(), new Tuple2ResultSortComparator());

		TestHarnessUtil.assertOutputEqualsSorted(
				"SideOutput was not correct.",
				lateExpected,
				(Iterable) testHarness.getSideOutput(lateOutputTag),
				new Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	public void testAlignedSideOutputDueToLatenessSliding() throws Exception {
		final int windowSize = 3;
		final int windowSlide = 1;
		final long lateness = 0;

		ReduceFunction<Tuple2<String, Integer>> sumReducer = new SumReducer();
		ListStateDescriptor<TimeWindow> windowStateDesc =
			new ListStateDescriptor<>("window-slice-mappings", new TimeWindow.Serializer());

		ReducingStateDescriptor<Tuple2<String, Integer>> sliceStateDesc = new ReducingStateDescriptor<>("slice-contents",
			sumReducer,
			STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		AlignedWindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new AlignedWindowOperator<>(
				AlignedSlidingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS), Time.of(windowSlide, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				windowStateDesc,
				sliceStateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				sumReducer::reduce,
				EventTimeTrigger.create(),
				lateness,
				lateOutputTag);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
		ConcurrentLinkedQueue<Object> sideExpected = new ConcurrentLinkedQueue<>();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processWatermark(new Watermark(1999));

		expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
		expected.add(new Watermark(1999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2000));
		testHarness.processWatermark(new Watermark(3000));

		expected.add(new StreamRecord<>(new Tuple2<>("key2", 2), 2999));
		expected.add(new Watermark(3000));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 3001));

		// lateness is set to 0 and window size = 3 sec and slide 1, the following 2 elements (2400)
		// are assigned to windows ending at 2999, 3999, 4999.
		// The 2999 is dropped because it is already late (WM = 2999) but the rest are kept.

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2400));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2400));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 3001));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3900));
		testHarness.processWatermark(new Watermark(6000));

		expected.add(new StreamRecord<>(new Tuple2<>("key2", 5), 3999));
		expected.add(new StreamRecord<>(new Tuple2<>("key1", 2), 3999));

		expected.add(new StreamRecord<>(new Tuple2<>("key2", 4), 4999));
		expected.add(new StreamRecord<>(new Tuple2<>("key1", 2), 4999));

		expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 5999));
		expected.add(new StreamRecord<>(new Tuple2<>("key1", 2), 5999));

		expected.add(new Watermark(6000));

		// sideoutput element due to lateness
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 3001));
		sideExpected.add(new StreamRecord<>(new Tuple2<>("key1", 1), 3001));

		testHarness.processWatermark(new Watermark(25000));

		expected.add(new Watermark(25000));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput(), new Tuple2ResultSortComparator());
		TestHarnessUtil.assertOutputEqualsSorted("SideOutput was not correct.", sideExpected, (Iterable) testHarness.getSideOutput(lateOutputTag), new Tuple2ResultSortComparator());
		testHarness.close();
	}

	@Test
	@Ignore
	public void testCleanupTimeOverflow() throws Exception {
		final int windowSize = 1000;
		final long lateness = 2000;

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			STRING_INT_TUPLE.createSerializer(new ExecutionConfig()));

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(windowSize));

		final WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new WindowOperator<>(
				windowAssigner,
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				EventTimeTrigger.create(),
				lateness,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

		long timestamp = Long.MAX_VALUE - 1750;
		Collection<TimeWindow> windows = windowAssigner.assignWindows(new Tuple2<>("key2", 1), timestamp, new WindowAssigner.WindowAssignerContext() {
			@Override
			public long getCurrentProcessingTime() {
				return operator.windowAssignerContext.getCurrentProcessingTime();
			}
		});
		TimeWindow window = Iterables.getOnlyElement(windows);

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), timestamp));

		// the garbage collection timer would wrap-around
		Assert.assertTrue(window.maxTimestamp() + lateness < window.maxTimestamp());

		// and it would prematurely fire with watermark (Long.MAX_VALUE - 1500)
		Assert.assertTrue(window.maxTimestamp() + lateness < Long.MAX_VALUE - 1500);

		// if we don't correctly prevent wrap-around in the garbage collection
		// timers this watermark will clean our window state for the just-added
		// element/window
		testHarness.processWatermark(new Watermark(Long.MAX_VALUE - 1500));

		// this watermark is before the end timestamp of our only window
		Assert.assertTrue(Long.MAX_VALUE - 1500 < window.maxTimestamp());
		Assert.assertTrue(window.maxTimestamp() < Long.MAX_VALUE);

		// push in a watermark that will trigger computation of our window
		testHarness.processWatermark(new Watermark(window.maxTimestamp()));

		expected.add(new Watermark(Long.MAX_VALUE - 1500));
		expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), window.maxTimestamp()));
		expected.add(new Watermark(window.maxTimestamp()));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput(), new Tuple2ResultSortComparator());
		testHarness.close();
	}

	private static <OUT> OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, OUT> createTestHarness(OneInputStreamOperator<Tuple2<String, Integer>, OUT> operator) throws Exception {
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);
	}

	// ------------------------------------------------------------------------
	//  UDFs
	// ------------------------------------------------------------------------

	private static class LengthSumAggregate implements AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Integer> createAccumulator() {
			return new Tuple2<>("", 0);
		}

		@Override
		public Tuple2<String, Integer> add(Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
			return new Tuple2<>(value.f0, accumulator.f1 + value.f1);
		}

		@Override
		public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
			return new Tuple2<>(accumulator.f0, accumulator.f1);
		}

		@Override
		public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
			return new Tuple2<>(a.f0, a.f1 + b.f1);
		}
	}

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

	private static class TupleKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}
}
