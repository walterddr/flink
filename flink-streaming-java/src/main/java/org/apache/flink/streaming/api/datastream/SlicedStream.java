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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.*;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SliceAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.slices.Slice;
import org.apache.flink.streaming.api.windowing.slices.SliceTypeInfo;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.SliceOperator;
import org.apache.flink.streaming.runtime.operators.windowing.functions.*;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.lang.reflect.Type;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code WindowedStream} represents a data stream where elements are grouped by
 * key, and for each key, the stream of elements is split into windows based on a
 * {@link WindowAssigner}. Window emission
 * is triggered based on a {@link Trigger}.
 *
 * <p>The windows are conceptually evaluated for each key individually, meaning windows can trigger
 * at different points for each key.
 *
 * <p>If an {@link Evictor} is specified it will be used to evict elements from the window after
 * evaluation was triggered by the {@code Trigger} but before the actual evaluation of the window.
 * When using an evictor window performance will degrade significantly, since
 * incremental aggregation of window results cannot be used.
 *
 * <p>Note that the {@code WindowedStream} is purely and API construct, during runtime the
 * {@code WindowedStream} will be collapsed together with the {@code KeyedStream} and the operation
 * over the window into one single operation.
 *
 * @param <T> The type of elements in the stream.
 * @param <K> The type of the key by which elements are grouped.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns the elements to.
 */
@Public
public class SlicedStream<T, K, W extends Window> extends WindowedStream<T, K, W> {

	/** The keyed data stream that is windowed by this stream. */
	private final KeyedStream<T, K> input;

	/** The window assigner. */
	private final SliceAssigner<? super T, W> sliceAssigner;

	/** The trigger that is used for window evaluation/emission. */
	private Trigger<? super T, ? super W> trigger;

	/** The user-specified allowed lateness. */
	private long allowedLateness = 0L;

	/**
	 * Side output {@code OutputTag} for late data. If no tag is set late data will simply be
	 * dropped.
 	 */
	private OutputTag<T> lateDataOutputTag;

	@PublicEvolving
	public SlicedStream(KeyedStream<T, K> input,
						SliceAssigner<? super T, W> windowAssigner) {
		super(input, windowAssigner);
		this.input = input;
		this.sliceAssigner = windowAssigner;
		this.trigger = windowAssigner.getDefaultTrigger(input.getExecutionEnvironment());
	}

	/**
	 * Sets the {@code Trigger} that should be used to trigger window emission.
	 */
	@PublicEvolving
	public SlicedStream<T, K, W> trigger(Trigger<? super T, ? super W> trigger) {
		this.trigger = trigger;
		return this;
	}

	/**
	 * Sets the time by which elements are allowed to be late. Elements that
	 * arrive behind the watermark by more than the specified time will be dropped.
	 * By default, the allowed lateness is {@code 0L}.
	 *
	 * <p>Setting an allowed lateness is only valid for event-time windows.
	 */
	@PublicEvolving
	public SlicedStream<T, K, W> allowedLateness(Time lateness) {
		final long millis = lateness.toMilliseconds();
		checkArgument(millis >= 0, "The allowed lateness cannot be negative.");

		this.allowedLateness = millis;
		return this;
	}

	/**
	 * Send late arriving data to the side output identified by the given {@link OutputTag}. Data
	 * is considered late after the watermark has passed the end of the window plus the allowed
	 * lateness set using {@link #allowedLateness(Time)}.
	 *
	 * <p>You can get the stream of late data using
	 * {@link SingleOutputStreamOperator#getSideOutput(OutputTag)} on the
	 * {@link SingleOutputStreamOperator} resulting from the windowed operation
	 * with the same {@link OutputTag}.
	 */
	@PublicEvolving
	public SlicedStream<T, K, W> sideOutputLateData(OutputTag<T> outputTag) {
		Preconditions.checkNotNull(outputTag, "Side output tag must not be null.");
		this.lateDataOutputTag = input.getExecutionEnvironment().clean(outputTag);
		return this;
	}

	// ------------------------------------------------------------------------
	//  Operations on the keyed windows
	// ------------------------------------------------------------------------

	/**
	 * Applies a reduce function to the window. The window function is called for each evaluation
	 * of the window for each key individually. The output of the reduce function is interpreted
	 * as a regular non-windowed stream.
	 *
	 * <p>This window will try and incrementally aggregate data as much as the window policies
	 * permit. For example, tumbling time windows can aggregate the data, meaning that only one
	 * element per key is stored. Sliding time windows will aggregate on the granularity of the
	 * slide interval, so a few elements are stored per key (one per slide interval).
	 * Custom windows may not be able to incrementally aggregate, or may need to store extra values
	 * in an aggregation tree.
	 *
	 * @param function The reduce function.
	 * @return The data stream that is the result of applying the reduce function to the window.
	 */
	@SuppressWarnings("unchecked")
	public SlicedResultStream<T, K, W> reduceSlice(ReduceFunction<T> function) {
		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of reduce can not be a RichFunction. " +
				"Please use reduce(ReduceFunction, WindowFunction) instead.");
		}
		TypeInformation<T> reducedType = input.getType();

		//clean the closure
		function = input.getExecutionEnvironment().clean(function);
		return reduceSlice(function, reducedType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given reducer.
	 *
	 * @param reduceFunction The reduce function that is used for incremental aggregation.
	 * @param resultType Type information for the result type of the window function.
	 * @return The data stream that is the result of applying the window function to the window.
	 */
	public <R> SlicedResultStream<T, K, W> reduceSlice(
			ReduceFunction<T> reduceFunction,
			TypeInformation<R> resultType) {

		if (reduceFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("ReduceFunction of reduce can not be a RichFunction.");
		}

		//clean the closures
		reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

		final String opName = generateOperatorName(sliceAssigner, trigger, null, reduceFunction, null);
		KeySelector<T, K> keySel = input.getKeySelector();

		OneInputStreamOperator<T, Slice<T, K, W>> operator;

		ReducingStateDescriptor<T> stateDesc = new ReducingStateDescriptor<>("window-contents",
			reduceFunction,
			input.getType().createSerializer(getExecutionEnvironment().getConfig()));

		operator =
			new SliceOperator<>(sliceAssigner,
				sliceAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
				keySel,
				input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<K, W, T>()),
				trigger,
				allowedLateness,
				lateDataOutputTag);

		SingleOutputStreamOperator<Slice<T, K, W>> transform = input.transform(
			opName,
			new SliceTypeInfo<>(resultType,
				input.getKeyType(),
				sliceAssigner.getWindowType()),
			operator);

		return new SlicedResultStream<>(transform.keyBy((KeySelector<Slice<T, K, W>, K>) Slice::getKey));
	}

//	/**
//	 * Applies the given window function to each window. The window function is called for each
//	 * evaluation of the window for each key individually. The output of the window function is
//	 * interpreted as a regular non-windowed stream.
//	 *
//	 * <p>Arriving data is incrementally aggregated using the given reducer.
//	 *
//	 * @param reduceFunction The reduce function that is used for incremental aggregation.
//	 * @param function The window function.
//	 * @return The data stream that is the result of applying the window function to the window.
//	 */
//	@PublicEvolving
//	public <R> SlicedResultStream<R, K, W> reduceSlice(ReduceFunction<T> reduceFunction, ProcessWindowFunction<T, R, K, W> function) {
//		TypeInformation<R> resultType = getProcessWindowFunctionReturnType(function, input.getType(), null);
//
//		return reduceSlice(reduceFunction, function, resultType);
//	}
//
//
//	/**
//	 * Applies the given window function to each window. The window function is called for each
//	 * evaluation of the window for each key individually. The output of the window function is
//	 * interpreted as a regular non-windowed stream.
//	 *
//	 * <p>Arriving data is incrementally aggregated using the given reducer.
//	 *
//	 * @param reduceFunction The reduce function that is used for incremental aggregation.
//	 * @param function The window function.
//	 * @param resultType Type information for the result type of the window function
//	 * @return The data stream that is the result of applying the window function to the window.
//	 */
//	@Internal
//	public <R> SlicedResultStream<R, K, W> reduceSlice(ReduceFunction<T> reduceFunction, ProcessWindowFunction<T, R, K, W> function, TypeInformation<R> resultType) {
//		if (reduceFunction instanceof RichFunction) {
//			throw new UnsupportedOperationException("ReduceFunction of apply can not be a RichFunction.");
//		}
//		//clean the closures
//		function = input.getExecutionEnvironment().clean(function);
//		reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);
//
//		final String opName = generateOperatorName(sliceAssigner, trigger, null, reduceFunction, function);
//		KeySelector<T, K> keySel = input.getKeySelector();
//
//		OneInputStreamOperator<T, Slice<R, K, W>> operator;
//
//		ReducingStateDescriptor<T> stateDesc = new ReducingStateDescriptor<>("window-contents",
//			reduceFunction,
//			input.getType().createSerializer(getExecutionEnvironment().getConfig()));
//
//		operator =
//			new SliceOperator<>(sliceAssigner,
//				sliceAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
//				keySel,
//				input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
//				stateDesc,
//				trigger,
//				allowedLateness,
//				lateDataOutputTag);
//
//		SingleOutputStreamOperator<Slice<R, K, W>> transform = input.transform(
//			opName,
//			new SliceTypeInfo<>(resultType,
//				input.getKeyType(),
//				sliceAssigner.getWindowType()),
//			operator);
//
//		return new SlicedResultStream<>(transform.keyBy((KeySelector<Slice<R, K, W>, K>) Slice::getKey));
//	}

	// ------------------------------------------------------------------------
	//  Aggregation Function
	// ------------------------------------------------------------------------

	/**
	 * Applies the given aggregation function to each window. The aggregation function is called for
	 * each element, aggregating values incrementally and keeping the state to one accumulator
	 * per key and window.
	 *
	 * @param function The aggregation function.
	 * @return The data stream that is the result of applying the fold function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 * @param <R> The type of the elements in the resulting stream, equal to the
	 *            AggregateFunction's result type
	 */
	@PublicEvolving
	public <ACC, R> SlicedResultStream<ACC, K, W> aggregateSlice(AggregateFunction<T, ACC, R> function) {
		checkNotNull(function, "function");

		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregation function cannot be a RichFunction.");
		}

		TypeInformation<ACC> accumulatorType = TypeExtractor.getAggregateFunctionAccumulatorType(
				function, input.getType(), null, false);

		TypeInformation<R> resultType = TypeExtractor.getAggregateFunctionReturnType(
				function, input.getType(), null, false);

		return aggregateSlice(function, accumulatorType, resultType);
	}

	/**
	 * Applies the given aggregation function to each window. The aggregation function is called for
	 * each element, aggregating values incrementally and keeping the state to one accumulator
	 * per key and window.
	 *
	 * @param function The aggregation function.
	 * @return The data stream that is the result of applying the aggregation function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 * @param <R> The type of the elements in the resulting stream, equal to the
	 *            AggregateFunction's result type
	 */
	@PublicEvolving
	public <ACC, R> SlicedResultStream<ACC, K, W> aggregateSlice(
			AggregateFunction<T, ACC, R> function,
			TypeInformation<ACC> accumulatorType,
			TypeInformation<R> resultType) {

		checkNotNull(function, "function");
		checkNotNull(accumulatorType, "accumulatorType");
		checkNotNull(resultType, "resultType");

		if (function instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregation function cannot be a RichFunction.");
		}

		AggregateFunction<T, ACC, ACC> aggFunction = wrapPartialAggregateFunction(function);
		WindowFunction<ACC, R, K, W> windowFunction = wrapAggregateWindowFunction(function);

		return aggregateSlice(aggFunction, windowFunction, accumulatorType, resultType);
	}

	private <R, ACC> AggregateFunction<T, ACC, ACC> wrapPartialAggregateFunction(AggregateFunction<T, ACC, R> function) {
		return new AggregateFunction<T, ACC, ACC>() {

			@Override
			public ACC createAccumulator() {
				return function.createAccumulator();
			}

			@Override
			public ACC add(T value, ACC accumulator) {
				return function.add(value, accumulator);
			}

			@Override
			public ACC getResult(ACC accumulator) {
				return accumulator;
			}

			@Override
			public ACC merge(ACC a, ACC b) {
				return function.merge(a, b);
			}
		};
	}

	private <R, ACC> WindowFunction<ACC, R, K, W> wrapAggregateWindowFunction(AggregateFunction<T,ACC,R> function) {
		return (WindowFunction<ACC, R, K, W>) (k, window, input, out) -> {
			ACC acc = function.createAccumulator();
			for (ACC element: input) {
				acc = function.merge(acc, element);
			}
			out.collect(function.getResult(acc));
		};
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given aggregate function. This means
	 * that the window function typically has only a single value to process when called.
	 *
	 * @param aggFunction The aggregate function that is used for incremental aggregation.
	 * @param windowFunction The window function.
	 *
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 * @param <R> The type of the elements in the resulting stream, equal to the
	 *            WindowFunction's result type
	 */
	@PublicEvolving
	public <ACC, R> SlicedResultStream<ACC, K, W> aggregateSlice(
			AggregateFunction<T, ACC, ACC> aggFunction,
			WindowFunction<ACC, R, K, W> windowFunction) {

		checkNotNull(aggFunction, "aggFunction");
		checkNotNull(windowFunction, "windowFunction");

		TypeInformation<ACC> accumulatorType = TypeExtractor.getAggregateFunctionAccumulatorType(
				aggFunction, input.getType(), null, false);

		TypeInformation<R> resultType = getWindowFunctionReturnType(windowFunction, accumulatorType);

		return aggregateSlice(aggFunction, windowFunction, accumulatorType, resultType);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given aggregate function. This means
	 * that the window function typically has only a single value to process when called.
	 *
	 * @param aggregateFunction The aggregation function that is used for incremental aggregation.
	 * @param windowFunction The window function.
	 * @param accumulatorType Type information for the internal accumulator type of the aggregation function
	 * @param resultType Type information for the result type of the window function
	 *
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 * @param <R> The type of the elements in the resulting stream, equal to the
	 *            WindowFunction's result type
	 */
	@PublicEvolving
	public <ACC, R> SlicedResultStream<ACC, K, W> aggregateSlice(
			AggregateFunction<T, ACC, ACC> aggregateFunction,
			WindowFunction<ACC, R, K, W> windowFunction,
			TypeInformation<ACC> accumulatorType,
			TypeInformation<R> resultType) {

		checkNotNull(aggregateFunction, "aggregateFunction");
		checkNotNull(windowFunction, "windowFunction");
		checkNotNull(accumulatorType, "accumulatorType");
		checkNotNull(resultType, "resultType");

		if (aggregateFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregate function cannot be a RichFunction.");
		}

		//clean the closures
		windowFunction = input.getExecutionEnvironment().clean(windowFunction);
		aggregateFunction = input.getExecutionEnvironment().clean(aggregateFunction);

		final String opName = generateOperatorName(sliceAssigner, trigger, null, aggregateFunction, windowFunction);
		KeySelector<T, K> keySel = input.getKeySelector();

		OneInputStreamOperator<T, Slice<ACC, K, W>> operator;

		AggregatingStateDescriptor<T, ACC, ACC> stateDesc = new AggregatingStateDescriptor<>("window-contents",
			aggregateFunction, accumulatorType.createSerializer(getExecutionEnvironment().getConfig()));

		operator = new SliceOperator<>(sliceAssigner,
			sliceAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
			keySel,
			input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
			stateDesc,
			new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<K, W, ACC>()),
			trigger,
			allowedLateness,
			lateDataOutputTag);

		SingleOutputStreamOperator<Slice<ACC, K, W>> transform = input.transform(
			opName,
			new SliceTypeInfo<>(resultType,
				input.getKeyType(),
				sliceAssigner.getWindowType()),
			operator);

		return new SlicedResultStream<>(transform.keyBy((KeySelector<Slice<ACC, K, W>, K>) Slice::getKey));
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given aggregate function. This means
	 * that the window function typically has only a single value to process when called.
	 *
	 * @param aggFunction The aggregate function that is used for incremental aggregation.
	 * @param windowFunction The window function.
	 *
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 * @param <V> The type of AggregateFunction's result, and the WindowFunction's input
	 * @param <R> The type of the elements in the resulting stream, equal to the
	 *            WindowFunction's result type
	 */
	@PublicEvolving
	public <ACC, V, R> SlicedResultStream<V, K, W> aggregateSlice(
			AggregateFunction<T, ACC, V> aggFunction,
			ProcessWindowFunction<V, R, K, W> windowFunction) {

		checkNotNull(aggFunction, "aggFunction");
		checkNotNull(windowFunction, "windowFunction");

		TypeInformation<ACC> accumulatorType = TypeExtractor.getAggregateFunctionAccumulatorType(
				aggFunction, input.getType(), null, false);

		TypeInformation<V> aggResultType = TypeExtractor.getAggregateFunctionReturnType(
				aggFunction, input.getType(), null, false);

		TypeInformation<R> resultType = getProcessWindowFunctionReturnType(windowFunction, aggResultType, null);

		return aggregateSlice(aggFunction, windowFunction, accumulatorType, aggResultType, resultType);
	}

	private static <IN, OUT, KEY> TypeInformation<OUT> getWindowFunctionReturnType(
		WindowFunction<IN, OUT, KEY, ?> function,
		TypeInformation<IN> inType) {
		return TypeExtractor.getUnaryOperatorReturnType(
			function,
			WindowFunction.class,
			0,
			1,
			new int[]{3, 0},
			inType,
			null,
			false);
	}

	private static <IN, OUT, KEY> TypeInformation<OUT> getProcessWindowFunctionReturnType(
			ProcessWindowFunction<IN, OUT, KEY, ?> function,
			TypeInformation<IN> inType,
			String functionName) {
		return TypeExtractor.getUnaryOperatorReturnType(
			function,
			ProcessWindowFunction.class,
			0,
			1,
			TypeExtractor.NO_INDEX,
			inType,
			functionName,
			false);
	}

	/**
	 * Applies the given window function to each window. The window function is called for each
	 * evaluation of the window for each key individually. The output of the window function is
	 * interpreted as a regular non-windowed stream.
	 *
	 * <p>Arriving data is incrementally aggregated using the given aggregate function. This means
	 * that the window function typically has only a single value to process when called.
	 *
	 * @param aggregateFunction The aggregation function that is used for incremental aggregation.
	 * @param windowFunction The window function.
	 * @param accumulatorType Type information for the internal accumulator type of the aggregation function
	 * @param resultType Type information for the result type of the window function
	 *
	 * @return The data stream that is the result of applying the window function to the window.
	 *
	 * @param <ACC> The type of the AggregateFunction's accumulator
	 * @param <V> The type of AggregateFunction's result, and the WindowFunction's input
	 * @param <R> The type of the elements in the resulting stream, equal to the
	 *            WindowFunction's result type
	 */
	@PublicEvolving
	public <ACC, V, R> SlicedResultStream<V, K, W> aggregateSlice(
			AggregateFunction<T, ACC, V> aggregateFunction,
			ProcessWindowFunction<V, R, K, W> windowFunction,
			TypeInformation<ACC> accumulatorType,
			TypeInformation<V> aggregateResultType,
			TypeInformation<R> resultType) {

		checkNotNull(aggregateFunction, "aggregateFunction");
		checkNotNull(windowFunction, "windowFunction");
		checkNotNull(accumulatorType, "accumulatorType");
		checkNotNull(aggregateResultType, "aggregateResultType");
		checkNotNull(resultType, "resultType");

		if (aggregateFunction instanceof RichFunction) {
			throw new UnsupportedOperationException("This aggregate function cannot be a RichFunction.");
		}

		//clean the closures
		windowFunction = input.getExecutionEnvironment().clean(windowFunction);
		aggregateFunction = input.getExecutionEnvironment().clean(aggregateFunction);

		final String opName = generateOperatorName(sliceAssigner, trigger, null, aggregateFunction, windowFunction);
		KeySelector<T, K> keySel = input.getKeySelector();

		OneInputStreamOperator<T, Slice<V, K, W>> operator;

		AggregatingStateDescriptor<T, ACC, V> stateDesc = new AggregatingStateDescriptor<>("window-contents",
			aggregateFunction, accumulatorType.createSerializer(getExecutionEnvironment().getConfig()));

		operator = new SliceOperator<>(sliceAssigner,
			sliceAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
			keySel,
			input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
			stateDesc,
			new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<K, W, V>()),
			trigger,
			allowedLateness,
			lateDataOutputTag);

		SingleOutputStreamOperator<Slice<V, K, W>> transform = input.transform(
			opName,
			new SliceTypeInfo<>(aggregateResultType,
				input.getKeyType(),
				sliceAssigner.getWindowType()),
			operator);

		return new SlicedResultStream<>(transform.keyBy((KeySelector<Slice<V, K, W>, K>) Slice::getKey));
	}

//	// ------------------------------------------------------------------------
//	//  Window Function (apply)
//	// ------------------------------------------------------------------------
//
//	/**
//	 * Applies the given window function to each window. The window function is called for each
//	 * evaluation of the window for each key individually. The output of the window function is
//	 * interpreted as a regular non-windowed stream.
//	 *
//	 * <p>Not that this function requires that all data in the windows is buffered until the window
//	 * is evaluated, as the function provides no means of incremental aggregation.
//	 *
//	 * @param function The window function.
//	 * @return The data stream that is the result of applying the window function to the window.
//	 */
//	public <R> SlicedResultStream<R, K, W> applySlice(WindowFunction<T, R, K, W> function) {
//		TypeInformation<R> resultType = getWindowFunctionReturnType(function, getInputType());
//
//		return applySlice(function, resultType);
//	}
//
//	/**
//	 * Applies the given window function to each window. The window function is called for each
//	 * evaluation of the window for each key individually. The output of the window function is
//	 * interpreted as a regular non-windowed stream.
//	 *
//	 * <p>Note that this function requires that all data in the windows is buffered until the window
//	 * is evaluated, as the function provides no means of incremental aggregation.
//	 *
//	 * @param function The window function.
//	 * @param resultType Type information for the result type of the window function
//	 * @return The data stream that is the result of applying the window function to the window.
//	 */
//	public <R> SlicedResultStream<R, K, W> applySlice(WindowFunction<T, R, K, W> function, TypeInformation<R> resultType) {
//		function = input.getExecutionEnvironment().clean(function);
//		return applySlice(new InternalIterableWindowFunction<>(function), resultType, function);
//	}
//
//	/**
//	 * Applies the given window function to each window. The window function is called for each
//	 * evaluation of the window for each key individually. The output of the window function is
//	 * interpreted as a regular non-windowed stream.
//	 *
//	 * <p>Not that this function requires that all data in the windows is buffered until the window
//	 * is evaluated, as the function provides no means of incremental aggregation.
//	 *
//	 * @param function The window function.
//	 * @return The data stream that is the result of applying the window function to the window.
//	 */
//	@PublicEvolving
//	public <R> SlicedResultStream<R, K, W> processSlice(ProcessWindowFunction<T, R, K, W> function) {
//		TypeInformation<R> resultType = getProcessWindowFunctionReturnType(function, getInputType(), null);
//
//		return processSlice(function, resultType);
//	}
//
//	/**
//	 * Applies the given window function to each window. The window function is called for each
//	 * evaluation of the window for each key individually. The output of the window function is
//	 * interpreted as a regular non-windowed stream.
//	 *
//	 * <p>Not that this function requires that all data in the windows is buffered until the window
//	 * is evaluated, as the function provides no means of incremental aggregation.
//	 *
//	 * @param function The window function.
//	 * @param resultType Type information for the result type of the window function
//	 * @return The data stream that is the result of applying the window function to the window.
//	 */
//	@Internal
//	public <R> SlicedResultStream<R, K, W> processSlice(ProcessWindowFunction<T, R, K, W> function, TypeInformation<R> resultType) {
//		function = input.getExecutionEnvironment().clean(function);
//		return applySlice(new InternalIterableProcessWindowFunction<>(function), resultType, function);
//	}
//
//	private <R> SlicedResultStream<R, K, W> applySlice(InternalWindowFunction<Iterable<T>, R, K, W> function, TypeInformation<R> resultType, Function originalFunction) {
//
//		final String opName = generateOperatorName(sliceAssigner, trigger, null, originalFunction, null);
//		KeySelector<T, K> keySel = input.getKeySelector();
//
//		WindowOperator<K, T, Iterable<T>, R, W> operator;
//
//		if (evictor != null) {
//			@SuppressWarnings({"unchecked", "rawtypes"})
//			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
//					(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));
//
//			ListStateDescriptor<StreamRecord<T>> stateDesc =
//					new ListStateDescriptor<>("window-contents", streamRecordSerializer);
//
//			operator =
//				new EvictingWindowOperator<>(sliceAssigner,
//					sliceAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
//					keySel,
//					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
//					stateDesc,
//					function,
//					trigger,
//					evictor,
//					allowedLateness,
//					lateDataOutputTag);
//
//		} else {
//			ListStateDescriptor<T> stateDesc = new ListStateDescriptor<>("window-contents",
//				input.getType().createSerializer(getExecutionEnvironment().getConfig()));
//
//			operator =
//				new WindowOperator<>(sliceAssigner,
//					sliceAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
//					keySel,
//					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
//					stateDesc,
//					function,
//					trigger,
//					allowedLateness,
//					lateDataOutputTag);
//		}
//
//		return input.transform(opName, resultType, operator);
//	}

	private static String generateFunctionName(Function function) {
		Class<? extends Function> functionClass = function.getClass();
		if (functionClass.isAnonymousClass()) {
			// getSimpleName returns an empty String for anonymous classes
			Type[] interfaces = functionClass.getInterfaces();
			if (interfaces.length == 0) {
				// extends an existing class (like RichMapFunction)
				Class<?> functionSuperClass = functionClass.getSuperclass();
				return functionSuperClass.getSimpleName() + functionClass.getName().substring(functionClass.getEnclosingClass().getName().length());
			} else {
				// implements a Function interface
				Class<?> functionInterface = functionClass.getInterfaces()[0];
				return functionInterface.getSimpleName() + functionClass.getName().substring(functionClass.getEnclosingClass().getName().length());
			}
		} else {
			return functionClass.getSimpleName();
		}
	}

	private static String generateOperatorName(
			WindowAssigner<?, ?> assigner,
			Trigger<?, ?> trigger,
			@Nullable Evictor<?, ?> evictor,
			Function function1,
			@Nullable Function function2) {
		return "Window(" +
			assigner + ", " +
			trigger.getClass().getSimpleName() + ", " +
			(evictor == null ? "" : (evictor.getClass().getSimpleName() + ", ")) +
			generateFunctionName(function1) +
			(function2 == null ? "" : (", " + generateFunctionName(function2))) +
			")";
	}

//	// ------------------------------------------------------------------------
//	//  Pre-defined aggregations on the keyed windows
//	// ------------------------------------------------------------------------
//
//	/**
//	 * Applies an aggregation that sums every window of the data stream at the
//	 * given position.
//	 *
//	 * @param positionToSum The position in the tuple/array to sum
//	 * @return The transformed DataStream.
//	 */
//	public SingleOutputStreamOperator<T> sum(int positionToSum) {
//		return aggregate(new SumAggregator<>(positionToSum, input.getType(), input.getExecutionConfig()));
//	}
//
//	/**
//	 * Applies an aggregation that sums every window of the pojo data stream at the given field for
//	 * every window.
//	 *
//	 * <p>A field expression is either the name of a public field or a getter method with
//	 * parentheses of the stream's underlying type. A dot can be used to drill down into objects,
//	 * as in {@code "field1.getInnerField2()" }.
//	 *
//	 * @param field The field to sum
//	 * @return The transformed DataStream.
//	 */
//	public SingleOutputStreamOperator<T> sum(String field) {
//		return aggregate(new SumAggregator<>(field, input.getType(), input.getExecutionConfig()));
//	}
//
//	/**
//	 * Applies an aggregation that that gives the minimum value of every window
//	 * of the data stream at the given position.
//	 *
//	 * @param positionToMin The position to minimize
//	 * @return The transformed DataStream.
//	 */
//	public SingleOutputStreamOperator<T> min(int positionToMin) {
//		return aggregate(new ComparableAggregator<>(positionToMin, input.getType(), AggregationFunction.AggregationType.MIN, input.getExecutionConfig()));
//	}
//
//	/**
//	 * Applies an aggregation that that gives the minimum value of the pojo data
//	 * stream at the given field expression for every window.
//	 *
//	 * <p>A field * expression is either the name of a public field or a getter method with
//	 * parentheses of the {@link DataStream}S underlying type. A dot can be used
//	 * to drill down into objects, as in {@code "field1.getInnerField2()" }.
//	 *
//	 * @param field The field expression based on which the aggregation will be applied.
//	 * @return The transformed DataStream.
//	 */
//	public SingleOutputStreamOperator<T> min(String field) {
//		return aggregate(new ComparableAggregator<>(field, input.getType(), AggregationFunction.AggregationType.MIN, false, input.getExecutionConfig()));
//	}
//
//	/**
//	 * Applies an aggregation that gives the minimum element of every window of
//	 * the data stream by the given position. If more elements have the same
//	 * minimum value the operator returns the first element by default.
//	 *
//	 * @param positionToMinBy
//	 *            The position to minimize by
//	 * @return The transformed DataStream.
//	 */
//	public SingleOutputStreamOperator<T> minBy(int positionToMinBy) {
//		return this.minBy(positionToMinBy, true);
//	}
//
//	/**
//	 * Applies an aggregation that gives the minimum element of every window of
//	 * the data stream by the given field. If more elements have the same
//	 * minimum value the operator returns the first element by default.
//	 *
//	 * @param field The field to minimize by
//	 * @return The transformed DataStream.
//	 */
//	public SingleOutputStreamOperator<T> minBy(String field) {
//		return this.minBy(field, true);
//	}
//
//	/**
//	 * Applies an aggregation that gives the minimum element of every window of
//	 * the data stream by the given position. If more elements have the same
//	 * minimum value the operator returns either the first or last one depending
//	 * on the parameter setting.
//	 *
//	 * @param positionToMinBy The position to minimize
//	 * @param first If true, then the operator return the first element with the minimum value, otherwise returns the last
//	 * @return The transformed DataStream.
//	 */
//	public SingleOutputStreamOperator<T> minBy(int positionToMinBy, boolean first) {
//		return aggregate(new ComparableAggregator<>(positionToMinBy, input.getType(), AggregationFunction.AggregationType.MINBY, first, input.getExecutionConfig()));
//	}
//
//	/**
//	 * Applies an aggregation that that gives the minimum element of the pojo
//	 * data stream by the given field expression for every window. A field
//	 * expression is either the name of a public field or a getter method with
//	 * parentheses of the {@link DataStream DataStreams} underlying type. A dot can be used
//	 * to drill down into objects, as in {@code "field1.getInnerField2()" }.
//	 *
//	 * @param field The field expression based on which the aggregation will be applied.
//	 * @param first If True then in case of field equality the first object will be returned
//	 * @return The transformed DataStream.
//	 */
//	public SingleOutputStreamOperator<T> minBy(String field, boolean first) {
//		return aggregate(new ComparableAggregator<>(field, input.getType(), AggregationFunction.AggregationType.MINBY, first, input.getExecutionConfig()));
//	}
//
//	/**
//	 * Applies an aggregation that gives the maximum value of every window of
//	 * the data stream at the given position.
//	 *
//	 * @param positionToMax The position to maximize
//	 * @return The transformed DataStream.
//	 */
//	public SingleOutputStreamOperator<T> max(int positionToMax) {
//		return aggregate(new ComparableAggregator<>(positionToMax, input.getType(), AggregationFunction.AggregationType.MAX, input.getExecutionConfig()));
//	}
//
//	/**
//	 * Applies an aggregation that that gives the maximum value of the pojo data
//	 * stream at the given field expression for every window. A field expression
//	 * is either the name of a public field or a getter method with parentheses
//	 * of the {@link DataStream DataStreams} underlying type. A dot can be used to drill
//	 * down into objects, as in {@code "field1.getInnerField2()" }.
//	 *
//	 * @param field The field expression based on which the aggregation will be applied.
//	 * @return The transformed DataStream.
//	 */
//	public SingleOutputStreamOperator<T> max(String field) {
//		return aggregate(new ComparableAggregator<>(field, input.getType(), AggregationFunction.AggregationType.MAX, false, input.getExecutionConfig()));
//	}
//
//	/**
//	 * Applies an aggregation that gives the maximum element of every window of
//	 * the data stream by the given position. If more elements have the same
//	 * maximum value the operator returns the first by default.
//	 *
//	 * @param positionToMaxBy
//	 *            The position to maximize by
//	 * @return The transformed DataStream.
//	 */
//	public SingleOutputStreamOperator<T> maxBy(int positionToMaxBy) {
//		return this.maxBy(positionToMaxBy, true);
//	}
//
//	/**
//	 * Applies an aggregation that gives the maximum element of every window of
//	 * the data stream by the given field. If more elements have the same
//	 * maximum value the operator returns the first by default.
//	 *
//	 * @param field
//	 *            The field to maximize by
//	 * @return The transformed DataStream.
//	 */
//	public SingleOutputStreamOperator<T> maxBy(String field) {
//		return this.maxBy(field, true);
//	}
//
//	/**
//	 * Applies an aggregation that gives the maximum element of every window of
//	 * the data stream by the given position. If more elements have the same
//	 * maximum value the operator returns either the first or last one depending
//	 * on the parameter setting.
//	 *
//	 * @param positionToMaxBy The position to maximize by
//	 * @param first If true, then the operator return the first element with the maximum value, otherwise returns the last
//	 * @return The transformed DataStream.
//	 */
//	public SingleOutputStreamOperator<T> maxBy(int positionToMaxBy, boolean first) {
//		return aggregate(new ComparableAggregator<>(positionToMaxBy, input.getType(), AggregationFunction.AggregationType.MAXBY, first, input.getExecutionConfig()));
//	}
//
//	/**
//	 * Applies an aggregation that that gives the maximum element of the pojo
//	 * data stream by the given field expression for every window. A field
//	 * expression is either the name of a public field or a getter method with
//	 * parentheses of the {@link DataStream}S underlying type. A dot can be used
//	 * to drill down into objects, as in {@code "field1.getInnerField2()" }.
//	 *
//	 * @param field The field expression based on which the aggregation will be applied.
//	 * @param first If True then in case of field equality the first object will be returned
//	 * @return The transformed DataStream.
//	 */
//	public SingleOutputStreamOperator<T> maxBy(String field, boolean first) {
//		return aggregate(new ComparableAggregator<>(field, input.getType(), AggregationFunction.AggregationType.MAXBY, first, input.getExecutionConfig()));
//	}
//
//	private SingleOutputStreamOperator<T> aggregate(AggregationFunction<T> aggregator) {
//		return reduce(aggregator);
//	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return input.getExecutionEnvironment();
	}

	public TypeInformation<T> getInputType() {
		return input.getType();
	}

	// -------------------- Testing Methods --------------------

	@VisibleForTesting
	long getAllowedLateness() {
		return allowedLateness;
	}
}
