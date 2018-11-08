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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.guava18.com.google.common.collect.FluentIterable;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowOverSliceAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.slices.Slice;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@code Slicedstream} represents a data stream where elements are produced from the result
 * of a {@link WindowedStream} that contains a special kind of window assigner type of
 * {@link org.apache.flink.streaming.api.windowing.assigners.SliceAssigner}. resulting stream
 * from such process returns non-overlapping {@link Slice} of processed results.
 *
 * <p>The windows are conceptually evaluated for each key individually, meaning windows can trigger
 * at different points for each key.
 *
 * <p>Noted that {@code OverSliceStream} is  product of a {@link WindowOperator} with a
 * non-overlapping window assigner. They are itself resulting data stream from a window operation.
 *
 * @param <T> The type of elements contained in the sliced stream.
 * @param <K> The type of the key by which elements are grouped.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns the elements to.
 */
@Public
public class OverSliceStream<T, V, K, W extends Window> {

	/** The keyed data stream that is windowed by this stream. */
	private final KeyedStream<Slice<T, K, W>, K> input;

	/** The default window function passing through from the sliced stream */
	private WindowFunction<T, V, K, W> defaultWindowFunction;

	private final KeySelector<Slice<T, K, W>, K> keySel;

	/** The trigger that is used for window evaluation/emission. */
	private Trigger<? super Slice<T, K, W>, ? super W> trigger;

	/** The evictor that is used for evicting elements before window evaluation. */
	private Evictor<? super Slice<T, K, W>, ? super W> evictor;

	/** The user-specified allowed lateness. */
	private long allowedLateness = 0L;

	/**
	 * Side output {@code OutputTag} for late data. If no tag is set late data will simply be
	 * dropped.
	 */
	private OutputTag<Slice<T, K, W>> lateDataOutputTag;

	@PublicEvolving
	public OverSliceStream(KeyedStream<Slice<T, K, W>, K> input,
						   WindowFunction<T, V, K, W> defaultWindowFunction) {
		this.input = input;
		this.defaultWindowFunction = defaultWindowFunction;
		this.keySel = (KeySelector<Slice<T, K, W>, K>) Slice::getKey;
	}

	/**
	 * Sets the {@code Trigger} that should be used to trigger window emission.
	 */
	@PublicEvolving
	public OverSliceStream<T, V, K, W> trigger(Trigger<? super Slice<T, K, W>, ? super W> trigger) {
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
	public OverSliceStream<T, V, K, W> allowedLateness(Time lateness) {
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
	public OverSliceStream<T, V, K, W> sideOutputLateData(OutputTag<Slice<T, K, W>> outputTag) {
		Preconditions.checkNotNull(outputTag, "Side output tag must not be null.");
		this.lateDataOutputTag = input.getExecutionEnvironment().clean(outputTag);
		return this;
	}

	/**
	 * Sets the {@code Evictor} that should be used to evict elements from a window before emission.
	 *
	 * <p>Note: When using an evictor window performance will degrade significantly, since
	 * incremental aggregation of window results cannot be used.
	 */
	@PublicEvolving
	public OverSliceStream<T, V, K, W> evictor(Evictor<? super Slice<T, K, W>, ? super W> evictor) {
		this.evictor = evictor;
		return this;
	}

	// ------------------------------------------------------------------------
	//  Operations on the keyed windows
	// ------------------------------------------------------------------------

	/**
	 * Collect slice results based on the slicing function provided during the slice stage.
	 *
	 * @param assigner slice over assigner
	 * @return
	 */
	@PublicEvolving
	public SingleOutputStreamOperator<V> slideOver(
		WindowOverSliceAssigner<T, K, W> assigner) {
		return this.slideOver(assigner, defaultWindowFunction);
	}

	// ------------------------------------------------------------------------
	//  Window Function
	// ------------------------------------------------------------------------

	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> slideOver(
		WindowOverSliceAssigner<T, K, W> assigner,
		WindowFunction<T, R, K, W> windowFunction) {

		WindowedStream<Slice<T, K, W>, K, W> windowedStream = new WindowedStream<>(input, assigner);
		if (trigger != null) {
			windowedStream = windowedStream.trigger(trigger);
		}
		if (evictor != null) {
			windowedStream = windowedStream.evictor(evictor);
		}
		return windowedStream.apply(wrapSliceFunction(windowFunction));
	}

	// ------------------------------------------------------------------------
	//  Reduce Function
	// ------------------------------------------------------------------------

	@PublicEvolving
	public SingleOutputStreamOperator<T> slideOver(
		WindowOverSliceAssigner<T, K, W> assigner,
		ReduceFunction<T> reduceFunction) {

		WindowedStream<Slice<T, K, W>, K, W> windowedStream = new WindowedStream<>(input, assigner);
		if (trigger != null) {
			windowedStream = windowedStream.trigger(trigger);
		}
		if (evictor != null) {
			windowedStream = windowedStream.evictor(evictor);
		}
		return windowedStream.aggregate(wrapSliceFunction(reduceFunction));
	}

	// ------------------------------------------------------------------------
	//  Aggregate Function
	// ------------------------------------------------------------------------

	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> slideOver(
		WindowOverSliceAssigner<T, K, W> assigner,
		AggregateFunction<T, T, R> aggregateFunction) {

		WindowedStream<Slice<T, K, W>, K, W> windowedStream = new WindowedStream<>(input, assigner);
		if (trigger != null) {
			windowedStream = windowedStream.trigger(trigger);
		}
		if (evictor != null) {
			windowedStream = windowedStream.evictor(evictor);
		}
		return windowedStream.aggregate(wrapSliceFunction(aggregateFunction));
	}

	// ------------------------------------------------------------------------
	//  Process Window Function - TODO support this - Needs to wrap window function with List iterative window function
	// ------------------------------------------------------------------------

	// ------------------------------------------------------------------------
	//  Function wrapper to process slice container
	// ------------------------------------------------------------------------

	private <R> AggregateFunction<Slice<T, K, W>, T, R> wrapSliceFunction(AggregateFunction<T, T, R> aggregateFunction) {
		return new AggregateFunction<Slice<T, K, W>, T, R>() {
			@Override
			public T createAccumulator() {
				return aggregateFunction.createAccumulator();
			}

			@Override
			public T add(Slice<T, K, W> value, T accumulator) {
				return aggregateFunction.merge(accumulator, value.getContent());
			}

			@Override
			public R getResult(T accumulator) {
				return aggregateFunction.getResult(accumulator);
			}

			@Override
			public T merge(T a, T b) {
				return aggregateFunction.merge(a, b);
			}
		};
	}

	private AggregateFunction<Slice<T, K, W>, T, T> wrapSliceFunction(ReduceFunction<T> reduceFunction) {
		return new AggregateFunction<Slice<T, K, W>, T, T>() {
			@Override
			public T createAccumulator() {
				return null;
			}

			@Override
			public T add(Slice<T, K, W> value, T accumulator) {
				try {
					return reduceFunction.reduce(accumulator, value.getContent());
				} catch (Exception e) {
					return value.getContent();
				}
			}

			@Override
			public T getResult(T accumulator) {
				return accumulator;
			}

			@Override
			public T merge(T a, T b) {
				try {
					return reduceFunction.reduce(a, b);
				} catch (Exception e) {
					return null;
				}
			}
		};
	}

	private <R> WindowFunction<Slice<T, K, W>, R, K, W> wrapSliceFunction(WindowFunction<T, R, K, W> windowFunction) {
		return (WindowFunction<Slice<T, K, W>, R, K, W>) (key, window, input, out) -> windowFunction.apply(key,
			window,
			FluentIterable
				.from(input)
				.transform(Slice::getContent),
			out);
	}
}
