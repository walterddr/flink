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

package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * Base interface for Retractable functions. Retracting functions are special operations on
 * elements and remove them from the aggregation state described in {@code AggregateFunction}
 * or in {@code ReduceFunction}.
 *
 * @param <T> Type of the input elements.
 */
@Public
@FunctionalInterface
public interface RetractableFunction<T> extends Function, Serializable {

	/**
	 * The retract method. Takes an element out of cumulative dataset.
	 *
	 * @param value The input value.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	void retract(T value) throws Exception;
}
