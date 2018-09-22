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

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;

/**
 * An {@code AlignedWindowAssigner} extends {@link WindowAssigner>}.
 *
 * <p>In addition to typical window assigner operations, Aligned window assigner assigns
 * one unique window slice that the element belongs to, or zero slice if it belongs to none
 * of the window slices.
 *
 * @param <T> The type of elements that this WindowAssigner can assign windows to.
 * @param <W> The type of {@code Window} that this assigner assigns.
 * */
public abstract class AlignedWindowAssigner<T, W extends Window> extends WindowAssigner<T, W> {


	/**
	 * Returns a {@code Collection} of window slices that the element belongs to
	 *
	 * <p>The collection return should either contain zero or one window slice:
	 * when zero window slice, the element does not belong to any slice.
	 * when one window slice, the element belongs to that particular slice.
	 * </p>
	 *
	 * @param element The element to which window slices should be assigned.
	 * @param timestamp The timestamp of the element.
	 * @param context The {@link WindowAssignerContext} in which the assigner operates.
	 */
	public abstract Collection<W> assignSlices(T element, long timestamp, WindowAssignerContext context);
}
