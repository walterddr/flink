/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.windowing.slices.Slice;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;

/**
 * A {@code WindowAssigner} assigns zero or more {@link Window Windows} to a specific {@link Slice}.
 *
 * @param <T> The type of elements that this WindowAssigner can assign windows to.
 * @param <K> The type of keys a slice is determined against.
 * @param <W> The type of {@code Window} that this assigner assigns.
 */
@PublicEvolving
public abstract class WindowOverSliceAssigner<T, K, W extends Window> extends WindowAssigner<Object, W> {
	private static final long serialVersionUID = 1L;

	public abstract TypeInformation<TimeWindow> getSliceType();

	public abstract Collection<W> assignWindowsFromSlice(Slice<T, K, W> element, long timestamp, WindowAssignerContext context);

	protected abstract Collection<W> assignWindowsFromTimestamp(long timestamp, WindowAssignerContext context);

	@SuppressWarnings("unchecked")
	@Override
	public Collection<W> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		if (element instanceof Slice) {
			return assignWindowsFromSlice((Slice<T, K, W>)element, timestamp, context);
		} else {
			return assignWindowsFromTimestamp(timestamp, context);
		}
	}
}
