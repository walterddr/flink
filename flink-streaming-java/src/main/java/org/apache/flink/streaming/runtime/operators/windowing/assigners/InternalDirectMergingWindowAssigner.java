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

package org.apache.flink.streaming.runtime.operators.windowing.assigners;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

/**
 * Internal window assigner that assigns actual affected window.
 *
 * @param <T> The type of elements that this WindowAssigner can assign windows to.
 * @param <W> The type of {@code Window} that this assigner assigns.
 */
@Internal
public class InternalDirectMergingWindowAssigner<T, W extends Window>
	implements InternalMergingWindowAssigner<T, W>, Serializable {
	private static final long serialVersionUID = 1L;

	private MergingWindowAssigner<T, W> windowAssigner;

	public InternalDirectMergingWindowAssigner(MergingWindowAssigner<T, W> windowAssigner) {
		this.windowAssigner = windowAssigner;
	}

	@Override
	public Collection<W> assignWindows(T element, long timestamp, WindowAssigner.WindowAssignerContext context) {
		return windowAssigner.assignWindows(element, timestamp, context);
	}

	@Override
	public Collection<W> getAffectedWindows(T element, long timestamp, WindowAssigner.WindowAssignerContext context) {
		return windowAssigner.assignWindows(element, timestamp, context);
	}

	@Override
	public Collection<W> getAssociatedWindows(W affectedWindow, long timestamp, WindowAssigner.WindowAssignerContext context) {
		return Collections.singletonList(affectedWindow);
	}

	@Override
	public WindowAssigner<T, W> getWindowAssigner() {
		return windowAssigner;
	}

	@Override
	public boolean isEventTime() {
		return windowAssigner.isEventTime();
	}

	@Override
	public void mergeWindows(Collection<W> windows, MergingWindowAssigner.MergeCallback<W> callback) {
		this.windowAssigner.mergeWindows(windows, callback);
	}
}
