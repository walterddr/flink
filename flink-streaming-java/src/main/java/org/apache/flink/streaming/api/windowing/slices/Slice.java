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

package org.apache.flink.streaming.api.windowing.slices;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.streaming.api.datastream.SlicedResultStream;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.types.Row;

/**
 * Output type for {@link SlicedResultStream}
 *
 * A slice is defined by its contents shared key and window
 * This slice container also contains the combined contents
 *
 *
 * @param <T> type of contents contains within the slice
 * @param <K> key type of the elements
 * @param <W> window type of the slice
 */
@TypeInfo(SliceTypeInfoFactory.class)
public class Slice<T, K, W extends Window> extends Row {

	private T content;
	private K key;
	private W window;

	public Slice(T content, K key, W window) {
		super(3);
		this.key = key;
		this.content = content;
		this.window = window;
	}


	public T getContent() {
		return content;
	}

	public void setContent(T content) {
		this.content = content;
	}

	public W getWindow() {
		return window;
	}

	public void setWindow(W window) {
		this.window = window;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}
}
