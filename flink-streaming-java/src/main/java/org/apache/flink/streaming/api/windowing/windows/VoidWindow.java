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

package org.apache.flink.streaming.api.windowing.windows;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A {@link Window} that is used during same window operations.
 * This window is designed to be used as a static constant.
 */
public final class VoidWindow extends Window {
	public static final VoidWindow INSTANCE = new VoidWindow();

	@Override
	public boolean equals(Object o) {
		return (o instanceof VoidWindow);
	}

	@Override
	public int hashCode() {
		return 274261021;
	}
	/**
	 * Void maxTimestamp method always returns -1
	 * @return
	 */
	@Override
	public long maxTimestamp() {
		return Long.MAX_VALUE;
	}

	// ------------------------------------------------------------------------
	// Serializer
	// ------------------------------------------------------------------------

	/**
	 * The serializer used to write the TimeWindow type.
	 */
	public static class Serializer extends TypeSerializerSingleton<VoidWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public VoidWindow createInstance() {
			return VoidWindow.INSTANCE;
		}

		@Override
		public VoidWindow copy(VoidWindow from) {
			return VoidWindow.INSTANCE;
		}

		@Override
		public VoidWindow copy(VoidWindow from, VoidWindow reuse) {
			return VoidWindow.INSTANCE;
		}

		@Override
		public int getLength() {
			return 0;
		}

		@Override
		public void serialize(VoidWindow record, DataOutputView target) throws IOException {
		}

		@Override
		public VoidWindow deserialize(DataInputView source) throws IOException {
			return VoidWindow.INSTANCE;
		}

		@Override
		public VoidWindow deserialize(VoidWindow reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.writeLong(source.readLong());
			target.writeLong(source.readLong());
		}

		// ------------------------------------------------------------------------

		@Override
		public TypeSerializerSnapshot<VoidWindow> snapshotConfiguration() {
			return new VoidWindow.Serializer.VoidWindowSerializerSnapshot();
		}

		/**
		 * Serializer configuration snapshot for compatibility and format evolution.
		 */
		@SuppressWarnings("WeakerAccess")
		public static final class VoidWindowSerializerSnapshot extends SimpleTypeSerializerSnapshot<VoidWindow> {

			public VoidWindowSerializerSnapshot() {
				super(VoidWindow.Serializer::new);
			}
		}
	}

}
