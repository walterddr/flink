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

package org.apache.flink.streaming.api.windowing.slices;

import org.apache.flink.api.common.typeutils.CompositeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Snapshot class for the {@link MapSerializer}.
 */
public class SliceSerializerSnapshot<T, K, W extends Window> implements TypeSerializerSnapshot<Slice<T, K, W>> {

	private static final int CURRENT_VERSION = 1;

	private CompositeSerializerSnapshot nestedKeyValueSerializerSnapshot;

	/**
	 * Constructor for read instantiation.
	 */
	public SliceSerializerSnapshot() {}

	/**
	 * Constructor to create the snapshot for writing.
	 */
	public SliceSerializerSnapshot(TypeSerializer<K> keySerializer, TypeSerializer<T> elementSerializer, TypeSerializer<W> windowSerializer) {
		this.nestedKeyValueSerializerSnapshot = new CompositeSerializerSnapshot(keySerializer, elementSerializer, windowSerializer);
	}

	@Override
	public int getCurrentVersion() {
		return CURRENT_VERSION;
	}

	@Override
	public TypeSerializer<Slice<T, K, W>> restoreSerializer() {
		return new SliceSerializer<>(
			nestedKeyValueSerializerSnapshot.getRestoreSerializer(0),
			nestedKeyValueSerializerSnapshot.getRestoreSerializer(1),
			nestedKeyValueSerializerSnapshot.getRestoreSerializer(2));
	}

	@Override
	public TypeSerializerSchemaCompatibility<Slice<T, K, W>> resolveSchemaCompatibility(TypeSerializer<Slice<T, K, W>> newSerializer) {
		checkState(nestedKeyValueSerializerSnapshot != null);

		if (newSerializer instanceof SliceSerializer) {
			SliceSerializer<T, K, W> serializer = (SliceSerializer<T, K, W>) newSerializer;

			return nestedKeyValueSerializerSnapshot.resolveCompatibilityWithNested(
				TypeSerializerSchemaCompatibility.compatibleAsIs(),
				serializer.getKeySerializer(),
				serializer.getElementSerializer(),
				serializer.getWindowSerializer());
		}
		else {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		nestedKeyValueSerializerSnapshot.writeCompositeSnapshot(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		this.nestedKeyValueSerializerSnapshot = CompositeSerializerSnapshot.readCompositeSnapshot(in, userCodeClassLoader);
	}
}
