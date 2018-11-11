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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A serializer for {@link Slice}.
 *
 * @param <T> The type of the elements in the slice.
 * @param <K> The type of the keys in the slice.
 * @param <W> The type of the window in the slice.
 */
@Internal
public final class SliceSerializer<T, K, W extends Window> extends TypeSerializer<Slice<T, K ,W>> {

	private static final long serialVersionUID = -6885593032367050078L;

	private final TypeSerializer<K> keySerializer;

	private final TypeSerializer<T> elementSerializer;

	private final TypeSerializer<W> windowSerializer;

	/**
	 * Creates a serializer
	 *
	 * @param elementSerializer The serializer for the elements in the map
	 * @param keySerializer The serializer for the keys in the map
	 * @param windowSerializer The serializer for the windows in the map
	 */
	public SliceSerializer(TypeSerializer<K> keySerializer, TypeSerializer<T> elementSerializer, TypeSerializer<W> windowSerializer) {
		this.keySerializer = Preconditions.checkNotNull(keySerializer, "The key serializer cannot be null");
		this.elementSerializer = Preconditions.checkNotNull(elementSerializer, "The element serializer cannot be null.");
		this.windowSerializer = Preconditions.checkNotNull(windowSerializer, "The window serializer cannot be null.");
	}

	// ------------------------------------------------------------------------
	//  MapSerializer specific properties
	// ------------------------------------------------------------------------

	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public TypeSerializer<T> getElementSerializer() {
		return elementSerializer;
	}

	public TypeSerializer<W> getWindowSerializer() {
		return windowSerializer;
	}

	// ------------------------------------------------------------------------
	//  Type Serializer implementation
	// ------------------------------------------------------------------------

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<Slice<T, K, W>> duplicate() {
		TypeSerializer<K> duplicateKeySerializer = keySerializer.duplicate();
		TypeSerializer<T> duplicateElementSerializer = elementSerializer.duplicate();
		TypeSerializer<W> duplicateWindowSerializer = windowSerializer.duplicate();

		return (duplicateKeySerializer == keySerializer) && (duplicateElementSerializer == elementSerializer) && (duplicateWindowSerializer == windowSerializer)
			? this
			: new SliceSerializer<>(duplicateKeySerializer, duplicateElementSerializer, duplicateWindowSerializer);
	}

	@Override
	public Slice<T, K, W> createInstance() {
		return new Slice<>(null, null, null);
	}

	@Override
	public Slice<T, K, W> copy(Slice<T, K, W> from) {
		return new Slice<>(from.getContent(), from.getKey(), from.getWindow());
	}

	@Override
	public Slice<T, K, W> copy(Slice<T, K, W> from, Slice<T, K, W> reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1; // var length
	}

	@Override
	public void serialize(Slice<T, K, W> slice, DataOutputView target) throws IOException {
		keySerializer.serialize(slice.getKey(), target);
		elementSerializer.serialize(slice.getContent(), target);
		windowSerializer.serialize(slice.getWindow(), target);
	}

	@Override
	public Slice<T, K, W> deserialize(DataInputView source) throws IOException {
		K key = keySerializer.deserialize(source);
		T content = elementSerializer.deserialize(source);
		W window = windowSerializer.deserialize(source);
		return new Slice<>(content, key, window);
	}

	@Override
	public Slice<T, K, W> deserialize(Slice<T, K, W> reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		final int size = source.readInt();
		target.writeInt(size);

		keySerializer.copy(source, target);
		elementSerializer.copy(source, target);
		windowSerializer.copy(source, target);
	}

	@Override
	public boolean equals(Object obj) {
		return obj == this ||
			(obj != null && obj.getClass() == getClass() &&
				keySerializer.equals(((SliceSerializer<?, ?, ?>) obj).getKeySerializer()) &&
				elementSerializer.equals(((SliceSerializer<?, ?, ?>) obj).getElementSerializer()) &&
				windowSerializer.equals(((SliceSerializer<?, ?, ?>) obj).getWindowSerializer()));
	}

	@Override
	public boolean canEqual(Object obj) {
		return (obj != null && obj.getClass() == getClass());
	}

	@Override
	public int hashCode() {
		return (keySerializer.hashCode() * 31 + elementSerializer.hashCode()) * 31 + windowSerializer.hashCode();
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting
	// --------------------------------------------------------------------------------------------

	@Override
	public TypeSerializerSnapshot<Slice<T, K, W>> snapshotConfiguration() {
		return new SliceSerializerSnapshot<>(keySerializer, elementSerializer, windowSerializer);
	}
}
