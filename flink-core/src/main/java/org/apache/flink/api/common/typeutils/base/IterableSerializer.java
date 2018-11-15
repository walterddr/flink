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

package org.apache.flink.api.common.typeutils.base;


import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class IterableSerializer<T> extends TypeSerializer<Iterable<T>> {

	private static final long serialVersionUID = 1119562170939152304L;

	/** The serializer for the elements of the list. */
	private final TypeSerializer<T> elementSerializer;

	/**
	 * Creates a list serializer that uses the given serializer to serialize the list's elements.
	 *
	 * @param elementSerializer The serializer for the elements of the list
	 */
	public IterableSerializer(TypeSerializer<T> elementSerializer) {
		this.elementSerializer = checkNotNull(elementSerializer);
	}

	// ------------------------------------------------------------------------
	//  ListSerializer specific properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the serializer for the elements of the list.
	 * @return The serializer for the elements of the list
	 */
	public TypeSerializer<T> getElementSerializer() {
		return elementSerializer;
	}

	// ------------------------------------------------------------------------
	//  Type Serializer implementation
	// ------------------------------------------------------------------------

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<Iterable<T>> duplicate() {
		TypeSerializer<T> duplicateElement = elementSerializer.duplicate();
		return duplicateElement == elementSerializer ? this : new IterableSerializer<>(duplicateElement);
	}

	@Override
	public Iterable<T> createInstance() {
		return new ArrayList<>(0);
	}

	@Override
	public Iterable<T> copy(Iterable<T> from) {
		List<T> newList = new ArrayList<>();
		from.forEach(newList::add);

		// We iterate here rather than accessing by index, because we cannot be sure that
		// the given list supports RandomAccess.
		// The Iterator should be stack allocated on new JVMs (due to escape analysis)
		for (T element : from) {
			newList.add(elementSerializer.copy(element));
		}
		return newList;
	}

	@Override
	public Iterable<T> copy(Iterable<T> from, Iterable<T> reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1; // var length
	}

	@Override
	public void serialize(Iterable<T> list, DataOutputView target) throws IOException {
		int size = 0;
		for (T aList : list) {
			size++;
		}
		target.writeInt(size);

		// We iterate here rather than accessing by index, because we cannot be sure that
		// the given list supports RandomAccess.
		// The Iterator should be stack allocated on new JVMs (due to escape analysis)
		for (T element : list) {
			elementSerializer.serialize(element, target);
		}
	}

	@Override
	public Iterable<T> deserialize(DataInputView source) throws IOException {
		final int size = source.readInt();
		// create new list with (size + 1) capacity to prevent expensive growth when a single element is added
		final List<T> list = new ArrayList<>(size + 1);
		for (int i = 0; i < size; i++) {
			list.add(elementSerializer.deserialize(source));
		}
		return list;
	}

	@Override
	public Iterable<T> deserialize(Iterable<T> reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		// copy number of elements
		final int num = source.readInt();
		target.writeInt(num);
		for (int i = 0; i < num; i++) {
			elementSerializer.copy(source, target);
		}
	}

	// --------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		return obj == this ||
			(obj != null && obj.getClass() == getClass() &&
				elementSerializer.equals(((IterableSerializer) obj).elementSerializer));
	}

	@Override
	public boolean canEqual(Object obj) {
		return true;
	}

	@Override
	public int hashCode() {
		return elementSerializer.hashCode();
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshot & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public TypeSerializerSnapshot<Iterable<T>> snapshotConfiguration() {
		return new IterableSerializerSnapshot<>(elementSerializer);
	}
}
