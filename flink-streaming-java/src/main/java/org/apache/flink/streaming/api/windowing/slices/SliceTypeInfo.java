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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.datastream.OverSliceStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.HashMap;
import java.util.Map;

/**
 * Output type for {@link OverSliceStream}
 *
 * A slice is defined by its contents shared key and window
 * This slice container also contains the combined contents
 *
 *
 * @param <T> type of contents contains within the slice
 * @param <K> key type of the elements
 * @param <W> window type of the slice
 */
public class SliceTypeInfo<T, K, W extends Window> extends TypeInformation<Slice<T, K, W>> {
	private TypeInformation elementType;
	private TypeInformation keyType;
	private TypeInformation windowType;

	public SliceTypeInfo(TypeInformation elementType,
						 TypeInformation keyType,
						 TypeInformation windowType) {
		this.elementType = elementType;
		this.keyType = keyType;
		this.windowType = windowType;
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 0;
	}

	@Override
	public int getTotalFields() {
		return 0;
	}

	@Override
	public Class<Slice<T, K, W>> getTypeClass() {
		return null;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	// Needs override for better serialization
	@Override
	public TypeSerializer<Slice<T, K, W>> createSerializer(ExecutionConfig config) {
		return null;
	}

	@Override
	public String toString() {
		return null;
	}

	@Override
	public boolean equals(Object obj) {
		return false;
	}

	@Override
	public int hashCode() {
		return 0;
	}

	@Override
	public boolean canEqual(Object obj) {
		return false;
	}

	@Override
	public Map<String, TypeInformation<?>> getGenericParameters() {
		Map<String, TypeInformation<?>> map = new HashMap<>(3);
		map.put("T", elementType);
		map.put("K", keyType);
		map.put("W", windowType);
		return map;
	}
}
