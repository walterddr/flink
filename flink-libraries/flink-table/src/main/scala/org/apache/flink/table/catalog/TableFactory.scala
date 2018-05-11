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

package org.apache.flink.table.catalog

import java.util

/**
  * A factory to create a [[org.apache.flink.table.sources.TableSource]] or
  * [[org.apache.flink.table.sinks.TableSink]]. This factory should be extended and should be used
  * with Java's Service Provider Interfaces (SPI) for discovering. A factory is called with a set
  * of normalized properties that describe the desired table.
  *
  * The factory allows for matching to the given set of properties before invoking other methods
  * to instantiate table objects.
  */
trait TableFactory[T] {

    /**
      * Specifies the context that this factory has been implemented for. The framework guarantees
      * to only call extended methods of the factory if the specified set of properties and
      * values are met.
      *
      * Typical properties might be:
      *   - connector.type
      *   - format.type
      *
      * Specified property versions allow the framework to provide backwards compatible properties
      * in case of string format changes:
      *   - connector.property-version
      *   - format.property-version
      *
      * An empty context means that the factory matches for all requests.
      */
    def requiredContext(): util.Map[String, String]

    /**
      * List of property keys that this factory can handle. This method will be used for validation.
      * If a property is passed that this factory cannot handle, an exception will be thrown. The
      * list must not contain the keys that are specified by the context.
      *
      * Example properties might be:
      *   - format.line-delimiter
      *   - format.ignore-parse-errors
      *   - format.fields.#.type
      *   - format.fields.#.name
      *
      * Note: Use "#" to denote an array of values where "#" represents one or more digits. Property
      * versions like "format.property-version" must not be part of the supported properties.
      */
    def supportedProperties(): util.List[String]
}
