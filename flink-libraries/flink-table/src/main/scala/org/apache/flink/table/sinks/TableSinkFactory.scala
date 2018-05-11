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

package org.apache.flink.table.sinks

import java.util

import org.apache.flink.table.catalog.TableFactory

/**
  * A factory to create a [[TableSink]]. This factory is used with Java's Service Provider
  * Interfaces (SPI) for discovering. A factory is called with a set of normalized properties that
  * describe the desired table sink. The factory allows for matching to the given set of
  * properties and creating a configured [[TableSink]] accordingly.
  *
  * Classes that implement this interface need to be added to the
  * "META_INF/services/org.apache.flink.table.sources.TableSinkFactory' file of a JAR file in
  * the current classpath to be found.
  */
trait TableSinkFactory[T] extends TableFactory[T] {

  /**
    * Creates and configures a [[TableSink]] using the given properties.
    *
    * @param properties normalized properties describing a table sink
    * @return the configured table sink
    */
  def create(properties: util.Map[String, String]): TableSink[T]

}
