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

package org.apache.flink.runtime.security.factories;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.contexts.SecurityContext;

/**
 * A factory for a {@link SecurityContext}.
 *
 * <p>There can only be one security context installed in each secure runtime.
 */
public interface SecurityContextFactory {

	/**
	 * Returns {@code true} if this factory is compatible with the options in the
	 * provided configuration, {@code false} otherwise.
	 *
	 * @param configuration flink configurations.
	 * @param securityConfig security configurations.
	 * @return true if factory is compatible
	 */
	boolean isCompatibleWith(final Configuration configuration, final SecurityConfiguration securityConfig);

	/**
	 * create security context.
	 *
	 * @param securityConfig security configuration used to create context.
	 * @return the security context object.
	 */
	SecurityContext createContext(SecurityConfiguration securityConfig);
}
