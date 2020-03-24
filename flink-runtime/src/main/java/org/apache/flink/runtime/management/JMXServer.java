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

package org.apache.flink.runtime.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMIServerSocketFactory;
import java.util.Collections;

/**
 * Wrapper to host the programmatically started JMX server and RMI server.
 *
 * <p>This server starts the {@link JMXConnectorServer} on a separate thread,
 * this must be closed properly in order to correctly stop the process.
 */
public class JMXServer implements AutoCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger(JMXServer.class);

	private final int requestServerPort;

	private FlinkRMIServerSocketFactory rmiServerSocketFactory;
	private int serverPort;
	private JMXConnectorServer jmxServer;
	private JMXServiceURL jmxUrl;

	public JMXServer() {
		this(0);
	}

	JMXServer(int serverPort) {
		this.requestServerPort = serverPort;
	}

	public void start() throws Exception {
		setJMXProperties();
		// prepare env
		rmiServerSocketFactory = new FlinkRMIServerSocketFactory();
		LocateRegistry.createRegistry(this.requestServerPort, null, rmiServerSocketFactory);
		int registryPort = rmiServerSocketFactory.socket.getLocalPort();
		int serverPort = tryPokeForNewPort();
		jmxUrl = new JMXServiceURL("service:jmx:rmi://localhost:" + serverPort + "/jndi/rmi://localhost:" + registryPort + "/jmxrmi");
		jmxServer = JMXConnectorServerFactory.newJMXConnectorServer(
			jmxUrl,
			Collections.emptyMap(),
			ManagementFactory.getPlatformMBeanServer());
		// start jmx server
		jmxServer.start();
	}

	private int tryPokeForNewPort() {
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(0);
			return serverSocket.getLocalPort();
		} catch (IOException e) {
			LOGGER.warn("Unable to allocate new Server Socket!", e);
			return -1;
		} finally {
			if (serverSocket != null) {
				try {
					serverSocket.close();
				} catch (IOException e) {
					LOGGER.warn("Unable to close allocated new Server Socket!", e);
				}
			}
		}
	}

	@Override
	public void close() throws Exception {
		if (jmxServer != null) {
			jmxServer.stop();
			jmxServer = null;
		}
		if (rmiServerSocketFactory != null) {
			rmiServerSocketFactory.socket.close();
			rmiServerSocketFactory = null;
		}
	}

	public int getRegistryPort() {
		return rmiServerSocketFactory.socket.getLocalPort();
	}

	public int getServerPort() {
		return serverPort;
	}

	public String getJmxUrl() {
		return jmxUrl.toString();
	}

	private static void setJMXProperties() {
		if (System.getProperty("com.sun.management.jmxremote") != null) {
			LOGGER.warn("com.sun.management.jmxremote doesn't need to be set, please use flink.runtime.enable.jmxremote instead!");
		}
		checkAndSetSystemProperties("com.sun.management.jmxremote.authenticate", "false");
		checkAndSetSystemProperties("com.sun.management.jmxremote.local.only", "false");
		checkAndSetSystemProperties("com.sun.management.jmxremote.ssl", "false");
		checkAndSetSystemProperties("java.rmi.server.hostname", "localhost");
	}

	private static void checkAndSetSystemProperties(String k, String v) {
		if (System.getProperty(k) == null) {
			System.setProperty(k, v);
		}
	}

	private static class FlinkRMIServerSocketFactory implements RMIServerSocketFactory {
		ServerSocket socket = null;

		public ServerSocket createServerSocket(int port) throws IOException {
			socket = new ServerSocket(port);
			return socket;
		}
	}
}
