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

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collections;

/**
 * JMX Server implementation.
 *
 * <p>This JMX Server is launched at start of any entry point of Flink
 * containerized runtime.
 *
 * <p>This implementation is heavily based on j256/simplejmx project.
 *
 * @see <a href="https://github.com/j256/simplejmx/blob/master/src/main/java/com/j256/simplejmx/server/JmxServer.java">j256/simplejmx project</a>
 */
public class JMXServer implements Closeable {
	private static final Logger LOGGER = LoggerFactory.getLogger(JMXServer.class);

	// TODO make this JMXServer a static instance only for each JVM.
	private String serviceUrl;
	private int serverPort;
	private int registryPort;
	private JMXConnectorServer connector;
	private MBeanServer mbeanServer;
	private FlinkRMIServerSocketFactory serverSocketFactory;
	private Registry rmiRegistry;

	/**
	 * Launch server with dynamic port assignment.
	 */
	public JMXServer() {
		this(0);
	}

	/**
	 * Create a JMX server running on a particular registry-port.
	 *
	 * @param registryPort The RMI registry port that you specify in jconsole to connect to the server.
	 */
	public JMXServer(int registryPort) {
		this(registryPort, 0);
	}

	/**
	 * Create a JMX server running on a particular registry and server port pair.
	 *
	 * @param registryPort The RMI registry port that you specify in jconsole to connect to the server.
	 * @param serverPort The RMI server port that jconsole uses to transfer data to/from the server.
	 */
	public JMXServer(int registryPort, int serverPort) {
		this.registryPort = registryPort;
		this.serverPort = serverPort;
	}

	/**
	 * Start our JMX service.
	 */
	public synchronized void open() throws IOException {
		if (mbeanServer != null) {
			// if we've already assigned a mbean-server then there's nothing to start
			return;
		}
		startRmiRegistry();
		startJmxService();
	}

	@Override
	public void close() {
		try {
			connector.stop();
		} catch (Exception e) {
			LOGGER.error("Unable to stop JMX server connector!", e);
		} finally {
			connector = null;
			mbeanServer = null;
		}

		try {
			UnicastRemoteObject.unexportObject(rmiRegistry, true);
		} catch (Exception e) {
			LOGGER.error("Unable to unexport rmiRegistry!", e);
		} finally {
			rmiRegistry = null;
		}

		try {
			serverSocketFactory.getSocket().close();
		} catch (Exception e) {
			LOGGER.error("Unable to close server socket factory port!", e);
			serverSocketFactory = null;
		}
		LOGGER.info("JMX connector server stopped!");
	}

	/**
	 * Get port number to listen for JMX connections.
	 */
	public int getRegistryPort() {
		return registryPort;
	}

	/**
	 * Get the "RMI server port".
	 */
	public int getServerPort() {
		if (serverPort == 0) {
			return registryPort;
		} else {
			return serverPort;
		}
	}

	/**
	 * Get service URL which is used to specify the connection endpoints.
	 */
	public String getServiceUrl() {
		return serviceUrl;
	}

	private void startRmiRegistry() throws IOException {
		if (rmiRegistry != null) {
			return;
		}
		try {
			serverSocketFactory = new FlinkRMIServerSocketFactory();
			rmiRegistry = LocateRegistry.createRegistry(registryPort, null, serverSocketFactory);
			if (registryPort == 0) {
				registryPort = serverSocketFactory.getSocket().getLocalPort();
			}
		} catch (IOException e) {
			throw new IOException("Unable to create RMI registry on port " + registryPort, e);
		}
	}

	private void startJmxService() throws IOException {
		if (connector != null) {
			return;
		}
		if (serverPort == 0) {
			serverPort = tryPokeForNewPort();
		}
		String serverHost = "localhost";
		String registryHost = "";
		if (serviceUrl == null) {
			serviceUrl = "service:jmx:rmi://" + serverHost + ":" + serverPort + "/jndi/rmi://" + registryHost + ":"
				+ registryPort + "/jmxrmi";
		}

		JMXServiceURL url;
		try {
			url = new JMXServiceURL(serviceUrl);
		} catch (MalformedURLException e) {
			throw new IOException("Malformed service url created " + serviceUrl, e);
		}

		try {
			mbeanServer = ManagementFactory.getPlatformMBeanServer();
			connector = JMXConnectorServerFactory.newJMXConnectorServer(
				url,
				Collections.emptyMap(),
				mbeanServer);
			connector.start();
			LOGGER.info("JMX connector server started at: {}", serviceUrl);
		} catch (IOException e) {
			mbeanServer = null;
			connector = null;
			throw new IOException("Could not start JMX connector server on URL: " + url, e);
		}
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

	/**
	 * Socket factory which allows us peek assigned service socket port.
	 */
	private static class FlinkRMIServerSocketFactory implements RMIServerSocketFactory {
		ServerSocket socket = null;

		public ServerSocket createServerSocket(int port) throws IOException {
			socket = new ServerSocket(port);
			return socket;
		}

		public ServerSocket getSocket() {
			return socket;
		}

	}
}
