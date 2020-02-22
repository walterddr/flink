/**
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

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.test.util.TestingSecurityContext;
import org.apache.flink.test.util.YarnSecureTestEnvironment;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * An extension of the {@link YARNSessionFIFOITCase} that runs the tests in a secured YARN cluster.
 */
public class YARNSessionFIFOSecuredDelegationTokenITCase extends YARNSessionFIFOITCase {

	protected static final Logger LOG = LoggerFactory.getLogger(YARNSessionFIFOSecuredDelegationTokenITCase.class);

	@BeforeClass
	public static void setup() {

		LOG.info("starting secure cluster environment for testing");

		YARN_CONFIGURATION.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
		YARN_CONFIGURATION.setInt(YarnConfiguration.NM_PMEM_MB, 768);
		YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 512);
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-fifo-secured");

		YarnSecureTestEnvironment.prepare(tmp);

		populateYarnSecureConfigurations(YARN_CONFIGURATION, YarnSecureTestEnvironment.getHadoopServicePrincipal(),
			YarnSecureTestEnvironment.getTestKeytab());

		Configuration flinkConfig = new Configuration();
		flinkConfig.setString(SecurityOptions.KERBEROS_LOGIN_KEYTAB,
			YarnSecureTestEnvironment.getTestKeytab());
		flinkConfig.setString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL,
			YarnSecureTestEnvironment.getHadoopServicePrincipal());

		SecurityConfiguration securityConfig =
			new SecurityConfiguration(flinkConfig);

		try {
			TestingSecurityContext.install(securityConfig, YarnSecureTestEnvironment.getClientSecurityConfigurationMap());

			SecurityUtils.getInstalledContext().runSecured(new Callable<Object>() {
				@Override
				public Integer call() {
					startYARNSecureMode(YARN_CONFIGURATION, YarnSecureTestEnvironment.getHadoopServicePrincipal(),
						YarnSecureTestEnvironment.getTestKeytab());
					return null;
				}
			});

		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while setting up secure test context. Reason: {}", e);
		}

	}

	@AfterClass
	public static void teardownSecureCluster() {
		LOG.info("tearing down secure cluster environment");
		YarnSecureTestEnvironment.cleanup();
	}

	@Test(timeout = 60000) // timeout after a minute.
	@Override
	public void testDetachedMode() throws Exception {
		runTest(() -> {
			LOG.info("Starting testDetachedMode()");
			runDetachedModeTest();

			final String[] mustHave = {"user-delegation.token", "(auth:KERBEROS)"};
			final String[] mustNotHave = {"using keytab file"};
			final boolean jobManagerRunsWithKerberos = verifyStringsInNamedLogFiles(mustHave, "jobmanager.log");
			final boolean jobManagerRunsWithoutKeytab = !verifyStringsInNamedLogFiles(mustNotHave, "jobmanager.log");
			final boolean taskManagerRunsWithKerberos = verifyStringsInNamedLogFiles(mustHave, "taskmanager.log");
			final boolean taskManagerRunsWithoutKeytab = !verifyStringsInNamedLogFiles(mustNotHave, "taskmanager.log");

			Assert.assertThat(
				"The JobManager and the TaskManager should not be logged in with keytab.",
				jobManagerRunsWithoutKeytab && taskManagerRunsWithoutKeytab,
				Matchers.is(true));

			Assert.assertThat(
				"The JobManager and the TaskManager should both run with Delegation Token.",
				jobManagerRunsWithKerberos && taskManagerRunsWithKerberos,
				Matchers.is(true));
		});
	}


	/* For secure cluster testing, it is enough to run only one test and override below test methods
	 * to keep the overall build time minimal
	 */
	@Override
	public void testQueryCluster() {}

	@Override
	public void testResourceComputation() {}

	@Override
	public void testfullAlloc() {}
}
