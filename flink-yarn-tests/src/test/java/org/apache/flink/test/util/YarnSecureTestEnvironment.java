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

package org.apache.flink.test.util;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper {@link SecureTestEnvironment} for Yarn end-to-end tests.
 *
 * <p>In addition to the generic secure test environment. This specific Helper class
 * also supports Hadoop delegation token based security.
 */
public class YarnSecureTestEnvironment extends SecureTestEnvironment {

	private static final Logger LOG = LoggerFactory.getLogger(YarnSecureTestEnvironment.class);

	private static String testDelegationToken = null;

	public static void prepare(TemporaryFolder tempFolder) {
		File baseDirForSecureRun;
		try {
			baseDirForSecureRun = tempFolder.newFolder();
		} catch (IOException e) {
			throw new RuntimeException("Cannot instantiate temp folder for secure testing.", e);
		}
		populateMiniKDCContents(baseDirForSecureRun);
		populateDelegationTokenContents(baseDirForSecureRun);
		installSecurityContext();
		populateJavaPropertyVariables();
	}

	public static String getTestDelegationToken() {
		return testDelegationToken;
	}

	protected static void populateDelegationTokenContents(File baseDirForSecureRun) {
		//Setup delegation token, it can be used with the setting useDelegationToken()
		File dtFile = new File(baseDirForSecureRun, "user-delegation.token");
		try {
			obtainDelegationToken(dtFile);
			testDelegationToken = dtFile.getAbsolutePath();
		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while preparing delegation token secure environment.", e);
		}

		// we need to install the system env change here in order to properly install the security context.
		final Map<String, String> originalEnv = System.getenv();
		Map<String, String> systemEnv = new HashMap<>(originalEnv);
		systemEnv.put("HADOOP_TOKEN_FILE_LOCATION", dtFile.getAbsolutePath());
		CommonTestUtils.setEnv(systemEnv);

		LOG.info("-------------------------------------------------------------------");
		LOG.info("Test Delegation Token: {}", testDelegationToken);
		LOG.info("Test Delegation Token sysEnv: {}", systemEnv);
		LOG.info("-------------------------------------------------------------------");
	}

	private static void obtainDelegationToken(File dtFile) throws Exception {
		final Text hdfsDelegationTokenKind = new Text("HDFS_DELEGATION_TOKEN");
		final Text service = new Text("test-service");
		Credentials cred = new Credentials();
		final Text amRmTokenKind = new Text("YARN_AM_RM_TOKEN");
		cred.addToken(amRmTokenKind, new Token<>(new byte[4], new byte[4], amRmTokenKind, service));
		cred.addToken(hdfsDelegationTokenKind, new Token<>(new byte[4], new byte[4],
			hdfsDelegationTokenKind, service));
		DataOutputStream os = new DataOutputStream(new FileOutputStream(dtFile));
		cred.writeTokenStorageToStream(os);
	}
}
