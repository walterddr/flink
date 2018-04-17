#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

set -Eeuo pipefail

source "$(dirname "$0")"/common.sh

TEST_PROGRAM_JAR=$END_TO_END_DIR/flink-cli-test/target/PeriodicStreamingJob.jar
TEST_EXEC_JAR=$END_TO_END_DIR/flink-rest-api-test/target/RestApiTest.jar

start_cluster

EXIT_CODE=0

printf "\n==============================================================================\n"
printf "Start periodic streaming job to test against\n"
JOB_ID=""
if [ $EXIT_CODE == 0 ]; then
    RETURN=`$FLINK_DIR/bin/flink run -d $TEST_PROGRAM_JAR --outputPath file://${TEST_DATA_DIR}/out/result`
    echo "$RETURN"
    JOB_ID=`extract_job_id_from_job_submission_return "$RETURN"`
    EXIT_CODE=$? # expect matching job id extraction
fi
printf "Periodic streaming job started!\n"
printf "==============================================================================\n"

printf "\n==============================================================================\n"
printf "Waiting for the job ID to be queryable and checkpoint has been finished.\n"
wait_for_job_state_transition $JOB_ID "CREATED" "RUNNING"
# wait for the checkpoint interval to reach (set to 5 sec)
sleep 5s
printf "Job_ID $JOB_ID queryable!\n"
printf "==============================================================================\n"

printf "\n==============================================================================\n"
printf "Starting the REST API test job and go through all REST APIs.\n"
$FLINK_DIR/bin/flink run $TEST_EXEC_JAR -checkpointPath file://${TEST_DATA_DIR}/out/checkpoint
EXIT_CODE=$?
printf "REST API test program finished with exit code: $EXIT_CODE !\n"
printf "==============================================================================\n"

stop_cluster

if [ $EXIT_CODE == 0 ];
    then
        echo "REST API test passed!";
    else
        echo "REST API test failed: $EXIT_CODE";
        PASS=""
        exit 1
fi
