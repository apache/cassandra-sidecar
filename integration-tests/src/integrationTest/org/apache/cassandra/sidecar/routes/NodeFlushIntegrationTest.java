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

package org.apache.cassandra.sidecar.routes;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.FAILED;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.SUCCEEDED;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for node flush operation
 */
public class NodeFlushIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    private static final String TEST_KEYSPACE = "testkeyspace";
    public static final String TESTTABLE_1 = "testtable1";
    public static final String TESTTABLE_2 = "testtable2";
    public static final String OPERATION_FLUSH = "flush";
    public static final String LOCALHOST = "localhost";
    public static final String TABLE_NAMES = "tableNames";

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, Map.of("replication_factor", 1));

        createTestTable(new QualifiedName(TEST_KEYSPACE, TESTTABLE_1),
                        "CREATE TABLE %s ( \n" +
                        "  id int PRIMARY KEY, \n" +
                        "  data text \n" +
                        ");");

        createTestTable(new QualifiedName(TEST_KEYSPACE, TESTTABLE_2),
                        "CREATE TABLE %s ( \n" +
                        "  id int PRIMARY KEY, \n" +
                        "  data text \n" +
                        ");");
    }

    /**
     * Tests flushing multiple tables within a keyspace.
     * Verifies the API accepts the request and the flush operation completes successfully.
     */
    @Test
    void testFlushMultipleTables()
    {
        JsonObject requestBody = new JsonObject();
        requestBody.put(TABLE_NAMES, List.of(TESTTABLE_1, TESTTABLE_2));

        String flushRoute = ApiEndpointsV1.NODE_FLUSH_ROUTE.replace(ApiEndpointsV1.KEYSPACE_PATH_PARAM, TEST_KEYSPACE);
        HttpResponse<Buffer> flushResponse = getBlocking(
        trustedClient().post(serverWrapper.serverPort, LOCALHOST, flushRoute)
                       .sendJsonObject(requestBody));

        assertThat(flushResponse.statusCode()).isIn(OK.code(), ACCEPTED.code());

        JsonObject responseBody = flushResponse.bodyAsJsonObject();
        assertThat(responseBody).isNotNull();
        String jobId = responseBody.getString("jobId");
        assertThat(jobId).isNotNull();
        assertThat(responseBody.getString("operation")).isEqualTo(OPERATION_FLUSH);

        // loopAssert that the job status is eventually SUCCEEDED
        validateOperationalJobStatusEventually(jobId, OPERATION_FLUSH, SUCCEEDED);
    }

    /**
     * Tests flushing a single table within a keyspace.
     * Verifies the API accepts the request and the flush operation completes successfully.
     */
    @Test
    void testFlushSingleTable()
    {
        JsonObject requestBody = new JsonObject();
        requestBody.put(TABLE_NAMES, List.of(TESTTABLE_1));

        String flushRoute = ApiEndpointsV1.NODE_FLUSH_ROUTE.replace(ApiEndpointsV1.KEYSPACE_PATH_PARAM, TEST_KEYSPACE);
        HttpResponse<Buffer> flushResponse = getBlocking(
        trustedClient().post(serverWrapper.serverPort, LOCALHOST, flushRoute)
                       .sendJsonObject(requestBody));

        assertThat(flushResponse.statusCode()).isIn(OK.code(), ACCEPTED.code());

        JsonObject responseBody = flushResponse.bodyAsJsonObject();
        assertThat(responseBody).isNotNull();
        String jobId = responseBody.getString("jobId");
        assertThat(jobId).isNotNull();
        assertThat(responseBody.getString("operation")).isEqualTo(OPERATION_FLUSH);

        // loopAssert that the job status is eventually SUCCEEDED
        validateOperationalJobStatusEventually(jobId, OPERATION_FLUSH, SUCCEEDED);
    }

    /**
     * Tests flushing an entire keyspace by providing an empty table list.
     * Verifies the API accepts the request and flushes all tables in the keyspace successfully.
     */
    @Test
    void testFlushEmptyTableList()
    {
        JsonObject requestBody = new JsonObject();
        requestBody.put(TABLE_NAMES, Collections.emptyList());

        String flushRoute = ApiEndpointsV1.NODE_FLUSH_ROUTE.replace(ApiEndpointsV1.KEYSPACE_PATH_PARAM, TEST_KEYSPACE);
        HttpResponse<Buffer> flushResponse = getBlocking(
        trustedClient().post(serverWrapper.serverPort, LOCALHOST, flushRoute)
                       .sendJsonObject(requestBody));

        assertThat(flushResponse.statusCode()).isIn(OK.code(), ACCEPTED.code());

        JsonObject responseBody = flushResponse.bodyAsJsonObject();
        assertThat(responseBody).isNotNull();
        String jobId = responseBody.getString("jobId");
        assertThat(jobId).isNotNull();
        assertThat(responseBody.getString("operation")).isEqualTo(OPERATION_FLUSH);

        // loopAssert that the job status is eventually SUCCEEDED
        validateOperationalJobStatusEventually(jobId, OPERATION_FLUSH, SUCCEEDED);
    }

    /**
     * Tests flushing an entire keyspace by providing no payload.
     * Verifies the API accepts the request and flushes all tables in the keyspace successfully.
     */
    @Test
    void testFlushNoPayload()
    {
        String flushRoute = ApiEndpointsV1.NODE_FLUSH_ROUTE.replace(ApiEndpointsV1.KEYSPACE_PATH_PARAM, TEST_KEYSPACE);
        HttpResponse<Buffer> flushResponse = getBlocking(
        trustedClient().post(serverWrapper.serverPort, LOCALHOST, flushRoute)
                       .send());

        assertThat(flushResponse.statusCode()).isIn(OK.code(), ACCEPTED.code());

        JsonObject responseBody = flushResponse.bodyAsJsonObject();
        assertThat(responseBody).isNotNull();
        String jobId = responseBody.getString("jobId");
        assertThat(jobId).isNotNull();
        assertThat(responseBody.getString("operation")).isEqualTo(OPERATION_FLUSH);

        // loopAssert that the job status is eventually SUCCEEDED
        validateOperationalJobStatusEventually(jobId, OPERATION_FLUSH, SUCCEEDED);
    }

    /**
     * Tests flushing a non-existent keyspace.
     * Verifies the API accepts the request but the flush operation ultimately fails.
     */
    @Test
    void testFlushNonExistentKeyspace()
    {
        JsonObject requestBody = new JsonObject();
        requestBody.put(TABLE_NAMES, List.of(TESTTABLE_1));

        String flushRoute = ApiEndpointsV1.NODE_FLUSH_ROUTE.replace(ApiEndpointsV1.KEYSPACE_PATH_PARAM, "garbagekeyspace");
        HttpResponse<Buffer> flushResponse = getBlocking(
        trustedClient().post(serverWrapper.serverPort, LOCALHOST, flushRoute)
                       .sendJsonObject(requestBody));

        assertThat(flushResponse.statusCode()).isIn(OK.code(), ACCEPTED.code());

        JsonObject responseBody = flushResponse.bodyAsJsonObject();
        assertThat(responseBody).isNotNull();
        String jobId = responseBody.getString("jobId");
        assertThat(jobId).isNotNull();
        assertThat(responseBody.getString("operation")).isEqualTo(OPERATION_FLUSH);

        // loopAssert that the job status is eventually FAILED
        validateOperationalJobStatusEventually(jobId, OPERATION_FLUSH, FAILED);
    }

    /**
     * Tests flushing a non-existent table within a valid keyspace.
     * Verifies the API accepts the request but the flush operation ultimately fails.
     */
    @Test
    void testFlushNonExistentTable()
    {
        JsonObject requestBody = new JsonObject();
        requestBody.put(TABLE_NAMES, List.of("garbagetesttable"));

        String flushRoute = ApiEndpointsV1.NODE_FLUSH_ROUTE.replace(ApiEndpointsV1.KEYSPACE_PATH_PARAM, TEST_KEYSPACE);
        HttpResponse<Buffer> flushResponse = getBlocking(
        trustedClient().post(serverWrapper.serverPort, LOCALHOST, flushRoute)
                       .sendJsonObject(requestBody));

        assertThat(flushResponse.statusCode()).isIn(OK.code(), ACCEPTED.code());

        JsonObject responseBody = flushResponse.bodyAsJsonObject();
        assertThat(responseBody).isNotNull();
        String jobId = responseBody.getString("jobId");
        assertThat(jobId).isNotNull();
        assertThat(responseBody.getString("operation")).isEqualTo(OPERATION_FLUSH);

        // loopAssert that the job status is eventually FAILED
        validateOperationalJobStatusEventually(jobId, OPERATION_FLUSH, FAILED);
    }

    /**
     * Tests sending a malformed JSON payload to the flush API.
     * Verifies the API immediately rejects the request with a bad request error.
     */
    @Test
    void testFlushMalformedPayload()
    {
        String malformedJson = "{ invalid json }";

        String flushRoute = ApiEndpointsV1.NODE_FLUSH_ROUTE.replace(ApiEndpointsV1.KEYSPACE_PATH_PARAM, TEST_KEYSPACE);
        HttpResponse<Buffer> flushResponse = getBlocking(
        trustedClient().post(serverWrapper.serverPort, LOCALHOST, flushRoute)
                       .sendBuffer(Buffer.buffer(malformedJson)));

        // Validate failure response
        assertThat(flushResponse.statusCode()).isEqualTo(BAD_REQUEST.code());
    }

    /**
     * Tests attempting to flush the system keyspace.
     * Verifies the API rejects the request as system keyspace operations are forbidden.
     */
    @Test
    void testFlushSystemKeyspaceFailure()
    {
        String flushRoute = ApiEndpointsV1.NODE_FLUSH_ROUTE.replace(ApiEndpointsV1.KEYSPACE_PATH_PARAM, "system");
        HttpResponse<Buffer> flushResponse = getBlocking(
        trustedClient().post(serverWrapper.serverPort, LOCALHOST, flushRoute)
                       .send());

        // Validate failure response
        assertThat(flushResponse.statusCode()).isEqualTo(FORBIDDEN.code());
    }

    /**
     * Validates that the operational job status eventually reaches the expected status
     *
     * @param jobId             the ID of the operational job to validate
     * @param expectedOperation the expected operation name (e.g., "flush", "decommission", "drain")
     * @param expectedStatus    the expected final status (SUCCEEDED or FAILED)
     */
    private void validateOperationalJobStatusEventually(String jobId, String expectedOperation, OperationalJobStatus expectedStatus)
    {
        String operationalJobRoute = ApiEndpointsV1.OPERATIONAL_JOB_ROUTE.replace(":operationId", jobId);

        loopAssert(30, 500, () -> {
            HttpResponse<Buffer> jobStatusResponse = getBlocking(
            trustedClient().get(serverWrapper.serverPort, LOCALHOST, operationalJobRoute)
                           .send());

            assertThat(jobStatusResponse.statusCode()).isEqualTo(OK.code());

            JsonObject jobStatusBody = jobStatusResponse.bodyAsJsonObject();
            assertThat(jobStatusBody).isNotNull();
            assertThat(jobStatusBody.getString("jobId")).isEqualTo(jobId);
            assertThat(jobStatusBody.getString("operation")).isEqualTo(expectedOperation);
            assertThat(jobStatusBody.getString("jobStatus")).isEqualTo(expectedStatus.name());
        });
    }
}
