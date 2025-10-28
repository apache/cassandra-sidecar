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

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Cassandra node drain operations
 */
public class CassandraNodeOperationsIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    public static final String CASSANDRA_VERSION_4_0 = "4.0";

    @Override
    protected void initializeSchemaForTest()
    {
        // No schema init needed
    }

    @Override
    protected void beforeTestStart()
    {
        // wait for the schema initialization
        waitForSchemaReady(30, TimeUnit.SECONDS);
    }

    @Test
    void testNodeDrainOperationSuccess()
    {
        // Initiate drain operation
        HttpResponse<Buffer> drainResponse = getBlocking(
        trustedClient().put(serverWrapper.serverPort, "localhost", ApiEndpointsV1.NODE_DRAIN_ROUTE)
                       .send());

        assertThat(drainResponse.statusCode()).isEqualTo(OK.code());

        JsonObject responseBody = drainResponse.bodyAsJsonObject();
        assertThat(responseBody).isNotNull();
        assertThat(responseBody.getString("jobId")).isNotNull();
        assertThat(responseBody.getString("jobStatus")).isIn(
        OperationalJobStatus.CREATED.name(),
        OperationalJobStatus.RUNNING.name(),
        OperationalJobStatus.SUCCEEDED.name()
        );

        loopAssert(30, 500, () -> {
            // Verify node status is DRAINED by checking the operationMode via stream stats endpoint
            HttpResponse<Buffer> streamStatsResponse = getBlocking(
            trustedClient().get(serverWrapper.serverPort, "localhost", ApiEndpointsV1.STREAM_STATS_ROUTE)
                           .send());

            assertThat(streamStatsResponse.statusCode()).isEqualTo(OK.code());

            JsonObject streamStats = streamStatsResponse.bodyAsJsonObject();
            assertThat(streamStats).isNotNull();
            assertThat(streamStats.getString("operationMode")).isEqualTo("DRAINED");
        });

        // Validate the operational job status using the OperationalJobHandler
        String jobId = responseBody.getString("jobId");
        validateOperationalJobStatus(jobId, "drain");
    }

    /**
     * Validates the operational job status by querying the OperationalJobHandler endpoint
     * and waiting for the job to reach a final state if necessary.
     *
     * @param jobId the ID of the operational job to validate
     * @param expectedOperation the expected operation name (e.g., "move", "decommission", "drain")
     */
    private void validateOperationalJobStatus(String jobId, String expectedOperation)
    {
        String operationalJobRoute = ApiEndpointsV1.OPERATIONAL_JOB_ROUTE.replace(":operationId", jobId);

        HttpResponse<Buffer> jobStatusResponse = getBlocking(
        trustedClient().get(serverWrapper.serverPort, "localhost", operationalJobRoute)
                       .send());

        assertThat(jobStatusResponse.statusCode()).isEqualTo(OK.code());

        JsonObject jobStatusBody = jobStatusResponse.bodyAsJsonObject();
        assertThat(jobStatusBody).isNotNull();
        assertThat(jobStatusBody.getString("jobId")).isEqualTo(jobId);
        assertThat(jobStatusBody.getString("operation")).isEqualTo(expectedOperation);
        assertThat(jobStatusBody.getString("jobStatus")).isIn(
        OperationalJobStatus.RUNNING.name(),
        OperationalJobStatus.SUCCEEDED.name()
        );

        // If the job is still running, wait for it to complete or reach a final state
        if (OperationalJobStatus.RUNNING.name().equals(jobStatusBody.getString("jobStatus")))
        {
            loopAssert(30, 500, () -> {
                HttpResponse<Buffer> finalJobStatusResponse = getBlocking(
                trustedClient().get(serverWrapper.serverPort, "localhost", operationalJobRoute)
                               .send());

                assertThat(finalJobStatusResponse.statusCode()).isEqualTo(OK.code());

                JsonObject finalJobStatusBody = finalJobStatusResponse.bodyAsJsonObject();
                assertThat(finalJobStatusBody).isNotNull();
                assertThat(finalJobStatusBody.getString("jobStatus")).isIn(
                OperationalJobStatus.SUCCEEDED.name(),
                OperationalJobStatus.FAILED.name()
                );
            });
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void tearDown() throws Exception
    {
        try
        {
            super.tearDown();
        }
        catch (IllegalStateException ex)
        {
            logger.error("Exception in tear down", ex);
            // When cluster.close() is called after drain For Cassandra 4.0
            // it throws IllegalStateException "HintsService has already been shut down".
            if (!CASSANDRA_VERSION_4_0.equals(this.testVersion.version()))
            {
                throw ex;
            }
            logger.warn("Suppressing {} for Cassandra version {}",
                        ex.getClass().getCanonicalName(), CASSANDRA_VERSION_4_0);
        }
    }
}
