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

package org.apache.cassandra.sidecar.lifecycle;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Utility class to test different lifecycle provider implementations.
 */
public class LifecycleProviderIntegrationTester
{
    protected final Logger logger = LoggerFactory.getLogger(LifecycleProviderIntegrationTester.class);

    static final int TIMEOUT_SECONDS = 120;

    final WebClient client;
    final String sidecarHost;
    final int sidecarPort;
    final Runnable cassandraNodeCrasher;

    public LifecycleProviderIntegrationTester(WebClient client, String sidecarHost, int sidecarPort, Runnable cassandraNodeCrasher)
    {
        this.client = client;
        this.sidecarHost = sidecarHost;
        this.sidecarPort = sidecarPort;
        this.cassandraNodeCrasher = cassandraNodeCrasher;
    }

    void testLifecycleProviderStartAndStopAndRecoveryAfterCrash() throws Exception
    {
        // Check CQL health is NOT_OK, since node is not started
        assertThat(getCqlStatus()).isEqualTo("NOT_OK");

        // GET /lifecycle when node has not started and no intent has been submitted
        HttpResponse<Buffer> lifecycleInfoOnStartup = getLifecycle();
        assertExpectedLifecycleResponse(lifecycleInfoOnStartup, OK.code(),
                                        "STOPPED", "UNKNOWN", "UNDEFINED",
                                        "No lifecycle task submitted for this instance yet.");

        // Start node via PUT /lifecycle and wait for node to be up
        HttpResponse<Buffer> lifecycleUpdateStart = putLifecycle("start");
        assertExpectedLifecycleResponse(lifecycleUpdateStart, ACCEPTED.code(),
                                        "STOPPED", "RUNNING", "CONVERGING", "Submitting start task for instance");

        waitForLastUpdateToConverge("Instance has started", TIMEOUT_SECONDS);
        waitForCqlStatus("OK", TIMEOUT_SECONDS);

        // GET /lifecycle after node has started up
        HttpResponse<Buffer> lifecycleInfoAfterStartup = getLifecycle();
        assertExpectedLifecycleResponse(lifecycleInfoAfterStartup, OK.code(),
                                        "RUNNING", "RUNNING", "CONVERGED", "Instance has started");

        // Try starting node with PUT /lifecycle again
        HttpResponse<Buffer> lifecycleInfoAfterStartAgain = putLifecycle("start");
        assertExpectedLifecycleResponse(lifecycleInfoAfterStartAgain, OK.code(),
                                        "RUNNING", "RUNNING", "CONVERGED", "Instance has started");

        // Simulate node crashing by directly calling the lifecycle provider's stop method
        cassandraNodeCrasher.run();
        waitForLastUpdateToConverge(String.format("Instance %s has unexpectedly diverged from the desired state RUNNING to STOPPED.", sidecarHost), TIMEOUT_SECONDS);

        // CQL status should be down since instance is crashed
        waitForCqlStatus("NOT_OK", TIMEOUT_SECONDS);

        // GET /lifecycle after node has crashed
        HttpResponse<Buffer> lifecycleInfoAfterCrash = getLifecycle();
        assertExpectedLifecycleResponse(lifecycleInfoAfterCrash, OK.code(),
                                        "STOPPED", "RUNNING", "DIVERGED",
                                        String.format("Instance %s has unexpectedly diverged from the desired state RUNNING to STOPPED.", sidecarHost));

        // Try starting node with PUT /lifecycle again
        HttpResponse<Buffer> lifecycleInfoOnStartAfterCrash = putLifecycle("start");
        assertExpectedLifecycleResponse(lifecycleInfoOnStartAfterCrash, ACCEPTED.code(),
                                        "STOPPED", "RUNNING", "CONVERGING", "Submitting start task for instance");
        waitForLastUpdateToConverge("Instance has started", TIMEOUT_SECONDS);
        waitForCqlStatus("OK", TIMEOUT_SECONDS);

        // Confirm that the node is up again with GET /lifecycle
        HttpResponse<Buffer> lifecycleInfoAfterRecovery = getLifecycle();
        assertExpectedLifecycleResponse(lifecycleInfoAfterRecovery, OK.code(),
                                        "RUNNING", "RUNNING", "CONVERGED", "Instance has started");

        // Stop node via PUT /lifecycle
        HttpResponse<Buffer> lifecycleUpdateStop = putLifecycle("stop");
        assertExpectedLifecycleResponse(lifecycleUpdateStop, ACCEPTED.code(),
                                        "RUNNING", "STOPPED", "CONVERGING", "Submitting stop task for instance");

        // Allow time for the async stop task to complete
        waitForLastUpdateToConverge("Instance has stopped", TIMEOUT_SECONDS);

        // GET /lifecycle after node has stopped
        HttpResponse<Buffer> lifecycleInfoAfterStop = getLifecycle();
        assertExpectedLifecycleResponse(lifecycleInfoAfterStop, OK.code(),
                                        "STOPPED", "STOPPED", "CONVERGED", "Instance has stopped");

        // CQL status should be down since instance is stopped
        assertThat(getCqlStatus()).isEqualTo("NOT_OK");

        // Try stopping node with PUT /lifecycle again, should be idempotent
        HttpResponse<Buffer> lifecycleInfoOnStopAgain = putLifecycle("stop");
        assertExpectedLifecycleResponse(lifecycleInfoOnStopAgain, OK.code(),
                                        "STOPPED", "STOPPED", "CONVERGED", "Instance has stopped");
    }

    private void assertExpectedLifecycleResponse(
    HttpResponse<Buffer> response,
    int expectedStatusCode,
    String currentState,
    String desiredState,
    String status,
    String lastUpdate)
    {
        assertThat(response.statusCode()).isEqualTo(expectedStatusCode);
        JsonObject body = response.bodyAsJsonObject();
        assertThat(body.getString("current_state")).isEqualTo(currentState);
        assertThat(body.getString("desired_state")).isEqualTo(desiredState);
        assertThat(body.getString("status")).isEqualTo(status);
        assertThat(body.getString("last_update")).isEqualTo(lastUpdate);
    }

    private HttpResponse<Buffer> getLifecycle()
    {
        return getBlocking(client
                           .get(sidecarPort, sidecarHost, "/api/v1/cassandra/lifecycle")
                           .send());
    }

    private HttpResponse<Buffer> putLifecycle(String desiredState)
    {
        return getBlocking(client
                           .put(sidecarPort, sidecarHost, "/api/v1/cassandra/lifecycle")
                           .sendBuffer(JsonObject.of("state", desiredState).toBuffer()));
    }

    public void waitForCqlStatus(String expectedStatus, int timeoutSeconds) throws TimeoutException
    {
        long startTime = System.nanoTime();
        String cqlStatus = getCqlStatus();
        while (!cqlStatus.equals(expectedStatus))
        {
            if (System.nanoTime() - startTime > TimeUnit.SECONDS.toNanos(timeoutSeconds))
            {
                throw new TimeoutException("Expected CQL status not reached after " + timeoutSeconds + " seconds. " +
                                           ", status: " + cqlStatus);
            }
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            cqlStatus = getCqlStatus();
        }
    }

    private String getCqlStatus()
    {
        HttpResponse<Buffer> cqlStatus = getBlocking(
        client
        .get(sidecarPort, sidecarHost, "/api/v1/cassandra/native/__health")
        .send());
        logger.info("Native health response: {}", cqlStatus.bodyAsString());

        return cqlStatus.bodyAsJsonObject().getString("status");
    }

    private void waitForLastUpdateToConverge(String expectedLastUpdate, int timeoutSeconds) throws TimeoutException
    {
        long startTime = System.nanoTime();
        String lastUpdate = getLifecycle().bodyAsJsonObject().getString("last_update");
        while (!lastUpdate.equals(expectedLastUpdate))
        {
            if (System.nanoTime() - startTime > TimeUnit.SECONDS.toNanos(timeoutSeconds))
            {
                throw new TimeoutException("Expected lifecycle update not reached after " + timeoutSeconds + " seconds. " +
                                           ", last_update: " + lastUpdate);
            }
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            lastUpdate = getLifecycle().bodyAsJsonObject().getString("last_update");
        }
    }

}
