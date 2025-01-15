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

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.apache.cassandra.sidecar.AssertionUtils.loopAssert;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.FAILED;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.RUNNING;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.SUCCEEDED;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Test the node decommission endpoint with cassandra container.
 */
@ExtendWith(VertxExtension.class)
public class NodeDecommissionIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest(nodesPerDc = 2)
    void decommissionNodeDefault(VertxTestContext context)
    {
        final AtomicReference<String> jobId = new AtomicReference<>();
        String testRoute = "/api/v1/cassandra/operations/decommission?force=true";
        testWithClient(client -> client.put(server.actualPort(), "127.0.0.1", testRoute)
                                       .send(context.succeeding(response -> {
                                           OperationalJobResponse decommissionResponse = response.bodyAsJson(OperationalJobResponse.class);
                                           assertThat(decommissionResponse.status()).isEqualTo(RUNNING);
                                           jobId.set(String.valueOf(decommissionResponse.jobId()));
                                       })));
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        assertThat(jobId.get()).isNotNull();
        pollStatusForState(jobId.get(), SUCCEEDED, null);
        context.completeNow();
    }

    @CassandraIntegrationTest(nodesPerDc = 2)
    void decommissionNodeWithFailure(VertxTestContext context)
    {
        String testRoute = "/api/v1/cassandra/operations/decommission";
        testWithClient(client -> client.put(server.actualPort(), "127.0.0.1", testRoute)
                                       .send(context.succeeding(response -> {
                                           OperationalJobResponse decommissionResponse = response.bodyAsJson(OperationalJobResponse.class);
                                           assertThat(decommissionResponse.status()).isEqualTo(FAILED);
                                           assertThat(decommissionResponse.jobId()).isNotNull();
                                           String jobId = String.valueOf(decommissionResponse.jobId());
                                           assertThat(jobId).isNotNull();
                                           context.completeNow();
                                       })));

    }

    private void pollStatusForState(String uuid,
                                    OperationalJobStatus expectedStatus,
                                    String expectedReason)
    {
        String status = "/api/v1/cassandra/operational-jobs/" + uuid;
        AtomicBoolean stateReached = new AtomicBoolean(false);
        AtomicInteger counter = new AtomicInteger(0);
        loopAssert(30, () -> {
            counter.incrementAndGet();
            // TODO: optionally create a helper method in the base class to get response in the blocking manner
            HttpResponse<Buffer> resp;
            try
            {
                resp = client.get(server.actualPort(), "127.0.0.1", status)
                                                  .send()
                                                  .toCompletionStage()
                                                  .toCompletableFuture()
                                                  .get();
                logger.info("Success Status Response code: {}", resp.statusCode());
                logger.info("Status Response: {}", resp.bodyAsString());
                if (resp.statusCode() == HttpResponseStatus.OK.code())
                {
                    stateReached.set(true);
                    OperationalJobResponse jobStatusResp = resp.bodyAsJson(OperationalJobResponse.class);
                    assertThat(jobStatusResp.jobId()).isEqualTo(UUID.fromString(uuid));
                    assertThat(jobStatusResp.status()).isEqualTo(expectedStatus);
                    assertThat(jobStatusResp.reason()).isEqualTo(expectedReason);
                    assertThat(jobStatusResp.operation()).isEqualTo("decommission");
                }
                else
                {
                    assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                    OperationalJobResponse jobStatusResp = resp.bodyAsJson(OperationalJobResponse.class);
                    assertThat(jobStatusResp.jobId()).isEqualTo(UUID.fromString(uuid));
                }
                logger.info("Request completed");
                assertThat(stateReached.get()).isTrue();
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new RuntimeException(e);
            }
        });
    }
}
