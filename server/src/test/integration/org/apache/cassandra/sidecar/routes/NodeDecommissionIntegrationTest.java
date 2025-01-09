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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
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
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.testing.BootstrapBBUtils;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

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
    private static final String DECOMMISSION_FAILED_MESSAGE = "Failed to decommission node";

    @CassandraIntegrationTest(nodesPerDc = 2)
    void decommissionNodeDefault(VertxTestContext context) throws InterruptedException
    {
        BBHelperDecommissionNode.reset();
        final String[] jobId = new String[1];
        String testRoute = "/api/v1/cassandra/operations/decommission?force=true";
        testWithClient(client -> client.put(server.actualPort(), "127.0.0.1", testRoute)
                                       .send(context.succeeding(response -> {
                                           logger.info("Response Status:" + response.statusCode());
                                           OperationalJobResponse decommissionResponse = response.bodyAsJson(OperationalJobResponse.class);
                                           assertThat(decommissionResponse.status()).isEqualTo(RUNNING);
                                           jobId[0] = String.valueOf(decommissionResponse.jobId());
                                       })));
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        pollStatusForState(jobId[0], SUCCEEDED, null);
        context.completeNow();
        context.awaitCompletion(2, TimeUnit.MINUTES);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, buildCluster = false)
    void decommissionNodeWithFailure(VertxTestContext context,
                                     ConfigurableCassandraTestContext cassandraTestContext) throws InterruptedException
    {
        BBHelperDecommissionNode.reset();
        cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withInstanceInitializer(BBHelperDecommissionNode::install);
        });

        AtomicReference<String> jobId = new AtomicReference<>();
        String testRoute = "/api/v1/cassandra/operations/decommission";
        testWithClient(client -> client.put(server.actualPort(), "127.0.0.1", testRoute)
                                       .send(context.succeeding(response -> {
                                           logger.info("Response Status:" + response.statusCode());
                                           OperationalJobResponse decommissionResponse = response.bodyAsJson(OperationalJobResponse.class);
                                           assertThat(decommissionResponse.status()).isEqualTo(RUNNING);
                                           jobId.set(String.valueOf(decommissionResponse.jobId()));
                                       })));
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        pollStatusForState(jobId.get(), FAILED, DECOMMISSION_FAILED_MESSAGE);
        context.completeNow();
        context.awaitCompletion(2, TimeUnit.MINUTES);
    }

    private void pollStatusForState(String uuid,
                                    OperationalJobStatus expectedStatus,
                                    String expectedReason)
    {
        int attempts = 10;
        String status = "/api/v1/cassandra/operational-jobs/" + uuid;
        AtomicBoolean stateReached = new AtomicBoolean(false);
        logger.info("Job Stats Attempt: {}", attempts);
        AtomicInteger counter = new AtomicInteger(0);
        loopAssert(30, () -> {
            counter.incrementAndGet();
            // todo: create a helper method in the base class to get response in the blocking manner
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

    /**
     * ByteBuddy helper to simulate decommission failure
     */
    public static class BBHelperDecommissionNode
    {
        static CountDownLatch transientStateStart = new CountDownLatch(1);
        static CountDownLatch transientStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            if (nodeNumber == 1)
            {
                BootstrapBBUtils.installSetBoostrapStateIntercepter(cl, BBHelperDecommissionNode.class);
            }
        }

        public static void setBootstrapState(SystemKeyspace.BootstrapState state, @SuperCall Callable<Void> orig) throws Exception
        {
            if (state == SystemKeyspace.BootstrapState.DECOMMISSIONED)
            {
                throw new Exception(DECOMMISSION_FAILED_MESSAGE);
            }
            orig.call();
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(1);
            transientStateEnd = new CountDownLatch(1);
        }

    }
}
