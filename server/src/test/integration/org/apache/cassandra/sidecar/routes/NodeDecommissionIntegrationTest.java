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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.NodeDecommissionResponse;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.testing.BootstrapBBUtils;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

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

    @CassandraIntegrationTest(nodesPerDc = 5)
    void decommissionNodeDefault(VertxTestContext context)
    {
        final String[] jobId = new String[1];
        String testRoute = "/api/v1/cassandra/node/decommission";
        testWithClient(client -> client.put(server.actualPort(), "127.0.0.1", testRoute)
                                       .send(context.succeeding(response -> {
                                           logger.info("Response Status:" + response.statusCode());
                                           NodeDecommissionResponse decommissionResponse = response.bodyAsJson(NodeDecommissionResponse.class);
                                           assertThat(decommissionResponse.status()).isEqualTo(RUNNING);
                                           jobId[0] = String.valueOf(decommissionResponse.jobId());
                                       })));
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        pollStatusForState(context, jobId[0], SUCCEEDED, null);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, buildCluster = false)
    void decommissionNodeWithFailure(VertxTestContext context,
                                     ConfigurableCassandraTestContext cassandraTestContext) throws InterruptedException
    {
        BBHelperDecommissionNode.reset();
        cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withInstanceInitializer(BBHelperDecommissionNode::install);
        });

        final String[] jobId = new String[1];
        String testRoute = "/api/v1/cassandra/node/decommission";
        testWithClient(client -> client.put(server.actualPort(), "127.0.0.1", testRoute)
                                       .send(context.succeeding(response -> {
                                           logger.info("Response Status:" + response.statusCode());
                                           NodeDecommissionResponse decommissionResponse = response.bodyAsJson(NodeDecommissionResponse.class);
                                           assertThat(decommissionResponse.status()).isEqualTo(RUNNING);
                                           jobId[0] = String.valueOf(decommissionResponse.jobId());
                                       })));
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        pollStatusForState(context, jobId[0], FAILED, DECOMMISSION_FAILED_MESSAGE);
    }

    private void pollStatusForState(VertxTestContext context,
                                    String uuid,
                                    OperationalJobStatus expectedStatus,
                                    String expectedReason)
    {

        int attempts = 10;
        String status = "/api/v1/cassandra/operational-jobs/" + uuid;
        AtomicBoolean stateReached = new AtomicBoolean(false);
        logger.info("Job Stats Attempt:" + attempts);
        final int[] counter = {0};
        int finalAttempts = attempts;
        vertx.setPeriodic(10000, id -> {
            counter[0]++;
            testWithClient(false, client -> client.get(server.actualPort(), "127.0.0.1", status)
                                                  .send(context.succeeding(resp -> {
                                                      if (resp.statusCode() == HttpResponseStatus.OK.code())
                                                      {
                                                          stateReached.set(true);
                                                          logger.info("Success Status Response code:" + resp.statusCode());
                                                          logger.info("Status Response:" + resp.bodyAsString());
                                                          OperationalJobResponse jobStatusResp = resp.bodyAsJson(OperationalJobResponse.class);
                                                          assertThat(jobStatusResp.jobId()).isEqualTo(UUID.fromString(uuid));
                                                          assertThat(jobStatusResp.status()).isEqualTo(expectedStatus);
                                                          assertThat(jobStatusResp.reason()).isEqualTo(expectedReason);
                                                          assertThat(jobStatusResp.operation()).isEqualTo("decommission");
                                                      }
                                                      else
                                                      {
                                                          assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                                                          logger.info("Status Response code:" + resp.statusCode());
                                                          logger.info("Status Response:" + resp.bodyAsString());
                                                          OperationalJobResponse jobStatusResp = resp.bodyAsJson(OperationalJobResponse.class);
                                                          assertThat(jobStatusResp.jobId()).isEqualTo(UUID.fromString(uuid));
                                                          // assertThat(jobStatusResp).isNull();
                                                      }
                                                      logger.info("Request completed");
                                                  })));
            if (stateReached.get() || counter[0] == finalAttempts)
            {
                vertx.cancelTimer(id);
                assertThat(stateReached.get()).isTrue();
                context.completeNow();
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
