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

package org.apache.cassandra.sidecar.common;

import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_READY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test CQLSessionProvider in a variety of cluster states
 */
@ExtendWith(VertxExtension.class)
public class CQLSessionProviderTest extends IntegrationTestBase
{

    public static final String OK_KEYSPACE_RESPONSE_START = "{\"schema\":\"CREATE KEYSPACE ";
    public static final String KEYSPACE_FAILED_RESPONSE_START = "{\"status\":\"Service Unavailable\",";

    @CassandraIntegrationTest(nodesPerDc = 2, startCluster = false)
    void testCqlSessionProviderWorksAsExpected(VertxTestContext context, CassandraTestContext cassandraTestContext)
    throws Exception
    {
        UpgradeableCluster cluster = cassandraTestContext.cluster();
        testWithClient(context, false, webClient -> {
                           // To start, both instances are stopped, so we should get 503s for both
                           buildInstanceHealthRequest(webClient, "1")
                           .send()
                           .onSuccess(response -> assertHealthCheckFailed(response, context))
                           .compose(_ignored ->
                                    buildInstanceHealthRequest(webClient, "2")
                                    .send()
                                    .onSuccess(response -> assertHealthCheckFailed(response, context)))
                           .compose(_ignored ->
                                    buildKeyspaceRequest(webClient)
                                    .send()
                                    // With no instances available in the cluster, keyspace requests should fail
                                    .onSuccess(response -> assertKeyspaceFailed(response, context)))
                           .compose(_ignored -> {
                               // Start instance 1 and check both again
                               return Future.future(promise -> {
                                   vertx.eventBus()
                                        .localConsumer(ON_CASSANDRA_CQL_READY.address(),
                                                       (Message<JsonObject> message) -> {
                                                           if (message.body().getInteger("cassandraInstanceId") == 1)
                                                           {
                                                               promise.complete();
                                                           }
                                                       });
                                   cluster.get(1).startup();
                               });
                           })
                           .compose(_ignored ->
                                    buildInstanceHealthRequest(webClient, "1")
                                    .send()
                                    .onSuccess(response -> assertHealthCheckOk(response, context)))
                           .compose(_ignored ->
                                    buildInstanceHealthRequest(webClient, "2")
                                    .send()
                                    .onSuccess(response -> assertHealthCheckFailed(response, context))
                           )
                           .compose(_ignored ->
                                    // Even with only 1 instance connected/up, we should still have keyspace metadata
                                    buildKeyspaceRequest(webClient)
                                    .send()
                                    .onSuccess(response -> assertKeyspaceOk(response, context)))
                           .compose(_ignored -> {
                               // Start instance 2 and check both again
                               return Future.future(promise -> {
                                   vertx.eventBus()
                                        .localConsumer(ON_CASSANDRA_CQL_READY.address(),
                                                       (Message<JsonObject> message) -> {
                                                           if (message.body().getInteger("cassandraInstanceId") == 2)
                                                           {
                                                               promise.complete();
                                                           }
                                                       });
                                   cluster.get(2).startup();
                               });
                           })
                           .compose(_ignored ->
                                    buildInstanceHealthRequest(webClient, "1")
                                    .send()
                                    .onSuccess(response -> assertHealthCheckOk(response, context)))
                           .compose(_ignored ->
                                    buildInstanceHealthRequest(webClient, "2")
                                    .send()
                                    .onSuccess(response -> assertHealthCheckOk(response, context))
                           )
                           .onSuccess(_ignored -> context.completeNow())
                           .onFailure(context::failNow);
                       }
        );
    }

    @CassandraIntegrationTest(nodesPerDc = 2, newNodesPerDc = 1)
    public void testChangingClusterSize(VertxTestContext context, CassandraTestContext cassandraTestContext)
    throws Exception
    {
        UpgradeableCluster cluster = cassandraTestContext.cluster();
        testWithClient(context, false, webClient -> {
                           waitForInstances(webClient, 1, 2)
                           .compose(_ignored ->
                                    // To start, 2 instances are running and the 3rd is not yet in the cluster.
                                    // Check all three and make sure we've got the right responses
                                    buildInstanceHealthRequest(webClient, "1")
                                    .send()
                                    .onSuccess(response -> assertHealthCheckOk(response, context)))
                           .compose(_ignored ->
                                    buildInstanceHealthRequest(webClient, "2")
                                    .send()
                                    .onSuccess(response -> assertHealthCheckOk(response, context)))
                           .compose(_ignored ->
                                    buildInstanceHealthRequest(webClient, "3")
                                    .send()
                                    .onSuccess(response -> assertHealthCheckFailed(response, context))
                           )
                           .compose(_ignored -> {
                               // Add instance 3 and check both again
                               return Future.future(promise -> {
                                   vertx.eventBus()
                                        .localConsumer(ON_CASSANDRA_CQL_READY.address(),
                                                       (Message<JsonObject> message) -> {
                                                           if (message.body().getInteger("cassandraInstanceId") == 3)
                                                           {
                                                               promise.complete();
                                                           }
                                                       });
                                   IUpgradeableInstance newInstance = ClusterUtils
                                                                      .addInstance(cluster,
                                                                                   cluster.get(1).config(), config -> {
                                                                          config.set("auto_bootstrap", true);
                                                                          config.with(Feature.GOSSIP,
                                                                                      Feature.JMX,
                                                                                      Feature.NATIVE_PROTOCOL);
                                                                      });
                                   newInstance.startup(cluster);
                               });
                           })
                           .compose(_ignored -> waitForInstances(webClient, 1, 2))
                           // Now, all three should be up and reachable
                           .compose(_ignored ->
                                    buildInstanceHealthRequest(webClient, "1")
                                    .send()
                                    .onSuccess(response -> assertHealthCheckOk(response, context)))
                           .compose(_ignored ->
                                    buildInstanceHealthRequest(webClient, "2")
                                    .send()
                                    .onSuccess(response -> assertHealthCheckOk(response, context)))
                           .compose(_ignored ->
                                    buildInstanceHealthRequest(webClient, "3")
                                    .send()
                                    .onSuccess(response -> assertHealthCheckOk(response, context))
                           )
                           .onSuccess(_ignored -> context.completeNow())
                           .onFailure(context::failNow);
                       }
        );
    }

    private Future<Void> waitForInstances(WebClient webClient, int... instanceNums)
    {
        // NOTE: Not using `Future.future` here as this is a long-running operation that seems
        // is run on the vert.x event loop if you just use `Future.future`, which blocks the sidecar from actually
        // functioning in the test
        Promise<Void> promise = Promise.promise();
        new Thread(() -> {
            for (int i : instanceNums)
            {
                int attempts = 0;
                while (attempts < 20)
                {
                    try
                    {
                        HttpResponse<String> resp = buildInstanceHealthRequest(webClient, String.valueOf(i))
                                                    .send()
                                                    .toCompletionStage()
                                                    .toCompletableFuture()
                                                    .get();
                        assertHealthCheckOk(resp);
                        break;
                    }
                    catch (AssertionError | InterruptedException | ExecutionException ex)
                    {
                        attempts++;
                        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                    }
                }
                if (attempts == 20)
                {
                    promise.fail(new RuntimeException("Instance " + i + "failed to become available"));
                    return;
                }
            }
            promise.complete();
        }).start();
        return promise.future();
    }

    private HttpRequest<String> buildInstanceHealthRequest(WebClient webClient, String instanceId)
    {
        return webClient.get(server.actualPort(),
                             "localhost",
                             "/api/v1/cassandra/__health?instanceId=" + instanceId)
                        .as(BodyCodec.string());
    }

    private HttpRequest<String> buildKeyspaceRequest(WebClient webClient)
    {
        return webClient.get(server.actualPort(),
                             "localhost",
                             "/api/v1/schema/keyspaces")
                        .as(BodyCodec.string());
    }

    private void assertHealthCheckOk(HttpResponse<String> response, VertxTestContext context)
    {
        context.verify(() -> {
            assertThat(response.statusCode()).isEqualTo(OK.code());
            assertThat(response.body()).isEqualTo("{\"status\":\"OK\"}");
        });
    }

    private void assertHealthCheckFailed(HttpResponse<String> response, VertxTestContext context)
    {
        context.verify(() -> {
            assertThat(response.statusCode()).isEqualTo(SERVICE_UNAVAILABLE.code());
            assertThat(response.body()).isEqualTo("{\"status\":\"NOT_OK\"}");
        });
    }

    private void assertKeyspaceOk(HttpResponse<String> response, VertxTestContext context)
    {
        context.verify(() -> {
            assertThat(response.statusCode()).isEqualTo(OK.code());
            assertThat(response.body()).startsWith(OK_KEYSPACE_RESPONSE_START);
        });
    }

    private void assertKeyspaceFailed(HttpResponse<String> response, VertxTestContext context)
    {
        context.verify(() -> {
            assertThat(response.statusCode()).isEqualTo(SERVICE_UNAVAILABLE.code());
            assertThat(response.body()).startsWith(KEYSPACE_FAILED_RESPONSE_START);
        });
    }
}
