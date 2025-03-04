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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.extension.ExtendWith;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.request.data.AllServicesConfigPayload;
import org.apache.cassandra.sidecar.common.request.data.UpdateCdcServiceConfigPayload;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests CDC config APIs like adding, updating,deleting and getting configs to/from
 * "configs" table
 */
@ExtendWith(VertxExtension.class)
public class CdcConfigApisTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void testConfigOperations(VertxTestContext context) throws Exception
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
        String configRoute = "/api/v1/services/cdc/config";

        // Create new configs
        UpdateCdcServiceConfigPayload payload = new UpdateCdcServiceConfigPayload(Map.of("k1", "v1"));
        UpdateCdcServiceConfigPayload newConfigResponse = client.put(server.actualPort(), "127.0.0.1", configRoute)
                .as(BodyCodec.json(UpdateCdcServiceConfigPayload.class))
                .sendJson(JsonObject.mapFrom(payload))
                .toCompletionStage()
                .toCompletableFuture()
                .get()
                .body();
        assertEquals(payload, newConfigResponse);

        // update configs
        UpdateCdcServiceConfigPayload updatedPayload = new UpdateCdcServiceConfigPayload(Map.of("k3", "v3"));
        UpdateCdcServiceConfigPayload updatedConfigResponse = client.put(server.actualPort(), "127.0.0.1", configRoute)
                .as(BodyCodec.json(UpdateCdcServiceConfigPayload.class))
                .sendJson(JsonObject.mapFrom(updatedPayload))
                .toCompletionStage()
                .toCompletableFuture()
                .get().body();
        assertEquals(updatedPayload, updatedConfigResponse);

        // GetConfigs should give updated configs
        String getConfigsRoute = ApiEndpointsV1.SERVICES_CONFIG_ROUTE;
        AllServicesConfigPayload getServicesResponse = client.get(server.actualPort(), "127.0.0.1", getConfigsRoute)
                .as(BodyCodec.json(AllServicesConfigPayload.class))
                .sendJson(JsonObject.mapFrom(updatedPayload))
                .toCompletionStage()
                .toCompletableFuture()
                .get()
                .body();
        List<AllServicesConfigPayload.Service> services = List.of(
                new AllServicesConfigPayload.Service("kafka", Map.of()),
                new AllServicesConfigPayload.Service("cdc", updatedPayload.config()));
        AllServicesConfigPayload expectedConfigPayload = new AllServicesConfigPayload(services);
        assertEquals(expectedConfigPayload, getServicesResponse);

        // delete all CDC configs
        client.delete(server.actualPort(), "127.0.0.1", configRoute)
                .send()
                .toCompletionStage()
                .toCompletableFuture()
                .get()
                .body();

        // Get configs should have no configs
        client.get(server.actualPort(), "127.0.0.1", getConfigsRoute)
                .as(BodyCodec.json(AllServicesConfigPayload.class))
                .sendJson(JsonObject.mapFrom(updatedPayload))
                .onSuccess(resp -> {
                    AllServicesConfigPayload response = resp.body();
                    response.services()
                            .forEach(service -> {
                                if (!service.config.isEmpty())
                                {
                                    context.failNow(new RuntimeException("Configs for all services should be empty"));
                                }
                            });
                    context.completeNow();
                })
                .onFailure(res -> context.failNow(res.getCause()));
    }

    @CassandraIntegrationTest
    void testConfigOperationWithInvalidService(VertxTestContext context) throws Exception
    {
        waitForSchemaReady(30, TimeUnit.SECONDS);
        String configRoute = "/api/v1/services/invalid/config";

        // Update with Invalid service
        Map<String, String> configs = Map.of("k1", "v1");
        UpdateCdcServiceConfigPayload payload = new UpdateCdcServiceConfigPayload(configs);
        testWithClient(context, client -> {
            client.put(server.actualPort(), "127.0.0.1", configRoute)
                    .sendJson(JsonObject.mapFrom(payload), context.succeeding(response -> {
                        assertEquals(response.statusCode(), HttpResponseStatus.NOT_FOUND.code());
                        context.completeNow();
                    }));
        });
    }
}
