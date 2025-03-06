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

import org.junit.jupiter.api.Test;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpResponseExpectation;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.request.data.AllServicesConfigPayload;
import org.apache.cassandra.sidecar.common.request.data.UpdateCdcServiceConfigPayload;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests CDC config APIs like adding, updating,deleting and getting configs to/from
 * "configs" table
 */
class CdcConfigIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    @Override
    protected void initializeSchemaForTest()
    {
        QualifiedName name = new QualifiedName("sidecar_internal", "configs");
        createTestKeyspace(name, DC1_RF1);
        String createTableStatement = "CREATE TABLE IF NOT EXISTS %s ( service text, config map<text, text>, PRIMARY KEY (service))";
        createTestTable(name, createTableStatement);
    }

    @Override
    protected void beforeTestStart()
    {
        // wait for the schema initialization
        waitForSchemaReady(30, TimeUnit.SECONDS);
    }

    @Test
    void testConfigOperations()
    {
        String configRoute = "/api/v1/services/cdc/config";

        // Create new configs
        UpdateCdcServiceConfigPayload payload = new UpdateCdcServiceConfigPayload(Map.of("k1", "v1"));
        UpdateCdcServiceConfigPayload newConfigResponse = getBlocking(trustedClient().put(server.actualPort(), "localhost", configRoute)
                                                                                     .as(BodyCodec.json(UpdateCdcServiceConfigPayload.class))
                                                                                     .sendJson(JsonObject.mapFrom(payload))
                                                                                     .expecting(HttpResponseExpectation.SC_OK))
                                                          .body();
        assertThat(newConfigResponse).isEqualTo(payload);

        // update configs
        UpdateCdcServiceConfigPayload updatedPayload = new UpdateCdcServiceConfigPayload(Map.of("k3", "v3"));
        UpdateCdcServiceConfigPayload updatedConfigResponse = getBlocking(trustedClient().put(server.actualPort(), "localhost", configRoute)
                                                                                         .as(BodyCodec.json(UpdateCdcServiceConfigPayload.class))
                                                                                         .sendJson(JsonObject.mapFrom(updatedPayload)))
                                                              .body();
        assertThat(updatedConfigResponse).isEqualTo(updatedPayload);

        // GetConfigs should give updated configs
        String getConfigsRoute = ApiEndpointsV1.SERVICES_CONFIG_ROUTE;
        AllServicesConfigPayload getServicesResponse = getBlocking(trustedClient().get(server.actualPort(), "localhost", getConfigsRoute)
                                                                                  .as(BodyCodec.json(AllServicesConfigPayload.class))
                                                                                  .sendJson(JsonObject.mapFrom(updatedPayload)))
                                                       .body();
        List<AllServicesConfigPayload.Service> services = List.of(
        new AllServicesConfigPayload.Service("kafka", Map.of()),
        new AllServicesConfigPayload.Service("cdc", updatedPayload.config()));
        AllServicesConfigPayload expectedConfigPayload = new AllServicesConfigPayload(services);
        assertThat(getServicesResponse).isEqualTo(expectedConfigPayload);

        // delete all CDC configs
        assertThat(getBlocking(trustedClient().delete(server.actualPort(), "localhost", configRoute)
                                              .send()).bodyAsJsonObject().getString("status")).isEqualTo("OK");

        // Get configs should have no configs
        AllServicesConfigPayload response = getBlocking(trustedClient().get(server.actualPort(), "localhost", getConfigsRoute)
                                                                       .as(BodyCodec.json(AllServicesConfigPayload.class))
                                                                       .sendJson(JsonObject.mapFrom(updatedPayload)))
                                            .body();
        response.services()
                .forEach(service -> assertThat(service.config).as("Configs for all services should be empty").isEmpty());
    }

    @Test
    void testConfigOperationWithInvalidService()
    {
        String configRoute = "/api/v1/services/invalid/config";

        // Update with Invalid service
        Map<String, String> configs = Map.of("k1", "v1");
        UpdateCdcServiceConfigPayload payload = new UpdateCdcServiceConfigPayload(configs);
        HttpResponse<Buffer> response = getBlocking(trustedClient().put(server.actualPort(), "localhost", configRoute)
                                                                   .sendJson(JsonObject.mapFrom(payload)));

        assertThat(HttpResponseStatus.BAD_REQUEST.code()).isEqualTo(response.statusCode());
        assertThat(response.bodyAsJsonObject().getString("message")).isEqualTo("Invalid service provided. Supported services: cdc, kafka");
    }

}
