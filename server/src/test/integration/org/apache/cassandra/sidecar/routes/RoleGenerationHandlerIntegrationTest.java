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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Session;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.request.data.GenerateRoleRequestPayload;
import org.apache.cassandra.sidecar.common.response.GenerateRoleResponse;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.AuthMode;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.apache.cassandra.sidecar.adapters.base.CassandraRolesOperations.UUIDGenerator.NAME_PREFIX_KEY;
import static org.apache.cassandra.sidecar.adapters.base.CassandraRolesOperations.UUIDGenerator.NAME_SUFFIX_KEY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
public class RoleGenerationHandlerIntegrationTest extends IntegrationTestBase
{
    private WebClient client;
    private Session session;

    @BeforeEach
    public void setUp() throws Throwable
    {
        sidecarTestContext.setUsernamePassword("cassandra", "cassandra");
        waitForSchemaReady(30, TimeUnit.SECONDS);
        session = sidecarTestContext.session();
        client = mTLSClient();
    }

    @CassandraIntegrationTest(authMode = AuthMode.PASSWORD, enableSsl = true)
    public void testSidecarSideRoleGenerationWithoutGeneratedPassword(VertxTestContext context)
    {
        GenerateRoleRequestPayload payload = new GenerateRoleRequestPayload(Map.of(), true, false, false, false);
        GenerateRoleResponse generatedRole = createGeneratedRole(client, context, payload);

        logger.info("Generated: {}", generatedRole);

        assertNotNull(generatedRole);
        assertNull(generatedRole.password());
        assertFalse(generatedRole.isGeneratedPassword());
    }

    @CassandraIntegrationTest(authMode = AuthMode.PASSWORD, enableSsl = true)
    public void testSidecarSideRoleGenerationWithGeneratedPassword(VertxTestContext context)
    {
        GenerateRoleRequestPayload payload = new GenerateRoleRequestPayload(Map.of(), true, true, false, false);
        GenerateRoleResponse generatedRole = createGeneratedRole(client, context, payload);

        logger.info("Generated: {}", generatedRole);

        assertNotNull(generatedRole);
        assertNotNull(generatedRole.password());
        assertTrue(generatedRole.isGeneratedPassword());
    }

    @CassandraIntegrationTest(authMode = AuthMode.PASSWORD, enableSsl = true)
    public void testSidecarSideRoleGenerationWithOptions(VertxTestContext context)
    {
        GenerateRoleRequestPayload payload = new GenerateRoleRequestPayload(Map.of(NAME_PREFIX_KEY, "prefix_", NAME_SUFFIX_KEY, "_suffix"), true, true, false, false);
        GenerateRoleResponse generatedRole = createGeneratedRole(client, context, payload);
        logger.info("Generated: {}", generatedRole);

        assertNotNull(generatedRole);
        assertNotNull(generatedRole.password());
        assertTrue(generatedRole.isGeneratedPassword());

        assertTrue(generatedRole.role().startsWith("prefix_"));
        assertTrue(generatedRole.role().endsWith("_suffix"));
    }

    private GenerateRoleResponse createGeneratedRole(WebClient client, VertxTestContext context, GenerateRoleRequestPayload payload)
    {
        CompletableFuture<GenerateRoleResponse> future = new CompletableFuture<>();

        client.put(server.actualPort(), "127.0.0.1", ApiEndpointsV1.GENERATE_ROLE)
              .as(BodyCodec.json(GenerateRoleResponse.class))
              .sendJsonObject(JsonObject.mapFrom(payload))
              .map(HttpResponse::body)
              .onSuccess(response -> {
                  assertNotNull(response);
                  future.complete(response);
                  context.completeNow();
              })
              .onFailure(context::failNow);

        try
        {
            return future.get(10, TimeUnit.SECONDS);
        }
        catch (Throwable t)
        {
            context.failNow(t);
            return null;
        }
    }
}
