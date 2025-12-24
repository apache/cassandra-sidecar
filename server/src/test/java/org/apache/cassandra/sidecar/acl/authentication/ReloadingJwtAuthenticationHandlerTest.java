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

package org.apache.cassandra.sidecar.acl.authentication;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.codahale.metrics.MetricRegistry;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.metrics.server.AuthMetrics;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link ReloadingJwtAuthenticationHandler}
 */
class ReloadingJwtAuthenticationHandlerTest
{
    @Test
    void testDelegateHandlerNotSet()
    {
        Vertx mockVertx = mock(Vertx.class);
        JwtParameterExtractor parameterExtractor = new JwtParameterExtractor(Map.of("enabled", "true",
                                                                                    "site", "www.apache.org",
                                                                                    "client_id", "id"));
        JwtRoleProcessor mockRoleProcessor = mock(JwtRoleProcessor.class);
        when(mockRoleProcessor.processRoles(any())).thenReturn(Future.succeededFuture(List.of("test_role")));
        PeriodicTaskExecutor mockTaskExecutor = mock(PeriodicTaskExecutor.class);
        doNothing().when(mockTaskExecutor).schedule(any());
        MetricRegistry metricRegistry = new MetricRegistry();
        AuthMetrics authMetrics = new AuthMetrics(metricRegistry);
        ReloadingJwtAuthenticationHandler reloadingJwtAuthenticationHandler
        = new ReloadingJwtAuthenticationHandler(mockVertx, parameterExtractor, mockRoleProcessor, mockTaskExecutor, authMetrics);
        RoutingContext mockCtx = mock(RoutingContext.class);
        reloadingJwtAuthenticationHandler.authenticate(mockCtx, result -> {
            assertThat(result.failed()).isTrue();
            assertThat(result.cause()).hasMessage("Service Unavailable");
        });
    }

    @Test
    void testDelegateHandlerNotCreatedWhenJWTDisabled()
    {
        Vertx mockVertx = mock(Vertx.class);
        JwtParameterExtractor parameterExtractor = new JwtParameterExtractor(Map.of("enabled", "false",
                                                                                    "site", "www.apache.org",
                                                                                    "client_id", "id"));
        JwtRoleProcessor mockRoleProcessor = mock(JwtRoleProcessor.class);
        PeriodicTaskExecutor mockTaskExecutor = mock(PeriodicTaskExecutor.class);
        MetricRegistry metricRegistry = new MetricRegistry();
        AuthMetrics authMetrics = new AuthMetrics(metricRegistry);
        ReloadingJwtAuthenticationHandler reloadingJwtAuthenticationHandler
        = new ReloadingJwtAuthenticationHandler(mockVertx, parameterExtractor, mockRoleProcessor, mockTaskExecutor, authMetrics);
        RoutingContext mockCtx = mock(RoutingContext.class);
        reloadingJwtAuthenticationHandler.authenticate(mockCtx, result -> {
            assertThat(result.failed()).isTrue();
            assertThat(result.cause()).hasMessage("Service Unavailable");
        });
    }

    @Test
    void testStatelessJwtAuthenticationWithValidToken() throws Exception
    {
        // Generate a test RSA key pair and PEM
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        KeyPair keyPair = keyGen.generateKeyPair();

        String publicKeyPem = "-----BEGIN PUBLIC KEY-----\n" +
                              Base64.getEncoder().encodeToString(keyPair.getPublic().getEncoded()) +
                              "\n-----END PUBLIC KEY-----";

        Vertx vertx = Vertx.vertx();
        try
        {
            // Mock HTTP server to serve the PEM
            HttpServer mockServer = vertx.createHttpServer();
            CountDownLatch serverLatch = new CountDownLatch(1);

            mockServer.requestHandler(request -> {
                request.response()
                       .putHeader("Content-Type", "text/plain")
                       .end(publicKeyPem);
            }).listen(0, result -> serverLatch.countDown());

            serverLatch.await(5, TimeUnit.SECONDS);

            // Configure for stateless authentication
            String site = String.format("http://localhost:%d/jwks", mockServer.actualPort());
            JwtParameterExtractor parameterExtractor = new JwtParameterExtractor(Map.of("enabled", "true",
                                                                                        "site", site,
                                                                                        "jwt_auth_type", JwtParameters.AuthType.STATELESS.toString().toLowerCase()));
            JwtRoleProcessor mockRoleProcessor = mock(JwtRoleProcessor.class);
            when(mockRoleProcessor.processRoles(any())).thenReturn(Future.succeededFuture(List.of("test_role")));
            ReloadingJwtAuthenticationHandler handler = getReloadingJwtAuthenticationHandler(vertx, parameterExtractor, mockRoleProcessor);

            // Wait a bit for the handler to be set
            loopAssert(1, () -> assertNotNull(handler.delegateHandler.get()));

            // Test authentication with valid token - create proper mocks
            RoutingContext mockCtx = mock(RoutingContext.class);
            HttpServerRequest mockRequest = mock(HttpServerRequest.class);

            // Mock the request properly for JWT parsing
            when(mockCtx.request()).thenReturn(mockRequest);
            // Create a valid JWT token using the private key
            String validToken = createTestJwtToken(keyPair.getPrivate());
            when(mockRequest.getHeader("Authorization")).thenReturn("Bearer " + validToken);
            when(mockRequest.headers()).thenReturn(io.vertx.core.MultiMap.caseInsensitiveMultiMap()
                                                                         .add("Authorization", "Bearer " + validToken));

            CountDownLatch authLatch = new CountDownLatch(1);
            AtomicReference<AsyncResult<User>> authResult = new AtomicReference<>();

            handler.authenticate(mockCtx, result -> {
                authResult.set(result);
                authLatch.countDown();
            });

            authLatch.await(5, TimeUnit.SECONDS);

            // The authentication should process (may succeed or fail based on token validation)
            assertThat(authResult.get()).isNotNull();
            // Ensure that user and role context was properly passed oto the next step.
            assertThat(authResult.get().result().attributes().getString("sub")).isEqualTo("test-user");
            assertThat(authResult.get().result().attributes().getJsonArray("cassandra_roles")).isEqualTo(new JsonArray(List.of("test_role")));
            mockServer.close();
        }
        finally
        {
            vertx.close();
        }
    }

    private static @NotNull ReloadingJwtAuthenticationHandler getReloadingJwtAuthenticationHandler(Vertx vertx, JwtParameterExtractor parameterExtractor, JwtRoleProcessor mockRoleProcessor)
    {
        ExecutorPools executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
        ClusterLease clusterLease = new ClusterLease();
        PeriodicTaskExecutor executor = new PeriodicTaskExecutor(executorPools, clusterLease);
        MetricRegistry metricRegistry = new MetricRegistry();
        AuthMetrics authMetrics = new AuthMetrics(metricRegistry);
        return new ReloadingJwtAuthenticationHandler(vertx,
                                                     parameterExtractor,
                                                     mockRoleProcessor,
                                                     executor,
                                                     authMetrics
        );
    }

    private String createTestJwtToken(PrivateKey privateKey)
    {
        Algorithm algorithm = Algorithm.RSA256(null, (RSAPrivateKey) privateKey);

        return JWT.create()
                  .withIssuer("test-issuer")
                  .withSubject("test-user")
                  .withExpiresAt(new Date(System.currentTimeMillis() + 3600000)) // 1 hour
                  .withClaim("roles", List.of("test_role"))
                  .sign(algorithm);
    }
}
