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

package org.apache.cassandra.sidecar.client.request;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.cassandra.sidecar.client.HttpClientConfig;
import org.apache.cassandra.sidecar.client.RequestContext;
import org.apache.cassandra.sidecar.client.exception.RetriesExhaustedException;
import org.apache.cassandra.sidecar.client.selection.InstanceSelectionPolicy;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;

/**
 * Provides base functionality for testing the RequestExecutor
 *
 * @param <T> the type of the Request to test
 */
public abstract class RequestExecutorTest<T> extends BaseRequestTest
{
    @AfterEach
    void cleanup() throws Exception
    {
        sidecarClient().close();
    }

    @ParameterizedTest(name = "testHappyPath {index} => request={0}, async={1}")
    @ArgumentsSource(TestParameters.class)
    public void testHappyPath(RequestTestParameters<T> parameters, boolean async) throws Exception
    {
        try (MockWebServer server = new MockWebServer())
        {
            server.enqueue(new MockResponse().setResponseCode(OK.code()).setBody(parameters.okResponseBody()));

            // Start the server.
            server.start();

            runTestScenario(parameters, buildRandomFromServerList(server), async);

            RecordedRequest request = server.takeRequest();
            assertThat(request.getPath()).isEqualTo(parameters.expectedEndpointPath());
            assertThat(request.getHeader("User-Agent")).isEqualTo("sidecar-client-test/1.0.0");
        }
    }

    @ParameterizedTest(name = "testRetriesOnTimeoutWithSlowServer {index} => request={0}, async={1}")
    @ArgumentsSource(TestParameters.class)
    public void testRetriesOnTimeoutWithSlowServer(RequestTestParameters<T> parameters, boolean async) throws Exception
    {
        String happyPathResponse = parameters.okResponseBody();
        MockWebServer slowServer = new MockWebServer();
        try (MockWebServer fastServer = new MockWebServer())
        {
            // something egregiously slow
            slowServer.enqueue(new MockResponse().setBodyDelay(5, TimeUnit.MINUTES)
                                                 .setResponseCode(OK.code())
                                                 .setBody(happyPathResponse));
            fastServer.enqueue(new MockResponse().setResponseCode(OK.code()).setBody(happyPathResponse));

            // Start both servers.
            slowServer.start();
            fastServer.start();

            runTestScenario(parameters, buildFromServerList(slowServer, fastServer), async);

            assertThat(slowServer.getRequestCount()).isEqualTo(1);
            RecordedRequest requestToSlowServer = slowServer.takeRequest();
            assertThat(requestToSlowServer.getPath()).isEqualTo(parameters.expectedEndpointPath());

            assertThat(fastServer.getRequestCount()).isEqualTo(1);
            RecordedRequest requestToFastServer = fastServer.takeRequest();
            assertThat(requestToFastServer.getPath()).isEqualTo(parameters.expectedEndpointPath());
            assertThat(requestToFastServer.getHeader("User-Agent")).isEqualTo("sidecar-client-test/1.0.0");
        }
    }

    @ParameterizedTest(name = "testRetriesWithServerReturningServerError {index} => request={0}, async={1}")
    @ArgumentsSource(TestParameters.class)
    public void testRetriesWithServerReturningServerError(RequestTestParameters<T> parameters, boolean async)
    throws Exception
    {
        try (MockWebServer serverErrorServer = new MockWebServer();
             MockWebServer normalOperatingServer = new MockWebServer())
        {
            serverErrorServer.enqueue(new MockResponse().setResponseCode(INTERNAL_SERVER_ERROR.code())
                                                        .setBody(parameters.serverErrorResponseBody()));
            normalOperatingServer.enqueue(new MockResponse().setResponseCode(OK.code())
                                                            .setBody(parameters.okResponseBody()));

            // Start both servers.
            serverErrorServer.start();
            normalOperatingServer.start();

            runTestScenario(parameters, buildFromServerList(serverErrorServer, normalOperatingServer), async);

            assertThat(serverErrorServer.getRequestCount()).isEqualTo(1);
            RecordedRequest requestToErrorServer = serverErrorServer.takeRequest();
            assertThat(requestToErrorServer.getPath()).isEqualTo(parameters.expectedEndpointPath());

            assertThat(normalOperatingServer.getRequestCount()).isEqualTo(1);
            RecordedRequest requestToNormalServer = normalOperatingServer.takeRequest();
            assertThat(requestToNormalServer.getPath()).isEqualTo(parameters.expectedEndpointPath());
            assertThat(requestToNormalServer.getHeader("User-Agent")).isEqualTo("sidecar-client-test/1.0.0");
        }
    }

    @ParameterizedTest(name = "testWithSingleServerAndRetries {index} => request={0}, async={1}")
    @ArgumentsSource(TestParameters.class)
    public void testWithSingleServerAndRetries(RequestTestParameters<T> parameters, boolean async) throws Exception
    {
        try (MockWebServer server = new MockWebServer())
        {
            String serverErrorResponseBody = parameters.serverErrorResponseBody();
            server.enqueue(new MockResponse().setResponseCode(INTERNAL_SERVER_ERROR.code())
                                             .setBody(serverErrorResponseBody));
            server.enqueue(new MockResponse().setResponseCode(INTERNAL_SERVER_ERROR.code())
                                             .setBody(serverErrorResponseBody));
            server.enqueue(new MockResponse().setResponseCode(OK.code()).setBody(parameters.okResponseBody()));

            // Start both servers.
            server.start();

            runTestScenario(parameters, buildFromServerList(server), async);

            assertThat(server.getRequestCount()).isEqualTo(3);
            RecordedRequest request = server.takeRequest();
            assertThat(request.getPath()).isEqualTo(parameters.expectedEndpointPath());
            assertThat(request.getHeader("User-Agent")).isEqualTo("sidecar-client-test/1.0.0");
        }
    }

    @ParameterizedTest(name = "testWithSingleServerExhaustsRetries {index} => request={0}, async={1}")
    @ArgumentsSource(TestParameters.class)
    public void testWithSingleServerExhaustsRetries(RequestTestParameters<T> parameters, boolean async)
    throws Exception
    {
        String serverErrorResponseBody = parameters.serverErrorResponseBody();

        try (MockWebServer server = new MockWebServer())
        {
            server.enqueue(new MockResponse().setResponseCode(INTERNAL_SERVER_ERROR.code())
                                             .setBody(serverErrorResponseBody));
            server.enqueue(new MockResponse().setResponseCode(INTERNAL_SERVER_ERROR.code())
                                             .setBody(serverErrorResponseBody));
            server.enqueue(new MockResponse().setResponseCode(INTERNAL_SERVER_ERROR.code())
                                             .setBody(serverErrorResponseBody));
            server.enqueue(new MockResponse().setResponseCode(OK.code()).setBody(parameters.okResponseBody()));

            // Start both servers.
            server.start();

            assertThatException().isThrownBy(() ->
                                             runTestScenario(parameters, buildFromServerList(server), async))
                                 .withCauseInstanceOf(RetriesExhaustedException.class)
                                 .withMessageContaining("Unable to complete request '")
                                 .withMessageContaining("' after 3 attempts");

            assertThat(server.getRequestCount()).isEqualTo(3);
            RecordedRequest request = server.takeRequest();
            assertThat(request.getPath()).isEqualTo(parameters.expectedEndpointPath());
            assertThat(request.getHeader("User-Agent")).isEqualTo("sidecar-client-test/1.0.0");
        }
    }

    protected HttpClientConfig.Builder<?> httpClientConfigBuilder()
    {
        return new HttpClientConfig.Builder<>()
               .userAgent("sidecar-client-test/1.0.0")
               .ssl(false)
               .timeoutMillis(100)
               .idleTimeoutMillis(100);
    }

    private void runTestScenario(RequestTestParameters<T> parameters,
                                 InstanceSelectionPolicy policy,
                                 boolean async) throws Exception
    {
        RequestContext requestContext = parameters.specificRequest(builder(policy)).build();
        T responseObject;
        if (async)
        {
            CompletableFuture<T> future = sidecarClient().executeRequestAsync(requestContext);
            responseObject = future.join();
            assertThat(future).isDone();
        }
        else
        {
            responseObject = sidecarClient().executeRequest(requestContext, 20, TimeUnit.SECONDS);
        }
        assertThat(responseObject).isNotNull();
        parameters.validateResponse(responseObject);
    }

    static class TestParameters implements ArgumentsProvider
    {
        private final List<RequestTestParameters<?>> requestTestParameters = Arrays.asList(
        new GossipInfoRequestTestParameters(),
        new NodeSettingsRequestTestParameters(),
        new RingRequestForKeyspaceTestParameters(),
        new RingRequestTestParameters(),
        new TimeSkewRequestTestParameters(),
        new TokenRangeReplicasRequestTestParameters(),
        new FullSchemaRequestTestParameters(),
        new SchemaRequestTestParameters(),
        new ListSnapshotFilesRequestTestParameters()
        );

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context)
        {
            return Stream.of(true, false)
                         .flatMap(async -> requestTestParameters.stream().map(p -> Arguments.arguments(p, async)));
        }
    }
}
