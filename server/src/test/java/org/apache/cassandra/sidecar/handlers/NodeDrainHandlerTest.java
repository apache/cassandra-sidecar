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

package org.apache.cassandra.sidecar.handlers;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;
import org.mockito.AdditionalAnswers;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.RUNNING;
import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link NodeDrainHandler}
 */
@ExtendWith(VertxExtension.class)
public class NodeDrainHandlerTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(NodeDrainHandlerTest.class);
    static final String TEST_ROUTE = "/api/v1/cassandra/operations/drain";
    static final String OPERATION_MODE_NORMAL = "NORMAL";
    static final String OPERATION_MODE_DRAINING = "DRAINING";
    static final String OPERATION_MODE_JOINING = "JOINING";
    static final String EXPECTED_OPERATION_NAME = "drain";
    static final String SIMULATED_DRAIN_FAILURE = "Simulated drain failure";

    Vertx vertx;
    Server server;
    StorageOperations mockStorageOperations = mock(StorageOperations.class);

    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        Module testOverride = Modules.override(new TestModule())
                                     .with(new NodeDrainHandlerTest.NodeDrainTestModule());
        injector = Guice.createInjector(Modules.override(SidecarModules.all())
                                               .with(testOverride));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void after() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    @Test
    void testDrainLongRunning(VertxTestContext context) throws Exception
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        doAnswer(AdditionalAnswers.answersWithDelay(6000, invocation -> null))
        .when(mockStorageOperations).drain();

        WebClient client = WebClient.create(vertx);
        client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
              .expect(ResponsePredicate.SC_ACCEPTED)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(ACCEPTED.code());
                  OperationalJobResponse drainResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(drainResponse).isNotNull();
                  assertThat(drainResponse.status()).isEqualTo(RUNNING);
                  assertThat(drainResponse.jobId()).isNotNull();
                  assertThat(drainResponse.operation()).isEqualTo(EXPECTED_OPERATION_NAME);
                  context.completeNow();
              }));
    }

    @Test
    void testDrainCompleted(VertxTestContext context)
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);

        WebClient client = WebClient.create(vertx);
        client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  LOGGER.info("Drain Response: {}", response.bodyAsString());

                  OperationalJobResponse drainResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(drainResponse).isNotNull();
                  assertThat(drainResponse.status()).isEqualTo(SUCCEEDED);
                  assertThat(drainResponse.jobId()).isNotNull();
                  assertThat(drainResponse.operation()).isEqualTo(EXPECTED_OPERATION_NAME);
                  context.completeNow();
              }));
    }

    @Test
    void testDrainFailed(VertxTestContext context) throws Exception
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_NORMAL);
        doThrow(new RuntimeException(SIMULATED_DRAIN_FAILURE)).when(mockStorageOperations).drain();

        WebClient client = WebClient.create(vertx);
        client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  LOGGER.info("Drain Response: {}", response.bodyAsString());

                  OperationalJobResponse drainResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(drainResponse).isNotNull();
                  assertThat(drainResponse.jobId()).isNotNull();
                  assertThat(drainResponse.operation()).isEqualTo(EXPECTED_OPERATION_NAME);
                  context.completeNow();
              }));
    }

    @Test
    void testDrainConflictWhenDraining(VertxTestContext context)
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_DRAINING);

        WebClient client = WebClient.create(vertx);
        client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
              .expect(ResponsePredicate.SC_CONFLICT)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(CONFLICT.code());
                  LOGGER.info("Drain Response: {}", response.bodyAsString());

                  OperationalJobResponse drainResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(drainResponse).isNotNull();
                  assertThat(drainResponse.jobId()).isNotNull();
                  assertThat(drainResponse.operation()).isEqualTo(EXPECTED_OPERATION_NAME);
                  context.completeNow();
              }));
    }

    @Test
    void testDrainAllowedWhenJoining(VertxTestContext context)
    {
        when(mockStorageOperations.operationMode()).thenReturn(OPERATION_MODE_JOINING);

        WebClient client = WebClient.create(vertx);
        client.put(server.actualPort(), "127.0.0.1", TEST_ROUTE)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  LOGGER.info("Drain Response: {}", response.bodyAsString());

                  OperationalJobResponse drainResponse = response.bodyAsJson(OperationalJobResponse.class);
                  assertThat(drainResponse).isNotNull();
                  assertThat(drainResponse.status()).isEqualTo(SUCCEEDED);
                  assertThat(drainResponse.jobId()).isNotNull();
                  assertThat(drainResponse.operation()).isEqualTo(EXPECTED_OPERATION_NAME);
                  context.completeNow();
              }));
    }

    /**
     * Test guice module for Node Drain handler tests
     */
    class NodeDrainTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public InstancesMetadata instanceMetadata()
        {
            int instanceId = 100;
            String host = "127.0.0.1";
            InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
            when(instanceMetadata.host()).thenReturn(host);
            when(instanceMetadata.port()).thenReturn(9042);
            when(instanceMetadata.id()).thenReturn(instanceId);
            when(instanceMetadata.stagingDir()).thenReturn("");

            CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);

            NodeSettings mockNodeSettings = NodeSettings.builder()
                                                        .hostId(UUID.randomUUID())
                                                        .build();
            when(delegate.storageOperations()).thenReturn(mockStorageOperations);
            when(delegate.nodeSettings()).thenReturn(mockNodeSettings);
            when(instanceMetadata.delegate()).thenReturn(delegate);

            InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
            when(mockInstancesMetadata.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesMetadata.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesMetadata.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesMetadata;
        }
    }
}
