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

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Metadata;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.Callback;
import datahub.client.MetadataWriteResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.utils.IOUtils;
import org.apache.cassandra.sidecar.datahub.EmitterFactory;
import org.apache.cassandra.sidecar.datahub.JsonEmitter;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ReportSchemaHandler}
 */
@ExtendWith(VertxExtension.class)
final class ReportSchemaHandlerTest
{
    private static final String CLUSTER = "cluster";
    private static final String DIRECTORY = "/tmp";
    private static final int IDENTIFIER = 42;
    private static final String LOCALHOST = "127.0.0.1";
    private static final int PORT = 9042;
    private static final String ENDPOINT = "/api/v1/datahub/schemas";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private static final class ThrowingEmitter extends JsonEmitter
    {
        @Override
        @NotNull
        public synchronized Future<MetadataWriteResponse> emit(@NotNull MetadataChangeProposal proposal,
                                                               @Nullable Callback callback) throws IOException
        {
            throw new IOException();
        }
    }

    private final class ReportSchemaHandlerTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        @NotNull
        public InstancesMetadata instancesMetadata()
        {
            Metadata metadata = mock(Metadata.class);
            when(metadata.getKeyspaces()).thenReturn(Collections.emptyList());

            StorageOperations operations = mock(StorageOperations.class);
            when(operations.clusterName()).thenReturn(CLUSTER);

            CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
            when(delegate.storageOperations()).thenReturn(operations);
            when(delegate.metadata()).thenReturn(metadata);

            InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
            when(instanceMetadata.stagingDir()).thenReturn(DIRECTORY);
            when(instanceMetadata.id()).thenReturn(IDENTIFIER);
            when(instanceMetadata.host()).thenReturn(LOCALHOST);
            when(instanceMetadata.port()).thenReturn(PORT);
            when(instanceMetadata.delegate()).thenReturn(delegate);

            InstancesMetadata instances = mock(InstancesMetadata.class);
            when(instances.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(instances.instanceFromId(IDENTIFIER)).thenReturn(instanceMetadata);
            when(instances.instanceFromHost(LOCALHOST)).thenReturn(instanceMetadata);
            return instances;
        }

        @Provides
        @Singleton
        @NotNull
        public EmitterFactory emitterFactory()
        {
            return () -> emitter;
        }
    }

    private final Injector injector = Guice.createInjector(Modules.override(new MainModule())
                                                                  .with(Modules.override(new TestModule())
                                                                               .with(new ReportSchemaHandlerTestModule())));
    private WebClient client;
    private Server server;
    private JsonEmitter emitter;

    @BeforeEach
    void before() throws InterruptedException
    {
        VertxTestContext context = new VertxTestContext();
        server = injector.getInstance(Server.class);
        server.start()
              .onSuccess(result -> context.completeNow())
              .onFailure(context::failNow);

        client = WebClient.create(injector.getInstance(Vertx.class));

        context.awaitCompletion(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    @AfterEach
    void after() throws InterruptedException
    {
        TestResourceReaper.create()
                          .with(server)
                          .with(client)
                          .close();
    }

    @Test
    void testSuccess(@NotNull VertxTestContext context) throws IOException
    {
        String expected = IOUtils.readFully("/datahub/empty_cluster.json");
        emitter = new JsonEmitter();
        assertThat(emitter.content().length()).isLessThanOrEqualTo(1);

        client.put(server.actualPort(), LOCALHOST, ENDPOINT)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(emitter.content()).isEqualTo(expected);
                  context.completeNow();
              }));
    }

    @Test
    void testFailure(@NotNull VertxTestContext context)
    {
        String expected = "[\n]";
        emitter = new ThrowingEmitter();
        assertThat(emitter.content().length()).isLessThanOrEqualTo(1);

        client.put(server.actualPort(), LOCALHOST, ENDPOINT)
              .expect(ResponsePredicate.SC_INTERNAL_SERVER_ERROR)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                  assertThat(emitter.content()).isEqualTo(expected);
                  context.completeNow();
              }));
    }
}
