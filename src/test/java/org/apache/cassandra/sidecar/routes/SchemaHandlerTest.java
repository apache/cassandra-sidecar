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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.cassandra.sidecar.MainModule;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link SchemaHandler}
 */
@ExtendWith(VertxExtension.class)
class SchemaHandlerTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(SchemaHandlerTest.class);
    Vertx vertx;
    Configuration config;
    HttpServer server;
    @TempDir
    File dataDir0;
    String testKeyspaceSchema;

    @SuppressWarnings("DataFlowIssue")
    @BeforeEach
    void before() throws InterruptedException, IOException
    {
        ClassLoader cl = getClass().getClassLoader();
        testKeyspaceSchema = IOUtils.toString(cl.getResourceAsStream("schema/test_keyspace_schema.cql"),
                                              StandardCharsets.UTF_8);

        Injector injector;
        injector = Guice.createInjector(Modules.override(new MainModule())
                                               .with(Modules.override(new TestModule())
                                                            .with(new SchemaHandlerTestModule())));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(HttpServer.class);
        config = injector.getInstance(Configuration.class);
        VertxTestContext context = new VertxTestContext();
        server.listen(config.getPort(), config.getHost(), context.succeedingThenComplete());
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void after() throws InterruptedException
    {
        final CountDownLatch closeLatch = new CountDownLatch(1);
        server.close(res -> closeLatch.countDown());
        vertx.close();
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    @Test
    void testAllKeyspacesSchema(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/schema/keyspaces";
        client.get(config.getPort(), config.getHost(), testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> context.verify(() ->
              {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  JsonObject jsonObject = response.bodyAsJsonObject();
                  assertThat(jsonObject.getString("keyspace")).isNull();
                  assertThat(jsonObject.getString("schema"))
                      .isEqualTo("FULL SCHEMA");
                  context.completeNow();
              })));
    }

    @Test
    void testKeyspaceSchema(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/schema/keyspaces/testKeyspace";
        client.get(config.getPort(), config.getHost(), testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> context.verify(() ->
              {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  JsonObject jsonObject = response.bodyAsJsonObject();
                  assertThat(jsonObject.getString("keyspace")).isEqualTo("testKeyspace");
                  assertThat(jsonObject.getString("schema"))
                      .isEqualTo(testKeyspaceSchema);
                  context.completeNow();
              })));
    }

    @Test
    void testGetKeyspaceDoesNotExist(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/schema/keyspaces/nonExistent";
        client.get(config.getPort(), config.getHost(), testRoute)
              .expect(ResponsePredicate.SC_NOT_FOUND)
              .send(context.succeeding(response -> context.verify(() ->
              {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  context.completeNow();
              })));
    }

    public class SchemaHandlerTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public InstancesConfig getInstanceConfig() throws IOException
        {
            final int instanceId = 100;
            final String host = "127.0.0.1";
            final InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
            when(instanceMetadata.host()).thenReturn(host);
            when(instanceMetadata.port()).thenReturn(9042);
            when(instanceMetadata.dataDirs()).thenReturn(Collections.singletonList(dataDir0.getCanonicalPath()));
            when(instanceMetadata.id()).thenReturn(instanceId);
            CassandraAdapterDelegate mockCassandraAdapterDelegate = mock(CassandraAdapterDelegate.class);
            when(instanceMetadata.delegate()).thenReturn(mockCassandraAdapterDelegate);
            Metadata mockMetadata = mock(Metadata.class);
            KeyspaceMetadata mockKeyspaceMetadata = mock(KeyspaceMetadata.class);
            when(mockMetadata.exportSchemaAsString()).thenReturn("FULL SCHEMA");
            when(mockMetadata.getKeyspace("testKeyspace")).thenReturn(mockKeyspaceMetadata);
            when(mockKeyspaceMetadata.exportAsString()).thenReturn(testKeyspaceSchema);

            when(mockCassandraAdapterDelegate.metadata()).thenReturn(mockMetadata);

            InstancesConfig mockInstancesConfig = mock(InstancesConfig.class);
            when(mockInstancesConfig.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesConfig.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesConfig.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesConfig;
        }
    }
}
