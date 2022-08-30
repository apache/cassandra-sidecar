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
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.CQLSession;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link KeyspacesHandler}
 */
@ExtendWith(VertxExtension.class)
class KeyspacesHandlerTest extends AbstractHandlerTest
{
    @TempDir
    File dataDir0;

    @Override
    protected Module initializeCustomModule()
    {
        return new KeyspacesHandlerTest.KeyspacesInfoTestModule();
    }

    @Test
    void testKeyspaceExists(VertxTestContext context)
    {
        runHeadRequestTests(context, "/api/v1/keyspace/testKeyspace", 200);
    }

    @Test
    void testKeyspaceDoesNotExist(VertxTestContext context)
    {
        runHeadRequestTests(context, "/api/v1/keyspace/nonExistent", 404);
    }

    @Test
    void testTableExists(VertxTestContext context)
    {
        runHeadRequestTests(context, "/api/v1/keyspace/testKeyspace/table/testTable", 200);
    }

    @Test
    void testTableDoesNotExist(VertxTestContext context)
    {
        runHeadRequestTests(context, "/api/v1/keyspace/testKeyspace/table/nonExistent", 404);
    }

    @Test
    void testKeyspaceMissingForTableCheckRequest(VertxTestContext context)
    {
        runHeadRequestTests(context, "/api/v1/keyspace/random/table/testTable", 404);
    }

    @Test
    void testKeyspaces(VertxTestContext context)
    {
    }

    private void runHeadRequestTests(VertxTestContext context, String uri, int expectedCode)
    {
        WebClient client = WebClient.create(vertx);
        client.head(config.getPort(), config.getHost(), uri)
              .as(BodyCodec.buffer())
              .send(context.succeeding(response -> context.verify(() ->
                                                                  {
                                                                      assertThat(response.statusCode()).isEqualTo(expectedCode);
                                                                      context.completeNow();
                                                                  })));
    }

    public class KeyspacesInfoTestModule extends AbstractModule
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
            when(instanceMetadata.delegate()).thenReturn(mock(CassandraAdapterDelegate.class));
            CQLSession mockCqlSession = mock(CQLSession.class);
            Session mockSession = mock(Session.class);
            when(mockCqlSession.getLocalCql()).thenReturn(mockSession);
            Cluster mockCluster = mock(Cluster.class);
            Metadata mockMetadata = mock(Metadata.class);
            KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
            TableMetadata tableMetadata = mock(TableMetadata.class);
            when(keyspaceMetadata.getTable("testTable")).thenReturn(tableMetadata);
            when(mockMetadata.getKeyspace("testKeyspace")).thenReturn(keyspaceMetadata);
            when(mockCluster.getMetadata()).thenReturn(mockMetadata);
            when(mockSession.getCluster()).thenReturn(mockCluster);
            when(instanceMetadata.session()).thenReturn(mockCqlSession);

            InstancesConfig mockInstancesConfig = mock(InstancesConfig.class);
            when(mockInstancesConfig.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesConfig.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesConfig.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesConfig;
        }
    }
}
