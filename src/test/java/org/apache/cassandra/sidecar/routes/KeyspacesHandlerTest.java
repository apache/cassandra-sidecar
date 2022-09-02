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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ClusteringOrder;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.vertx.core.json.jackson.JacksonCodec;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.CQLSession;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.data.ColumnSchema;
import org.apache.cassandra.sidecar.common.data.KeyspaceSchema;
import org.apache.cassandra.sidecar.common.data.TableSchema;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
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
        runHeadRequestTests(context, "/api/v1/keyspaces/testKeyspace", 200);
    }

    @Test
    void testKeyspaceDoesNotExist(VertxTestContext context)
    {
        runHeadRequestTests(context, "/api/v1/keyspaces/nonExistent", 404);
    }

    @Test
    void testTableExists(VertxTestContext context)
    {
        runHeadRequestTests(context, "/api/v1/keyspaces/testKeyspace/tables/testTable", 200);
    }

    @Test
    void testTableDoesNotExist(VertxTestContext context)
    {
        runHeadRequestTests(context, "/api/v1/keyspaces/testKeyspace/tables/nonExistent", 404);
    }

    @Test
    void testKeyspaceMissingForTableCheckRequest(VertxTestContext context)
    {
        runHeadRequestTests(context, "/api/v1/keyspaces/random/tables/testTable", 404);
    }

    @Test
    void testKeyspaces(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspaces";
        client.get(config.getPort(), config.getHost(), testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response-> context.verify(() ->
              {
                  List<KeyspaceSchema> keyspaces =
                  JacksonCodec.decodeValue(response.body(), new TypeReference<List<KeyspaceSchema>>()
                  {
                  });
                  assertThat(keyspaces.size()).isEqualTo(1);
                  validateKeyspace(keyspaces.get(0));
                  context.completeNow();
              })));
    }

    @Test
    void testGetKeyspace(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspaces/testKeyspace";
        client.get(config.getPort(), config.getHost(), testRoute)
        .expect(ResponsePredicate.SC_OK)
        .send(context.succeeding(response-> context.verify(() ->
        {
            assertThat(response.statusCode()).isEqualTo(OK.code());
            KeyspaceSchema ksSchema = response.bodyAsJson(KeyspaceSchema.class);
            validateKeyspace(ksSchema);
            context.completeNow();
        })));
    }

    @Test
    void testGetKeyspaceDoesNotExist(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspaces/nonExistent";
        client.get(config.getPort(), config.getHost(), testRoute)
              .expect(ResponsePredicate.SC_NOT_FOUND)
              .send(context.succeeding(response-> context.verify(() ->
              {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  context.completeNow();
              })));
    }

    @Test
    void testGetTable(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspaces/testKeyspace/tables/testTable";
        client.get(config.getPort(), config.getHost(), testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response-> context.verify(() ->
              {
                  assertThat(response.statusCode()).isEqualTo(OK.code());
                  TableSchema tableSchema = response.bodyAsJson(TableSchema.class);
                  validateTable(tableSchema);
                  context.completeNow();
              })));
    }

    @Test
    void testGetTableDoesNotExist(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspaces/testKeyspace/tables/nonExistent";
        client.get(config.getPort(), config.getHost(), testRoute)
              .expect(ResponsePredicate.SC_NOT_FOUND)
              .send(context.succeeding(response-> context.verify(() ->
              {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  context.completeNow();
              })));
    }

    @Test
    void testKeyspaceDoesNotExistInGetTableRequest(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        String testRoute = "/api/v1/keyspaces/nonExistent/tables/testTable";
        client.get(config.getPort(), config.getHost(), testRoute)
              .expect(ResponsePredicate.SC_NOT_FOUND)
              .send(context.succeeding(response-> context.verify(() ->
              {
                  assertThat(response.statusCode()).isEqualTo(NOT_FOUND.code());
                  context.completeNow();
              })));
    }

    private void validateKeyspace(KeyspaceSchema ksSchema)
    {
        assertThat(ksSchema.getName()).isEqualTo("testKeyspace");
        assertThat(ksSchema.isVirtual()).isFalse();
        assertThat(ksSchema.isDurableWrites()).isTrue();
        assertThat(ksSchema.getReplication()).containsEntry("DC1", "3");
        assertThat(ksSchema.getReplication()).containsEntry("DC2", "3");
        assertThat(ksSchema.getReplication()).containsEntry("DC3", "2");
        assertThat(ksSchema.getTables().size()).isEqualTo(1);
        validateTable(ksSchema.getTables().get(0));
    }

    private void validateTable(TableSchema tableSchema)
    {
        org.apache.cassandra.sidecar.common.data.DataType bigint =
        new org.apache.cassandra.sidecar.common.data.DataType("BIGINT");
        List<ColumnSchema> expectedPartitionKey = Arrays.asList(
        new ColumnSchema("partition_key_1", new org.apache.cassandra.sidecar.common.data.DataType("UUID")),
        new ColumnSchema("partition_key_2", new org.apache.cassandra.sidecar.common.data.DataType("TEXT")),
        new ColumnSchema("partition_key_3",
                         new org.apache.cassandra.sidecar.common.data.DataType("SET", true,
                                                                               Collections.singletonList(bigint),
                                                                               true, null))
        );

        List<ColumnSchema> expectedClusteringColumns = Arrays.asList(
        new ColumnSchema("clustering_column_1",
                         new org.apache.cassandra.sidecar.common.data.DataType("DATE")),
        new ColumnSchema("clustering_column_2",
                         new org.apache.cassandra.sidecar.common.data.DataType("DURATION"))
        );

        String customTypeClassName = "org.apache.cassandra.db.marshal.DateType";
        List<ColumnSchema> expectedColumns = Arrays.asList(
        new ColumnSchema("column_1",
                         new org.apache.cassandra.sidecar.common.data.DataType("LIST", false, Collections.singletonList(
                         new org.apache.cassandra.sidecar.common.data.DataType("BLOB")
                         ), true, null)),
        new ColumnSchema("column_2", new org.apache.cassandra.sidecar.common.data.DataType("INET")),
        new ColumnSchema("column_3",
                         new org.apache.cassandra.sidecar.common.data.DataType("MAP", false, Arrays.asList(
                         new org.apache.cassandra.sidecar.common.data.DataType("TEXT"),
                         new org.apache.cassandra.sidecar.common.data.DataType("BIGINT")
                         ), true, null)),
        new ColumnSchema("custom_type_column_4",
                         new org.apache.cassandra.sidecar.common.data.DataType("CUSTOM", false,
                                                                               Collections.emptyList(), false,
                                                                               customTypeClassName))
        );

        List<ColumnSchema> expectedPrimaryKey = new ArrayList<>();
        expectedPrimaryKey.addAll(expectedPartitionKey);
        expectedPrimaryKey.addAll(expectedClusteringColumns);

        assertThat(tableSchema.getKeyspaceName()).isEqualTo("testKeyspace");
        assertThat(tableSchema.getName()).isEqualTo("testTable");
        assertThat(tableSchema.isVirtual()).isFalse();
        assertThat(tableSchema.hasSecondaryIndexes()).isFalse();
        assertThat(tableSchema.getPartitionKey()).containsExactlyElementsOf(expectedPartitionKey);
        assertThat(tableSchema.getClusteringColumns()).containsExactlyElementsOf(expectedClusteringColumns);
        assertThat(tableSchema.getClusteringOrder()).containsExactly("ASC", "DESC");
        assertThat(tableSchema.getColumns()).containsExactlyElementsOf(expectedColumns);
        assertThat(tableSchema.getPrimaryKey()).containsExactlyElementsOf(expectedPrimaryKey);
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
            List<TableMetadata> tables = mockTableMetadataCollection();
            KeyspaceMetadata keyspaceMetadata = mockKeyspaceMetadata(tables);

            when(keyspaceMetadata.getTable("testTable")).thenReturn(tables.get(0));
            when(mockMetadata.getKeyspace("testKeyspace")).thenReturn(keyspaceMetadata);
            when(mockMetadata.getKeyspaces()).thenReturn(Collections.singletonList(keyspaceMetadata));
            when(mockCluster.getMetadata()).thenReturn(mockMetadata);
            when(mockSession.getCluster()).thenReturn(mockCluster);
            when(instanceMetadata.session()).thenReturn(mockCqlSession);

            InstancesConfig mockInstancesConfig = mock(InstancesConfig.class);
            when(mockInstancesConfig.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesConfig.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesConfig.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesConfig;
        }

        List<TableMetadata> mockTableMetadataCollection()
        {
            KeyspaceMetadata ksMock = mock(KeyspaceMetadata.class);
            when(ksMock.getName()).thenReturn("testKeyspace");
            List<TableMetadata> listOfMocks = new ArrayList<>();
            TableMetadata mock1 = mock(TableMetadata.class);
            when(mock1.getKeyspace()).thenReturn(ksMock);
            when(mock1.getName()).thenReturn("testTable");
            when(mock1.isVirtual()).thenReturn(false);
            when(mock1.getIndexes()).thenReturn(Collections.emptyList());

            ColumnMetadata pk1 = mock(ColumnMetadata.class);
            when(pk1.getName()).thenReturn("partition_key_1");
            when(pk1.getType()).thenReturn(DataType.uuid());

            ColumnMetadata pk2 = mock(ColumnMetadata.class);
            when(pk2.getName()).thenReturn("partition_key_2");
            when(pk2.getType()).thenReturn(DataType.text());

            ColumnMetadata pk3 = mock(ColumnMetadata.class);
            when(pk3.getName()).thenReturn("partition_key_3");
            when(pk3.getType()).thenReturn(DataType.frozenSet(DataType.bigint()));

            when(mock1.getPartitionKey()).thenReturn(Arrays.asList(pk1, pk2, pk3));

            ColumnMetadata cc1 = mock(ColumnMetadata.class);
            when(cc1.getName()).thenReturn("clustering_column_1");
            when(cc1.getType()).thenReturn(DataType.date());

            ColumnMetadata cc2 = mock(ColumnMetadata.class);
            when(cc2.getName()).thenReturn("clustering_column_2");
            when(cc2.getType()).thenReturn(DataType.duration());

            when(mock1.getClusteringColumns()).thenReturn(Arrays.asList(cc1, cc2));
            when(mock1.getClusteringOrder()).thenReturn(Arrays.asList(ClusteringOrder.ASC, ClusteringOrder.DESC));

            ColumnMetadata col1 = mock(ColumnMetadata.class);
            when(col1.getName()).thenReturn("column_1");
            when(col1.getType()).thenReturn(DataType.list(DataType.blob()));

            ColumnMetadata col2 = mock(ColumnMetadata.class);
            when(col2.getName()).thenReturn("column_2");
            when(col2.getType()).thenReturn(DataType.inet());

            ColumnMetadata col3 = mock(ColumnMetadata.class);
            when(col3.getName()).thenReturn("column_3");
            when(col3.getType()).thenReturn(DataType.map(DataType.text(), DataType.bigint()));

            ColumnMetadata col4 = mock(ColumnMetadata.class);
            when(col4.getName()).thenReturn("custom_type_column_4");
            when(col4.getType()).thenReturn(DataType.custom("org.apache.cassandra.db.marshal.DateType"));

            when(mock1.getColumns()).thenReturn(Arrays.asList(col1, col2, col3, col4));

            listOfMocks.add(mock1);
            return listOfMocks;
        }

        KeyspaceMetadata mockKeyspaceMetadata(Collection<TableMetadata> tables)
        {
            KeyspaceMetadata mock = mock(KeyspaceMetadata.class);
            when(mock.getName()).thenReturn("testKeyspace");
            when(mock.getReplication()).thenReturn(ImmutableMap.of("DC1", "3", "DC2", "3", "DC3", "2"));
            when(mock.isDurableWrites()).thenReturn(true);
            when(mock.isVirtual()).thenReturn(false);
            when(mock.getTables()).thenReturn(tables);
            return mock;
        }
    }
}
