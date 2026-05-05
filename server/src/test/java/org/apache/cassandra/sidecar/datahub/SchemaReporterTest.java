/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.datahub;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.SharedMetricRegistries;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.DefaultListType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import org.apache.cassandra.sidecar.common.server.utils.IOUtils;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetricsImpl;
import org.apache.cassandra.sidecar.metrics.server.SchemaReportingMetrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SchemaReporter}
 */
@SuppressWarnings("resource")
final class SchemaReporterTest
{
    private static final IdentifiersProvider IDENTIFIERS = new TestIdentifiers();
    private static final MetricRegistryFactory FACTORY = new MetricRegistryFactory(SchemaReporterTest.class.getSimpleName(),
                                                                                   Collections.emptyList(),
                                                                                   Collections.emptyList());

    private SidecarMetrics metrics;

    @BeforeEach
    void beforeEach()
    {
        metrics = new SidecarMetricsImpl(FACTORY, null);
    }

    @AfterEach
    void afterEach()
    {
        SharedMetricRegistries.clear();
    }

    @Test
    void testEmptyCluster() throws IOException
    {
        Metadata metadata = mock(Metadata.class);
        when(metadata.getKeyspaces()).thenReturn(Collections.emptyMap());

        JsonEmitter emitter = new JsonEmitter();
        new SchemaReporter(IDENTIFIERS, () -> emitter, metrics).processRequested(metadata);

        String actual = emitter.content();
        String expected = IOUtils.readFully("/datahub/empty_cluster.json");
        assertThat(actual).isEqualTo(expected);

        SchemaReportingMetrics metrics = this.metrics.server().schemaReporting();                      // Validate captured metrics:
        assertThat(metrics.startedRequest.metric.getValue()).isOne();                                  //  * one execution triggered by request
        assertThat(metrics.startedSchedule.metric.getValue()).isZero();                                //  * zero executions triggered by schedule
        assertThat(metrics.finishedSuccess.metric.getValue()).isOne();                                 //  * one execution resulted in success
        assertThat(metrics.finishedFailure.metric.getValue()).isZero();                                //  * zero executions resulted in failure
        assertThat(metrics.sizeAspects.metric.getCount()).isOne();                                     //  * single number of aspects,
        assertThat(metrics.sizeAspects.metric.getSnapshot().getValues()).containsExactly(2L);          //    equal to two
        assertThat(metrics.totalDuration.metric.getCount()).isOne();                            //  * single duration of execution,
        assertThat(metrics.totalDuration.metric.getSnapshot().getValues()[0]).isNotNegative();  //    that is non-negative
    }

    @Test
    void testEmptyKeyspace() throws IOException
    {
        Metadata metadata = mock(Metadata.class);
        CqlIdentifier keyspaceIdentifier = CqlIdentifier.fromCql("sample_keyspace");
        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        when(metadata.getKeyspaces()).thenReturn(Map.of(keyspaceIdentifier, keyspace));
        when(keyspace.getName()).thenReturn(keyspaceIdentifier);
        when(keyspace.getTables()).thenReturn(Collections.emptyMap());

        JsonEmitter emitter = new JsonEmitter();
        new SchemaReporter(IDENTIFIERS, () -> emitter, metrics).processRequested(metadata);

        String actual = emitter.content();
        String expected = IOUtils.readFully("/datahub/empty_keyspace.json");
        assertThat(actual).isEqualTo(expected);

        SchemaReportingMetrics metrics = this.metrics.server().schemaReporting();                      // Validate captured metrics:
        assertThat(metrics.startedRequest.metric.getValue()).isOne();                                  //  * one execution triggered by request
        assertThat(metrics.startedSchedule.metric.getValue()).isZero();                                //  * zero executions triggered by schedule
        assertThat(metrics.finishedSuccess.metric.getValue()).isOne();                                 //  * one execution resulted in success
        assertThat(metrics.finishedFailure.metric.getValue()).isZero();                                //  * zero executions resulted in failure
        assertThat(metrics.sizeAspects.metric.getCount()).isOne();                                     //  * single number of aspects,
        assertThat(metrics.sizeAspects.metric.getSnapshot().getValues()).containsExactly(6L);          //    equal to six
        assertThat(metrics.totalDuration.metric.getCount()).isOne();                            //  * single duration of execution,
        assertThat(metrics.totalDuration.metric.getSnapshot().getValues()[0]).isNotNegative();  //    that is non-negative
    }

    @Test
    void testEmptyTable() throws IOException
    {
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        TableMetadata table = mock(TableMetadata.class);
        CqlIdentifier keyspaceId = CqlIdentifier.fromCql("sample_keyspace");
        when(metadata.getKeyspaces()).thenReturn(Map.of(keyspaceId, keyspace));
        when(keyspace.getName()).thenReturn(keyspaceId);
        CqlIdentifier tableId = CqlIdentifier.fromCql("sample_table");
        when(keyspace.getTables()).thenReturn(Map.of(tableId, table));
        when(table.getKeyspace()).thenReturn(keyspaceId);
        when(table.getName()).thenReturn(tableId);
        when(table.getOptions()).thenReturn(Map.of(CqlIdentifier.fromInternal("comment"), "table comment"));
        when(table.describeWithChildren(anyBoolean())).thenReturn("CREATE TABLE sample_keyspace.sample_table (...);");

        JsonEmitter emitter = new JsonEmitter();
        new SchemaReporter(IDENTIFIERS, () -> emitter, metrics).processRequested(metadata);

        String actual = emitter.content();
        String expected = IOUtils.readFully("/datahub/empty_table.json");
        assertThat(actual).isEqualTo(expected);

        SchemaReportingMetrics metrics = this.metrics.server().schemaReporting();                      // Validate captured metrics:
        assertThat(metrics.startedRequest.metric.getValue()).isOne();                                  //  * one execution triggered by request
        assertThat(metrics.startedSchedule.metric.getValue()).isZero();                                //  * zero executions triggered by schedule
        assertThat(metrics.finishedSuccess.metric.getValue()).isOne();                                 //  * one execution resulted in success
        assertThat(metrics.finishedFailure.metric.getValue()).isZero();                                //  * zero executions resulted in failure
        assertThat(metrics.sizeAspects.metric.getCount()).isOne();                                     //  * single number of aspects,
        assertThat(metrics.sizeAspects.metric.getSnapshot().getValues()).containsExactly(13L);         //    equal to thirteen
        assertThat(metrics.totalDuration.metric.getCount()).isOne();                            //  * single duration of execution,
        assertThat(metrics.totalDuration.metric.getSnapshot().getValues()[0]).isNotNegative();  //    that is non-negative
    }

    @Test
    void testPrimitiveTypes() throws IOException
    {
        CqlSession session = mock(CqlSession.class);
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        TableMetadata table = mock(TableMetadata.class);
        ColumnMetadata pk1 = mock(ColumnMetadata.class);
        ColumnMetadata pk2 = mock(ColumnMetadata.class);
        ColumnMetadata ck1 = mock(ColumnMetadata.class);
        ColumnMetadata ck2 = mock(ColumnMetadata.class);
        ColumnMetadata c1 = mock(ColumnMetadata.class);
        ColumnMetadata c2 = mock(ColumnMetadata.class);
        ColumnMetadata c3 = mock(ColumnMetadata.class);
        ColumnMetadata c4 = mock(ColumnMetadata.class);
        ColumnMetadata c5 = mock(ColumnMetadata.class);
        ColumnMetadata c6 = mock(ColumnMetadata.class);
        ColumnMetadata c7 = mock(ColumnMetadata.class);
        ColumnMetadata c8 = mock(ColumnMetadata.class);
        when(session.getMetadata()).thenReturn(metadata);
        CqlIdentifier keyspaceId = CqlIdentifier.fromCql("sample_keyspace");
        when(metadata.getKeyspaces()).thenReturn(Map.of(keyspaceId, keyspace));
        when(keyspace.getName()).thenReturn(keyspaceId);
        CqlIdentifier tableId = CqlIdentifier.fromCql("sample_table");
        when(keyspace.getTables()).thenReturn(Map.of(tableId, table));
        when(table.getKeyspace()).thenReturn(keyspaceId);
        when(table.getName()).thenReturn(tableId);
        when(table.getOptions()).thenReturn(Collections.emptyMap());
        when(table.getColumns()).thenReturn(ImmutableMap.<CqlIdentifier, ColumnMetadata>builder()
                                            .put(CqlIdentifier.fromInternal("pk1"), pk1)
                                            .put(CqlIdentifier.fromInternal("pk2"), pk2)
                                            .put(CqlIdentifier.fromInternal("ck1"), ck1)
                                            .put(CqlIdentifier.fromInternal("ck2"), ck2)
                                            .put(CqlIdentifier.fromInternal("c1"), c1)
                                            .put(CqlIdentifier.fromInternal("c2"), c2)
                                            .put(CqlIdentifier.fromInternal("c3"), c3)
                                            .put(CqlIdentifier.fromInternal("c4"), c4)
                                            .put(CqlIdentifier.fromInternal("c5"), c5)
                                            .put(CqlIdentifier.fromInternal("c6"), c6)
                                            .put(CqlIdentifier.fromInternal("c7"), c7)
                                            .put(CqlIdentifier.fromInternal("c8"), c8)
                                            .build());
        when(table.getPartitionKey()).thenReturn(ImmutableList.of(pk1, pk2));
        when(table.getClusteringColumns()).thenReturn(Map.of(
        ck1, ClusteringOrder.ASC,
        ck2, ClusteringOrder.ASC));
        when(table.describeWithChildren(anyBoolean())).thenReturn("CREATE TABLE sample_keyspace.sample_table (...);");
        when(table.getOptions()).thenReturn(Map.of(CqlIdentifier.fromInternal("comment"), "table comment"));
        when(pk1.getParent()).thenReturn(tableId);
        when(pk1.getName()).thenReturn(CqlIdentifier.fromInternal("pk1"));
        when(pk1.getType()).thenReturn(new PrimitiveType(ProtocolConstants.DataType.INT));
        when(pk2.getParent()).thenReturn(tableId);
        when(pk2.getName()).thenReturn(CqlIdentifier.fromInternal("pk2"));
        when(pk2.getType()).thenReturn(new PrimitiveType(ProtocolConstants.DataType.FLOAT));
        when(ck1.getParent()).thenReturn(tableId);
        when(ck1.getName()).thenReturn(CqlIdentifier.fromInternal("ck1"));
        when(ck1.getType()).thenReturn(new PrimitiveType(ProtocolConstants.DataType.VARINT));
        when(ck2.getParent()).thenReturn(tableId);
        when(ck2.getName()).thenReturn(CqlIdentifier.fromInternal("ck2"));
        when(ck2.getType()).thenReturn(new PrimitiveType(ProtocolConstants.DataType.DECIMAL));
        when(c1.getParent()).thenReturn(tableId);
        when(c1.getName()).thenReturn(CqlIdentifier.fromInternal("c1"));
        when(c1.getType()).thenReturn(new PrimitiveType(ProtocolConstants.DataType.BOOLEAN));
        when(c2.getParent()).thenReturn(tableId);
        when(c2.getName()).thenReturn(CqlIdentifier.fromInternal("c2"));
        when(c2.getType()).thenReturn(new PrimitiveType(ProtocolConstants.DataType.DATE));
        when(c3.getParent()).thenReturn(tableId);
        when(c3.getName()).thenReturn(CqlIdentifier.fromInternal("c3"));
        when(c3.getType()).thenReturn(new PrimitiveType(ProtocolConstants.DataType.TIME));
        when(c4.getParent()).thenReturn(tableId);
        when(c4.getName()).thenReturn(CqlIdentifier.fromInternal("c4"));
        when(c4.getType()).thenReturn(new PrimitiveType(ProtocolConstants.DataType.ASCII));
        when(c5.getParent()).thenReturn(tableId);
        when(c5.getName()).thenReturn(CqlIdentifier.fromInternal("c6"));
        when(c5.getType()).thenReturn(new PrimitiveType(ProtocolConstants.DataType.VARCHAR));
        when(c6.getParent()).thenReturn(tableId);
        when(c6.getName()).thenReturn(CqlIdentifier.fromInternal("c6"));
        when(c6.getType()).thenReturn(new PrimitiveType(ProtocolConstants.DataType.BLOB));
        when(c7.getParent()).thenReturn(tableId);
        when(c7.getName()).thenReturn(CqlIdentifier.fromInternal("c7"));
        when(c7.getType()).thenReturn(new DefaultListType(new PrimitiveType(ProtocolConstants.DataType.UUID), false));
        when(c8.getParent()).thenReturn(tableId);
        when(c8.getName()).thenReturn(CqlIdentifier.fromInternal("c8"));
        when(c8.getType()).thenReturn(new DefaultMapType(new PrimitiveType(ProtocolConstants.DataType.TIMESTAMP),
                                                         new PrimitiveType(ProtocolConstants.DataType.INET), false));

        JsonEmitter emitter = new JsonEmitter();
        new SchemaReporter(IDENTIFIERS, () -> emitter, metrics).processScheduled(session);

        String actual = emitter.content();
        String expected = IOUtils.readFully("/datahub/primitive_types.json");
        assertThat(actual).isEqualTo(expected);

        SchemaReportingMetrics metrics = this.metrics.server().schemaReporting();                      // Validate captured metrics:
        assertThat(metrics.startedRequest.metric.getValue()).isZero();                                 //  * zero executions triggered by request
        assertThat(metrics.startedSchedule.metric.getValue()).isOne();                                 //  * one execution triggered by schedule
        assertThat(metrics.finishedSuccess.metric.getValue()).isOne();                                 //  * one execution resulted in success
        assertThat(metrics.finishedFailure.metric.getValue()).isZero();                                //  * zero executions resulted in failure
        assertThat(metrics.sizeAspects.metric.getCount()).isOne();                                     //  * single number of aspects,
        assertThat(metrics.sizeAspects.metric.getSnapshot().getValues()).containsExactly(13L); //    equal to thirteen
        assertThat(metrics.totalDuration.metric.getCount()).isOne();                            //  * single duration of execution,
        assertThat(metrics.totalDuration.metric.getSnapshot().getValues()[0]).isNotNegative();  //    that is non-negative
    }

    @Test
    void testUserTypes() throws IOException
    {
        CqlSession cluster = mock(CqlSession.class);
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        TableMetadata table = mock(TableMetadata.class);
        ColumnMetadata pk = mock(ColumnMetadata.class);
        ColumnMetadata ck = mock(ColumnMetadata.class);
        ColumnMetadata udt1 = mock(ColumnMetadata.class);
        ColumnMetadata udt2 = mock(ColumnMetadata.class);
        ColumnMetadata c1 = mock(ColumnMetadata.class);
        ColumnMetadata c2 = mock(ColumnMetadata.class);
        UserDefinedType udt2t = new UserDefinedTypeBuilder("sample_keyspace", "udt2")
                                .withField("c2", new PrimitiveType(ProtocolConstants.DataType.BOOLEAN))
                                .build();
        UserDefinedType udt1t = new UserDefinedTypeBuilder("sample_keyspace", "udt1")
                                .withField("c1", new PrimitiveType(ProtocolConstants.DataType.ASCII))
                                .withField("udt2", udt2t)
                                .build();
        when(cluster.getMetadata()).thenReturn(metadata);
        CqlIdentifier keyspaceId = CqlIdentifier.fromCql("sample_keyspace");
        CqlIdentifier tableId = CqlIdentifier.fromCql("sample_table");
        when(metadata.getKeyspaces()).thenReturn(Map.of(keyspaceId, keyspace));
        when(keyspace.getName()).thenReturn(keyspaceId);
        when(keyspace.getTables()).thenReturn(Map.of(tableId, table));
        when(table.getKeyspace()).thenReturn(keyspaceId);
        when(table.getName()).thenReturn(tableId);
        Map<CqlIdentifier, ColumnMetadata> columns = Maps.newLinkedHashMap();
        columns.put(CqlIdentifier.fromCql("pk"), pk);
        columns.put(CqlIdentifier.fromCql("cd"), ck);
        columns.put(CqlIdentifier.fromCql("udt1"), udt1);
        when(table.getColumns()).thenReturn(columns);
        when(table.getPartitionKey()).thenReturn(ImmutableList.of(pk));
        when(table.getClusteringColumns()).thenReturn(Map.of(ck, ClusteringOrder.ASC));
        when(table.describeWithChildren(anyBoolean())).thenReturn("CREATE TABLE sample_keyspace.sample_table (...);");
        when(table.getOptions()).thenReturn(Map.of(CqlIdentifier.fromInternal("comment"), "table comment"));
        when(pk.getParent()).thenReturn(tableId);
        when(pk.getName()).thenReturn(CqlIdentifier.fromInternal("pk"));
        when(pk.getType()).thenReturn(new PrimitiveType(ProtocolConstants.DataType.INT));
        when(ck.getParent()).thenReturn(tableId);
        when(ck.getName()).thenReturn(CqlIdentifier.fromInternal("ck"));
        when(ck.getType()).thenReturn(new PrimitiveType(ProtocolConstants.DataType.FLOAT));
        when(udt1.getParent()).thenReturn(tableId);
        when(udt1.getName()).thenReturn(CqlIdentifier.fromInternal("udt1"));
        when(udt1.getType()).thenReturn(udt1t);
        when(udt2.getParent()).thenReturn(tableId);
        when(udt2.getName()).thenReturn(CqlIdentifier.fromInternal("udt2"));
        when(udt2.getType()).thenReturn(udt2t);
        when(c1.getParent()).thenReturn(tableId);
        when(c1.getName()).thenReturn(CqlIdentifier.fromInternal("c1"));
        when(c1.getType()).thenReturn(new PrimitiveType(ProtocolConstants.DataType.ASCII));
        when(c2.getParent()).thenReturn(tableId);
        when(c2.getName()).thenReturn(CqlIdentifier.fromInternal("c2"));
        when(c2.getType()).thenReturn(new PrimitiveType(ProtocolConstants.DataType.BOOLEAN));

        JsonEmitter emitter = new JsonEmitter();
        new SchemaReporter(IDENTIFIERS, () -> emitter, metrics).processScheduled(cluster);

        String actual = emitter.content();
        String expected = IOUtils.readFully("/datahub/user_types.json");
        assertThat(actual).isEqualTo(expected);

        SchemaReportingMetrics metrics = this.metrics.server().schemaReporting();                      // Validate captured metrics:
        assertThat(metrics.startedRequest.metric.getValue()).isZero();                                 //  * zero executions triggered by request
        assertThat(metrics.startedSchedule.metric.getValue()).isOne();                                 //  * one execution triggered by schedule
        assertThat(metrics.finishedSuccess.metric.getValue()).isOne();                                 //  * one execution resulted in success
        assertThat(metrics.finishedFailure.metric.getValue()).isZero();                                //  * zero executions resulted in failure
        assertThat(metrics.sizeAspects.metric.getCount()).isOne();                                     //  * single number of aspects,
        assertThat(metrics.sizeAspects.metric.getSnapshot().getValues()).containsExactly(13L); //    equal to thirteen
        assertThat(metrics.totalDuration.metric.getCount()).isOne();                            //  * single duration of execution,
        assertThat(metrics.totalDuration.metric.getSnapshot().getValues()[0]).isNotNegative();  //    that is non-negative
    }
}
