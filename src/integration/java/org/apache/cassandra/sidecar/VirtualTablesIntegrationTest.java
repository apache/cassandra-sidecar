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
package org.apache.cassandra.sidecar;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.apache.cassandra.sidecar.IntegrationTestUtils.SHARED;

/**
 * Test virtual tables
 */
public class VirtualTablesIntegrationTest
{

    private static final ClusterSpec OLD_VERSION = ClusterSpec.builder()
                                                              .withCassandraVersion("3.11.17")
                                                              .withNodes(1)
                                                              .build();

    private static final ClusterSpec SUPPORTED = ClusterSpec.builder()
                                                            .withCassandraVersion("4.0.0")
                                                            .withNodes(1)
                                                            .build();

    private static void primeVirtualTableSchemas(BoundCluster cluster) throws Exception
    {
        cluster.prime(when("SELECT * FROM system_virtual_schema.keyspaces")
                      .then(rows()
                      .row("keyspace_name", "system_views")
                      .columnTypes("keyspace_name", "varchar")));

        cluster.prime(when("SELECT * FROM system_virtual_schema.tables")
                      .then(rows()
                            .row("keyspace_name", "system_views", "table_name", "caches", "comment", "comment")
                            .row("keyspace_name", "system_views", "table_name", "clients", "comment", "comment")
                            .row("keyspace_name", "system_views", "table_name", "settings", "comment", "comment")
                            .row("keyspace_name", "system_views", "table_name", "sstable_tasks", "comment", "comment")
                            .row("keyspace_name", "system_views", "table_name", "thread_pools", "comment", "comment")
                            .columnTypes("keyspace_name", "varchar", "table_name", "varchar", "comment", "varchar")
                      ));

        //CHECKSTYLE:OFF
        cluster.prime(when("SELECT * FROM system_virtual_schema.columns")
                      .then(rows()
                            .row("keyspace_name", "system_views", "table_name", "caches", "column_name", "capacity_bytes", "clustering_order", "none", "column_name_bytes", "capacity_bytes".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "bigint")
                            .row("keyspace_name", "system_views", "table_name", "caches", "column_name", "entry_count", "clustering_order", "none", "column_name_bytes", "entry_count".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "int")
                            .row("keyspace_name", "system_views", "table_name", "caches", "column_name", "hit_count", "clustering_order", "none", "column_name_bytes", "hit_count".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "bigint")
                            .row("keyspace_name", "system_views", "table_name", "caches", "column_name", "hit_ratio", "clustering_order", "none", "column_name_bytes", "hit_ratio".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "double")
                            .row("keyspace_name", "system_views", "table_name", "caches", "column_name", "name", "clustering_order", "none", "column_name_bytes", "name".getBytes(StandardCharsets.UTF_8), "kind", "partition_key", "position", 0, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "caches", "column_name", "recent_hit_rate_per_second", "clustering_order", "none", "column_name_bytes", "recent_hit_rate_per_second".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "bigint")
                            .row("keyspace_name", "system_views", "table_name", "caches", "column_name", "recent_request_rate_per_second", "clustering_order", "none", "column_name_bytes", "recent_request_rate_per_second".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "bigint")
                            .row("keyspace_name", "system_views", "table_name", "caches", "column_name", "request_count", "clustering_order", "none", "column_name_bytes", "request_count".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "bigint")
                            .row("keyspace_name", "system_views", "table_name", "caches", "column_name", "size_bytes", "clustering_order", "none", "column_name_bytes", "size_bytes".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "bigint")
                            .row("keyspace_name", "system_views", "table_name", "clients", "column_name", "address", "clustering_order", "none", "column_name_bytes", "address".getBytes(StandardCharsets.UTF_8), "kind", "partition_key", "position", 0, "type", "inet")
                            .row("keyspace_name", "system_views", "table_name", "clients", "column_name", "connection_stage", "clustering_order", "none", "column_name_bytes", "connection_stage".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "clients", "column_name", "driver_name", "clustering_order", "none", "column_name_bytes", "driver_name".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "clients", "column_name", "driver_version", "clustering_order", "none", "column_name_bytes", "driver_version".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "clients", "column_name", "hostname", "clustering_order", "none", "column_name_bytes", "hostname".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "clients", "column_name", "port", "clustering_order", "asc", "column_name_bytes", "port".getBytes(StandardCharsets.UTF_8), "kind", "clustering", "position", 0, "type", "int")
                            .row("keyspace_name", "system_views", "table_name", "clients", "column_name", "protocol_version", "clustering_order", "none", "column_name_bytes", "protocol_version".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "int")
                            .row("keyspace_name", "system_views", "table_name", "clients", "column_name", "request_count", "clustering_order", "none", "column_name_bytes", "request_count".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "bigint")
                            .row("keyspace_name", "system_views", "table_name", "clients", "column_name", "ssl_cipher_suite", "clustering_order", "none", "column_name_bytes", "ssl_cipher_suite".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "clients", "column_name", "ssl_enabled", "clustering_order", "none", "column_name_bytes", "ssl_enabled".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "boolean")
                            .row("keyspace_name", "system_views", "table_name", "clients", "column_name", "ssl_protocol", "clustering_order", "none", "column_name_bytes", "ssl_protocol".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "clients", "column_name", "username", "clustering_order", "none", "column_name_bytes", "username".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "settings", "column_name", "name", "clustering_order", "none", "column_name_bytes", "name".getBytes(StandardCharsets.UTF_8), "kind", "partition_key", "position", 0, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "settings", "column_name", "value", "clustering_order", "none", "column_name_bytes", "value".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "sstable_tasks", "column_name", "keyspace_name", "clustering_order", "none", "column_name_bytes", "keyspace_name".getBytes(StandardCharsets.UTF_8), "kind", "partition_key", "position", 0, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "sstable_tasks", "column_name", "kind", "clustering_order", "none", "column_name_bytes", "kind".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "sstable_tasks", "column_name", "progress", "clustering_order", "none", "column_name_bytes", "progress".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "bigint")
                            .row("keyspace_name", "system_views", "table_name", "sstable_tasks", "column_name", "table_name", "clustering_order", "asc", "column_name_bytes", "table_name".getBytes(StandardCharsets.UTF_8), "kind", "clustering", "position", 0, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "sstable_tasks", "column_name", "task_id", "clustering_order", "asc", "column_name_bytes", "task_id".getBytes(StandardCharsets.UTF_8), "kind", "clustering", "position", 1, "type", "uuid")
                            .row("keyspace_name", "system_views", "table_name", "sstable_tasks", "column_name", "total", "clustering_order", "none", "column_name_bytes", "total".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "bigint")
                            .row("keyspace_name", "system_views", "table_name", "sstable_tasks", "column_name", "unit", "clustering_order", "none", "column_name_bytes", "unit".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "thread_pools", "column_name", "active_tasks", "clustering_order", "none", "column_name_bytes", "active_tasks".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "int")
                            .row("keyspace_name", "system_views", "table_name", "thread_pools", "column_name", "active_tasks_limit", "clustering_order", "none", "column_name_bytes", "active_tasks_limit".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "int")
                            .row("keyspace_name", "system_views", "table_name", "thread_pools", "column_name", "blocked_tasks", "clustering_order", "none", "column_name_bytes", "blocked_tasks".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "bigint")
                            .row("keyspace_name", "system_views", "table_name", "thread_pools", "column_name", "blocked_tasks_all_time", "clustering_order", "none", "column_name_bytes", "blocked_tasks_all_time".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "bigint")
                            .row("keyspace_name", "system_views", "table_name", "thread_pools", "column_name", "completed_tasks", "clustering_order", "none", "column_name_bytes", "completed_tasks".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "bigint")
                            .row("keyspace_name", "system_views", "table_name", "thread_pools", "column_name", "name", "clustering_order", "none", "column_name_bytes", "name".getBytes(StandardCharsets.UTF_8), "kind", "partition_key", "position", 0, "type", "text")
                            .row("keyspace_name", "system_views", "table_name", "thread_pools", "column_name", "pending_tasks", "clustering_order", "none", "column_name_bytes", "pending_tasks".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "int")
                            .row("keyspace_name", "system_virtual_schema", "table_name", "columns", "column_name", "clustering_order", "clustering_order", "none", "column_name_bytes", "clustering_order".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "text")
                            .row("keyspace_name", "system_virtual_schema", "table_name", "columns", "column_name", "column_name", "clustering_order", "asc", "column_name_bytes", "column_name".getBytes(StandardCharsets.UTF_8), "kind", "clustering", "position", 1, "type", "text")
                            .row("keyspace_name", "system_virtual_schema", "table_name", "columns", "column_name", "column_name_bytes", "clustering_order", "none", "column_name_bytes", "column_name_bytes".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "blob")
                            .row("keyspace_name", "system_virtual_schema", "table_name", "columns", "column_name", "keyspace_name", "clustering_order", "none", "column_name_bytes", "keyspace_name".getBytes(StandardCharsets.UTF_8), "kind", "partition_key", "position", 0, "type", "text")
                            .row("keyspace_name", "system_virtual_schema", "table_name", "columns", "column_name", "kind", "clustering_order", "none", "column_name_bytes", "kind".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "text")
                            .row("keyspace_name", "system_virtual_schema", "table_name", "columns", "column_name", "position", "clustering_order", "none", "column_name_bytes", "position".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "int")
                            .row("keyspace_name", "system_virtual_schema", "table_name", "columns", "column_name", "table_name", "clustering_order", "asc", "column_name_bytes", "table_name".getBytes(StandardCharsets.UTF_8), "kind", "clustering", "position", 0, "type", "text")
                            .row("keyspace_name", "system_virtual_schema", "table_name", "columns", "column_name", "type", "clustering_order", "none", "column_name_bytes", "type".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "text")
                            .row("keyspace_name", "system_virtual_schema", "table_name", "keyspaces", "column_name", "keyspace_name", "clustering_order", "none", "column_name_bytes", "keyspace_name".getBytes(StandardCharsets.UTF_8), "kind", "partition_key", "position", 0, "type", "text")
                            .row("keyspace_name", "system_virtual_schema", "table_name", "tables", "column_name", "comment", "clustering_order", "none", "column_name_bytes", "comment".getBytes(StandardCharsets.UTF_8), "kind", "regular", "position", -1, "type", "text")
                            .row("keyspace_name", "system_virtual_schema", "table_name", "tables", "column_name", "keyspace_name", "clustering_order", "none", "column_name_bytes", "keyspace_name".getBytes(StandardCharsets.UTF_8), "kind", "partition_key", "position", 0, "type", "text")
                            .row("keyspace_name", "system_virtual_schema", "table_name", "tables", "column_name", "table_name", "clustering_order", "asc", "column_name_bytes", "table_name".getBytes(StandardCharsets.UTF_8), "kind", "clustering", "position", 0, "type", "text")
                            .columnTypes("clustering_order", "varchar", "column_name", "varchar", "column_name_bytes", "blob", "keyspace_name", "varchar", "kind", "varchar", "position", "int", "table_name", "varchar", "type", "varchar")
                      ));
        //CHECKSTYLE:ON
    }

    @Test
    public void pre40()
    {
        try (Server server = Server.builder()
                                   .withMultipleNodesPerIp(true)
                                   .build())
        {
            BoundCluster bCluster = server.register(OLD_VERSION);
            BoundNode node = bCluster.node(0);
            CQLSession session = new CQLSession(node.inetSocketAddress(), SHARED);
            VirtualTables tables = new VirtualTables(session);

            assertThrows(NotImplementedException.class, tables::settings);
            assertThrows(NotImplementedException.class, tables::sstableTasks);
            assertThrows(NotImplementedException.class, tables::threadPools);
        }
    }

    @Test
    public void neverConnectedToConnected() throws Exception
    {
        try (Server server = Server.builder()
                                   .withMultipleNodesPerIp(true)
                                   .build())
        {
            BoundCluster bCluster = server.register(SUPPORTED);
            primeVirtualTableSchemas(bCluster);
            BoundNode node = bCluster.node(0);
            node.stop();
            CQLSession session = new CQLSession(node.inetSocketAddress(), SHARED);
            VirtualTables tables = new VirtualTables(session);

            assertThrows(NoHostAvailableException.class, tables::settings);
            assertThrows(NoHostAvailableException.class, tables::sstableTasks);
            assertThrows(NoHostAvailableException.class, tables::threadPools);
            node.start();
            long start = System.currentTimeMillis();
            while ((session.getLocalCql() == null || session.getLocalCql().getState().getConnectedHosts().isEmpty())
                   && System.currentTimeMillis() - start < 10000)
                Thread.sleep(250);

            // should not throw no host found exception
            tables.settings();
        }
    }

    @Test
    public void testSettings() throws Exception
    {
        try (Server server = Server.builder()
                                   .withMultipleNodesPerIp(true)
                                   .build())
        {
            BoundCluster bCluster = server.register(SUPPORTED);
            primeVirtualTableSchemas(bCluster);

            bCluster.prime(when("SELECT * FROM system_views.settings")
                          .then(rows()
                                .row("name", "a", "value", "1")
                                .row("name", "b", "value", "2")
                                .row("name", "c", "value", "3")
                                .columnTypes("name", "varchar", "value", "varchar")));
            BoundNode node = bCluster.node(0);
            CQLSession session = new CQLSession(node.inetSocketAddress(), SHARED);
            VirtualTables tables = new VirtualTables(session);
            List<VirtualTables.Setting> settings = tables.settings().get().all();
            assertEquals(3, settings.size());
            assertEquals("a", settings.get(0).getName());
            assertEquals("1", settings.get(0).getValue());
            assertEquals("b", settings.get(1).getName());
            assertEquals("2", settings.get(1).getValue());
            assertEquals("c", settings.get(2).getName());
            assertEquals("3", settings.get(2).getValue());
        }
    }

    @Test
    public void testOverFetchSize() throws Exception
    {
        try (Server server = Server.builder()
                                   .withMultipleNodesPerIp(true)
                                   .build())
        {
            BoundCluster bCluster = server.register(SUPPORTED);
            primeVirtualTableSchemas(bCluster);

            PrimeDsl.RowBuilder rowBuilder = rows();
            for (int i = 0; i < 5001; i++)
            {
                rowBuilder.row("name", "a" + i, "value", "" + i);
            }
            bCluster.prime(when("SELECT * FROM system_views.settings")
                           .then(rowBuilder
                                 .columnTypes("name", "varchar", "value", "varchar")));
            BoundNode node = bCluster.node(0);
            CQLSession session = new CQLSession(node.inetSocketAddress(), SHARED);
            VirtualTables tables = new VirtualTables(session);
            List<VirtualTables.Setting> settings = tables.settings().get().all();
            assertEquals(5001, settings.size());

            for (int i = 0; i < 5001; i++)
            {
                assertEquals("a" + i, settings.get(i).getName());
                assertEquals("" + i, settings.get(i).getValue());
            }
        }
    }
}
