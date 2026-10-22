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

package org.apache.cassandra.sidecar.bridge;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.TypeCache;
import org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverter;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link CassandraBridgeFactory}
 */
class CassandraBridgeFactoryTest
{
    private static final String CASSANDRA_VERSION = "5.0.0";
    private static final String KEYSPACE = "bridge_factory_test_ks";
    private static final ReplicationFactor REPLICATION_FACTOR =
    new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, Map.of("DC1", 3));

    private final CassandraBridgeFactory factory = new CassandraBridgeFactory();

    @Test
    void testSharesClassLoaderWithCdcBridge()
    {
        CassandraBridge bridge = factory.get(CASSANDRA_VERSION);
        CdcBridge cdcBridge = factory.getCdcBridge(bridge);
        CqlToAvroSchemaConverter avroSchemaConverter = factory.getCqlToAvroSchemaConverter(bridge);

        assertThat(bridge).isSameAs(CdcBridgeFactory.get(CASSANDRA_VERSION));
        assertThat(cdcBridge).isSameAs(CdcBridgeFactory.getCdcBridge(bridge));
        assertThat(avroSchemaConverter).isSameAs(CdcBridgeFactory.getCqlToAvroSchemaConverter(bridge));
        ClassLoader classLoader = bridge.getClass().getClassLoader();
        assertThat(cdcBridge.getClass().getClassLoader()).isSameAs(classLoader);
        assertThat(avroSchemaConverter.getClass().getClassLoader()).isSameAs(classLoader);
    }

    @Test
    void testCassandraVersion()
    {
        assertThat(factory.cassandraVersion("5.0.4")).contains(CassandraVersion.FIVEZERO);
        assertThat(factory.cassandraVersion("4.0.17")).contains(CassandraVersion.FOURZERO);
        assertThat(factory.cassandraVersion("9.9.9")).isEmpty();
        assertThatThrownBy(() -> factory.get("9.9.9")).isInstanceOf(IllegalArgumentException.class)
                                                    .hasMessageContaining("9.9.9");
    }

    @Test
    void testGetByReleaseVersionUsesCassandraVersion()
    {
        CassandraBridgeFactory overriding = new CassandraBridgeFactory()
        {
            @Override
            public Optional<CassandraVersion> cassandraVersion(String releaseVersion)
            {
                return "custom-1.0".equals(releaseVersion) ? Optional.of(CassandraVersion.FIVEZERO) : super.cassandraVersion(releaseVersion);
            }
        };

        assertThat(overriding.get("custom-1.0")).isSameAs(CdcBridgeFactory.get(CassandraVersion.FIVEZERO));
    }

    @Test
    void testUdtCollectionFromParsedSchemaIsUsableByTypeCache()
    {
        // Mirrors the CDC flow: Sidecar parses the schema with its bridge, the UDTs are registered
        // in the CDC bridge's types (CassandraSchema#updateCdcSchema), and TypeCache later resolves
        // column types against them when deserializing/serializing mutations.
        CassandraBridge bridge = factory.get(CASSANDRA_VERSION);
        String udt = "CREATE TYPE " + KEYSPACE + ".task_result (status text, message text);";
        String table = "CREATE TABLE " + KEYSPACE + ".task (id uuid PRIMARY KEY, name text, " +
                       "results list<frozen<task_result>>) WITH cdc = true;";
        CqlTable cqlTable = bridge.buildSchema(table, KEYSPACE, REPLICATION_FACTOR,
                                               Partitioner.Murmur3Partitioner, Set.of(udt));
        cqlTable.udts().forEach(u -> CdcBridgeFactory.get(bridge.getVersion())
                                                     .cassandraTypes()
                                                     .updateUDTs(KEYSPACE, u));

        CqlField.CqlType type = TypeCache.get(bridge.getVersion()).getType(KEYSPACE, "list<frozen<task_result>>");

        // an empty list is enough to build the element serializer, which previously failed with a
        // ClassCastException because the UDT was loaded by a different classloader
        ByteBuffer emptyList = ByteBuffer.allocate(4).putInt(0).flip();
        assertThat(type.deserializeToJavaType(emptyList)).asList().isEmpty();
    }
}
