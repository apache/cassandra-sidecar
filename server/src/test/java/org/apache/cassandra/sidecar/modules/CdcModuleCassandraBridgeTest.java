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

package org.apache.cassandra.sidecar.modules;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.inject.Binding;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.spi.Elements;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.TypeCache;
import org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverter;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.CqlField;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link CassandraBridge} binding of {@link CdcModule}
 */
class CdcModuleCassandraBridgeTest
{
    private static final String KEYSPACE = "cdc_module_bridge_test_ks";
    private static final ReplicationFactor REPLICATION_FACTOR =
    new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, Map.of("DC1", 3));

    private final CdcModule module = new CdcModule();
    private final InstanceMetadataFetcher instanceMetadataFetcher = mock(InstanceMetadataFetcher.class);

    @Test
    void testBindingIsNotASingleton()
    {
        // A singleton would pin the bridge of the version first seen, and fail injection while no instance is up
        Binding<?> binding = Elements.getElements(module).stream()
                                     .filter(element -> element instanceof Binding)
                                     .map(element -> (Binding<?>) element)
                                     .filter(element -> element.getKey().equals(Key.get(CassandraBridge.class)))
                                     .findFirst()
                                     .orElseThrow(AssertionError::new);

        assertThat(Scopes.isSingleton(binding)).isFalse();
    }

    @Test
    void testResolvesTheReleaseVersionOnEachCall()
    {
        when(instanceMetadataFetcher.callOnFirstAvailableInstance(any())).thenReturn("4.1.0", "5.0.4");

        CassandraBridge before = module.cassandraBridge(instanceMetadataFetcher);
        // e.g. after the instances are upgraded
        CassandraBridge after = module.cassandraBridge(instanceMetadataFetcher);

        assertThat(before).isSameAs(CdcBridgeFactory.get(CassandraVersion.FOURONE));
        assertThat(after).isSameAs(CdcBridgeFactory.get(CassandraVersion.FIVEZERO))
                         .isNotSameAs(before);
    }

    @Test
    void testSharesClassLoaderWithCdcBridge()
    {
        when(instanceMetadataFetcher.callOnFirstAvailableInstance(any())).thenReturn("5.0.4");

        CassandraBridge bridge = module.cassandraBridge(instanceMetadataFetcher);
        CdcBridge cdcBridge = CdcBridgeFactory.getCdcBridge(bridge);
        CqlToAvroSchemaConverter avroSchemaConverter = CdcBridgeFactory.getCqlToAvroSchemaConverter(bridge);

        ClassLoader classLoader = bridge.getClass().getClassLoader();
        assertThat(cdcBridge.getClass().getClassLoader()).isSameAs(classLoader);
        assertThat(avroSchemaConverter.getClass().getClassLoader()).isSameAs(classLoader);
    }

    @Test
    void testUdtCollectionFromParsedSchemaIsUsableByTypeCache()
    {
        // Mirrors the CDC flow: Sidecar parses the schema with its bridge, the UDTs are registered
        // in the CDC bridge's types (CassandraSchema#updateCdcSchema), and TypeCache later resolves
        // column types against them when deserializing/serializing mutations.
        when(instanceMetadataFetcher.callOnFirstAvailableInstance(any())).thenReturn("5.0.4");
        CassandraBridge bridge = module.cassandraBridge(instanceMetadataFetcher);
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
