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

package org.apache.cassandra.sidecar.cdc;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.cdc.api.SchemaSupplier;
import org.apache.cassandra.spark.data.CqlTable;
import org.apache.cassandra.spark.data.ReplicationFactor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for {@link SidecarReplicationFactorSupplier}. */
public class SidecarReplicationFactorSupplierTest
{
    private static final String DC = "dc1";

    private CdcOptions cdcOptions;
    private SchemaSupplier schemaSupplier;
    private SidecarReplicationFactorSupplier supplier;

    @BeforeEach
    void setup()
    {
        cdcOptions = mock(CdcOptions.class);
        schemaSupplier = mock(SchemaSupplier.class);
        when(cdcOptions.dc()).thenReturn(DC);
        supplier = new SidecarReplicationFactorSupplier(cdcOptions, schemaSupplier);
    }

    @Test
    void getReplicationFactorDelegatesToCdcOptions()
    {
        ReplicationFactor rf = ReplicationFactor.simpleStrategy(3);
        when(cdcOptions.replicationFactor("ks1")).thenReturn(rf);

        assertThat(supplier.getReplicationFactor("ks1")).isSameAs(rf);
        verify(cdcOptions).replicationFactor("ks1");
    }

    @Test
    void getMaximalReplicationFactorReturnsHighestRfForDc()
    {
        CqlTable t1 = tableWithNtsRf(Map.of(DC, 2));
        CqlTable t2 = tableWithNtsRf(Map.of(DC, 5));
        CqlTable t3 = tableWithNtsRf(Map.of(DC, 3));
        when(schemaSupplier.getCdcEnabledTables()).thenReturn(CompletableFuture.completedFuture(Set.of(t1, t2, t3)));

        ReplicationFactor result = supplier.getMaximalReplicationFactor();

        assertThat(result.getOptions().get(DC)).isEqualTo(5);
    }

    @Test
    void getMaximalReplicationFactorFallsBackToSimpleStrategy1WhenNoDcMatch()
    {
        CqlTable table = tableWithNtsRf(Map.of("other_dc", 5));
        when(schemaSupplier.getCdcEnabledTables()).thenReturn(CompletableFuture.completedFuture(Set.of(table)));

        ReplicationFactor result = supplier.getMaximalReplicationFactor();

        assertThat(result).isEqualTo(ReplicationFactor.simpleStrategy(3));
    }

    @Test
    void getMaximalReplicationFactorFallsBackToSimpleStrategy1WhenNoTablesPresent()
    {
        when(schemaSupplier.getCdcEnabledTables()).thenReturn(CompletableFuture.completedFuture(Set.of()));

        ReplicationFactor result = supplier.getMaximalReplicationFactor();

        assertThat(result).isEqualTo(ReplicationFactor.simpleStrategy(3));
    }

    private static CqlTable tableWithNtsRf(Map<String, Integer> dcOptions)
    {
        CqlTable table = mock(CqlTable.class);
        when(table.replicationFactor()).thenReturn(
            new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy, dcOptions));
        return table;
    }
}
