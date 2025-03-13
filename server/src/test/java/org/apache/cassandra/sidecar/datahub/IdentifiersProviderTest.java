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

package org.apache.cassandra.sidecar.datahub;

import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import org.jetbrains.annotations.NotNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link IdentifiersProvider}
 */
final class IdentifiersProviderTest
{
    private static final IdentifiersProvider IDENTIFIERS = new TestIdentifiers();  // Single instance of immutable class

    /**
     * Tests the default values for the individual components of the composite identifier
     */
    @Test
    void testValues()
    {
        String organization = "Cassandra";
        String platform = "cassandra";
        String environment = "ENVIRONMENT";
        String application = "application";
        String cluster = "cluster";

        assertThat(IDENTIFIERS.organization()).isEqualTo(organization);
        assertThat(IDENTIFIERS.platform()).isEqualTo(platform);
        assertThat(IDENTIFIERS.environment()).isEqualTo(environment);
        assertThat(IDENTIFIERS.application()).isEqualTo(application);
        assertThat(IDENTIFIERS.cluster()).isEqualTo(cluster);
    }

    /**
     * Tests the generation algorithm for the unique identifier of the composite identifier
     */
    @Test
    void testIdentifier()
    {
        UUID identifier = UUID.fromString("ace3ba6b-49b2-3dd5-955a-1de13730188b");  // Calculated deterministically

        assertThat(IDENTIFIERS.identifier()).isEqualTo(identifier);
    }

    /**
     * Tests the generation formats for the URN identifiers of the composite identifier
     */
    @Test
    void testUrns()
    {
        KeyspaceMetadata keyspace = mock(KeyspaceMetadata.class);
        TableMetadata table = mock(TableMetadata.class);
        when(keyspace.getName()).thenReturn("keyspace");
        when(table.getName()).thenReturn("table");
        when(keyspace.getTables()).thenReturn(Collections.singleton(table));
        when(table.getKeyspace()).thenReturn(keyspace);

        String urnDataPlatform = "urn:li:dataPlatform:cassandra";
        String urnDataPlatformInstance = "urn:li:dataPlatformInstance:" +
                                         "(urn:li:dataPlatform:cassandra,ace3ba6b-49b2-3dd5-955a-1de13730188b)";
        String urnContainer = "urn:li:container:ace3ba6b-49b2-3dd5-955a-1de13730188b_keyspace";
        String urnDataset = "urn:li:dataset:" +
                            "(urn:li:dataPlatform:cassandra,ace3ba6b-49b2-3dd5-955a-1de13730188b.keyspace.table,PROD)";

        assertThat(IDENTIFIERS.urnDataPlatform()).isEqualTo(urnDataPlatform);
        assertThat(IDENTIFIERS.urnDataPlatformInstance()).isEqualTo(urnDataPlatformInstance);
        assertThat(IDENTIFIERS.urnContainer(keyspace)).isEqualTo(urnContainer);
        assertThat(IDENTIFIERS.urnDataset(table)).isEqualTo(urnDataset);
    }

    /**
     * Tests the method implementations of {@link IdentifiersProvider#equals} and {@link IdentifiersProvider#hashCode}
     */
    @Test
    void testEqualsAndHashCode()
    {
        IdentifiersProvider same = new IdentifiersProvider()
        {
            @Override
            @NotNull
            public String cluster()
            {
                return "cluster";
            }
        };
        IdentifiersProvider different = new IdentifiersProvider()
        {
            @Override
            @NotNull
            public String cluster()
            {
                return "qwerty";
            }
        };

        assertThat(IDENTIFIERS).isEqualTo(same).isNotEqualTo(different);
        assertThat(IDENTIFIERS).hasSameHashCodeAs(same).doesNotHaveSameHashCodeAs(different);
    }

    /**
     * Tests the method implementation of {@link IdentifiersProvider#toString}
     */
    @Test
    void testToString()
    {
        String string = "org.apache.cassandra.sidecar.datahub.TestIdentifiers" +
                        "(Cassandra,cassandra,ENVIRONMENT,application,cluster,ace3ba6b-49b2-3dd5-955a-1de13730188b)";

        assertThat(IDENTIFIERS).hasToString(string);
    }
}
