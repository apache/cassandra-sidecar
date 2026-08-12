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

package org.apache.cassandra.sidecar.utils;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MetadataUtils#keyspace(Metadata, String)} and {@link MetadataUtils#table(KeyspaceMetadata, String)}
 */
public class MetadataUtilsTest
{
    @Test
    void keyspace_quotedMixedCase_findsViaQuotedFallback()
    {
        // "MyKeyspace" created with CQL quotes: Cassandra internal name is MyKeyspace (case-preserved).
        // Sidecar stores MyKeyspace (no quote chars). Raw lookup folds to mykeyspace → not found.
        // Quoted fallback finds the case-sensitive entry.
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        String keyspace = "MyKeyspace";
        when(metadata.getKeyspace(keyspace)).thenReturn(null);
        when(metadata.getKeyspace(Metadata.quoteIfNecessary(keyspace))).thenReturn(keyspaceMetadata);

        assertThat(MetadataUtils.keyspace(metadata, keyspace)).isSameAs(keyspaceMetadata);
        verify(metadata).getKeyspace(Metadata.quoteIfNecessary(keyspace));
    }

    @Test
    void keyspace_unquotedMixedCase_findsViaRawLookupWithoutFallback()
    {
        // MyKeyspace created without CQL quotes: Cassandra folds to mykeyspace internally.
        // Sidecar stores MyKeyspace. The raw lookup succeeds (driver handles case-folding);
        // the quoted fallback must NOT be reached — that would look for case-sensitive MyKeyspace
        // and return null, which was the regression introduced by quoteIfNecessary.
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getName()).thenReturn("mykeyspace");
        String keyspace = "MyKeyspace";
        when(metadata.getKeyspace(keyspace)).thenReturn(keyspaceMetadata);

        assertThat(MetadataUtils.keyspace(metadata, keyspace)).isSameAs(keyspaceMetadata);
        verify(metadata, never()).getKeyspace(Metadata.quoteIfNecessary(keyspace));
    }

    @Test
    void keyspace_lowercase_findsViaRawLookup()
    {
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(metadata.getKeyspace("myks")).thenReturn(keyspaceMetadata);

        assertThat(MetadataUtils.keyspace(metadata, "myks")).isSameAs(keyspaceMetadata);
    }

    @Test
    void keyspace_nonExistent_returnsNull()
    {
        Metadata metadata = mock(Metadata.class);
        when(metadata.getKeyspace("nonexistent")).thenReturn(null);
        when(metadata.getKeyspace(Metadata.quoteIfNecessary("nonexistent"))).thenReturn(null);

        assertThat(MetadataUtils.keyspace(metadata, "nonexistent")).isNull();
    }

    @Test
    void table_quotedMixedCase_findsViaQuotedFallback()
    {
        // "MyTable" created with CQL quotes: Cassandra internal name is MyTable (case-preserved).
        // Sidecar stores MyTable (no quote chars). Raw lookup folds to mytable → not found.
        // Quoted fallback finds the case-sensitive entry.
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        TableMetadata tableMetadata = mock(TableMetadata.class);
        String table = "MyTable";
        when(keyspaceMetadata.getTable(table)).thenReturn(null);
        when(keyspaceMetadata.getTable(Metadata.quoteIfNecessary(table))).thenReturn(tableMetadata);

        assertThat(MetadataUtils.table(keyspaceMetadata, table)).isSameAs(tableMetadata);
        verify(keyspaceMetadata).getTable(Metadata.quoteIfNecessary(table));
    }

    @Test
    void table_unquotedMixedCase_findsViaRawLookupWithoutFallback()
    {
        // MyTable created without CQL quotes: Cassandra folds to mytable internally.
        // Sidecar stores MyTable. The raw lookup succeeds (driver handles case-folding);
        // the quoted fallback must NOT be reached.
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        TableMetadata tableMetadata = mock(TableMetadata.class);
        String table = "MyTable";
        when(keyspaceMetadata.getTable(table)).thenReturn(tableMetadata);

        assertThat(MetadataUtils.table(keyspaceMetadata, table)).isSameAs(tableMetadata);
        verify(keyspaceMetadata, never()).getTable(Metadata.quoteIfNecessary(table));
    }

    @Test
    void table_lowercase_findsViaRawLookup()
    {
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        TableMetadata tableMetadata = mock(TableMetadata.class);
        when(keyspaceMetadata.getTable("mytable")).thenReturn(tableMetadata);

        assertThat(MetadataUtils.table(keyspaceMetadata, "mytable")).isSameAs(tableMetadata);
    }

    @Test
    void table_nonExistent_returnsNull()
    {
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        when(keyspaceMetadata.getTable("nonexistent")).thenReturn(null);
        when(keyspaceMetadata.getTable(Metadata.quoteIfNecessary("nonexistent"))).thenReturn(null);

        assertThat(MetadataUtils.table(keyspaceMetadata, "nonexistent")).isNull();
    }
}
