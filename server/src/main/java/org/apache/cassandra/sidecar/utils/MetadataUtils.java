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

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.TableMetadata;
import org.apache.cassandra.sidecar.common.server.data.Name;

/**
 * Utilities for {@link Metadata} operations
 */
public class MetadataUtils
{
    /**
     * Returns the metadata of a keyspace given its name.
     * Delegates to {@link #keyspace(Metadata, String)} using the unquoted name. CQL quote context
     * from the source {@link Name} is intentionally discarded: the two-step lookup in
     * {@link #keyspace(Metadata, String)} handles both unquoted (driver case-folds) and CQL-quoted
     * mixed-case keyspaces via its {@code quoteIfNecessary} fallback.
     *
     * @param metadata the metadata object.
     * @param keyspace the name of the keyspace for which metadata should be returned.
     * @return the metadata of the requested keyspace or {@code null} if {@code keyspace} is not a
     * known keyspace. Note that the result might be stale or null if metadata was explicitly
     * disabled with {@link QueryOptions#setMetadataEnabled(boolean)}.
     */
    public static KeyspaceMetadata keyspace(Metadata metadata, Name keyspace)
    {
        return keyspace(metadata, keyspace.name());
    }

    /**
     * Returns the metadata of a keyspace given its name as stored in sidecar (without CQL quote context).
     * Tries a raw lookup first (driver folds to lowercase, handles unquoted names), then falls back to a
     * quoted lookup to handle mixed-case keyspaces created with CQL double-quotes.
     *
     * @param metadata the metadata object.
     * @param keyspace the keyspace name as stored in sidecar.
     * @return the metadata of the requested keyspace or {@code null} if not found.
     */
    public static KeyspaceMetadata keyspace(Metadata metadata, String keyspace)
    {
        if (keyspace == null)
            return null;
        KeyspaceMetadata ks = metadata.getKeyspace(keyspace);
        if (ks == null)
        {
            ks = metadata.getKeyspace(Metadata.quoteIfNecessary(keyspace));
        }
        return ks;
    }

    /**
     * Returns the metadata for a table contained in this keyspace.
     * Delegates to {@link #table(KeyspaceMetadata, String)} using the unquoted name so that mixed-case
     * tables stored without CQL double-quotes (e.g. from URL path parameters) are found via the
     * quoted fallback in that overload.
     *
     * @param metadata the metadata object.
     * @param table    the name of table to retrieve
     * @return the metadata for table {@code name} if it exists in this keyspace, {@code null}
     * otherwise.
     */
    public static TableMetadata table(KeyspaceMetadata metadata, Name table)
    {
        return table(metadata, table.name());
    }

    /**
     * Returns the metadata for a table contained in this keyspace given its name as stored in sidecar.
     * Tries a raw lookup first, then falls back to a quoted lookup to handle mixed-case tables created
     * with CQL double-quotes.
     *
     * @param metadata the metadata object.
     * @param table    the table name as stored in sidecar.
     * @return the metadata for the table or {@code null} if not found.
     */
    public static TableMetadata table(KeyspaceMetadata metadata, String table)
    {
        if (table == null)
            return null;
        TableMetadata tm = metadata.getTable(table);
        if (tm == null)
        {
            tm = metadata.getTable(Metadata.quoteIfNecessary(table));
        }
        return tm;
    }
}
