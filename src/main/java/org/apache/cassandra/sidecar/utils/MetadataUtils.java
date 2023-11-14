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
import org.apache.cassandra.sidecar.common.data.Keyspace;
import org.apache.cassandra.sidecar.common.data.Table;

/**
 * Utilities for {@link Metadata} operations
 */
public class MetadataUtils
{
    /**
     * Returns the metadata of a keyspace given its name.
     *
     * @param metadata the metadata object.
     * @param keyspace the name of the keyspace for which metadata should be returned.
     * @return the metadata of the requested keyspace or {@code null} if {@code keyspace} is not a
     * known keyspace. Note that the result might be stale or null if metadata was explicitly
     * disabled with {@link QueryOptions#setMetadataEnabled(boolean)}.
     */
    public static KeyspaceMetadata keyspace(Metadata metadata, Keyspace keyspace)
    {
        return metadata.getKeyspace(keyspace.maybeQuotedName());
    }

    /**
     * Returns the metadata for a table contained in this keyspace.
     *
     * @param metadata  the metadata object.
     * @param table the name of table to retrieve
     * @return the metadata for table {@code name} if it exists in this keyspace, {@code null}
     * otherwise.
     */
    public static TableMetadata table(KeyspaceMetadata metadata, Table table)
    {
        return metadata.getTable(table.maybeQuotedName());
    }
}
