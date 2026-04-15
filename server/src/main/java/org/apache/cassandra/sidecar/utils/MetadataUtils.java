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

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.util.Strings;
import org.apache.cassandra.sidecar.common.server.data.Name;

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
     * known keyspace.
     */
    public static KeyspaceMetadata keyspace(Metadata metadata, Name keyspace)
    {
        return metadata.getKeyspace(keyspace.maybeQuotedName()).orElse(null);
    }

    /**
     * Returns the metadata for a table contained in this keyspace.
     *
     * @param metadata the metadata object.
     * @param table    the name of table to retrieve
     * @return the metadata for table {@code name} if it exists in this keyspace, {@code null}
     * otherwise.
     */
    public static TableMetadata table(KeyspaceMetadata metadata, Name table)
    {
        return metadata.getTable(table.maybeQuotedName()).orElse(null);
    }

    public static String quoteIfNecessary(String literal)
    {
        return Strings.needsDoubleQuotes(literal) ? Strings.doubleQuote(literal) : literal;
    }

    public static String describe(Metadata metadata)
    {
        StringBuilder builder = new StringBuilder();
        metadata.getKeyspaces().values().forEach(ks -> {
            builder.append(ks.describeWithChildren(true)).append("\n");
        });
        return builder.toString();
    }
}
