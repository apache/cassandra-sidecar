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

package org.apache.cassandra.sidecar.testing;

import java.util.Objects;

/**
 * Simple class representing a Cassandra table, used for integration testing
 */
public class QualifiedName
{
    private final String keyspace;
    private final String table;
    private final String maybeQuotedKeyspace;
    private final String maybeQuotedTable;

    public QualifiedName(String keyspace,
                         String table)
    {
        this(keyspace, table, false, false);
    }

    public QualifiedName(String keyspace,
                         String table,
                         boolean quoteKeyspace,
                         boolean quoteTable)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.maybeQuotedKeyspace = quoteKeyspace ? "\"" + keyspace + "\"" : keyspace;
        this.maybeQuotedTable = quoteTable ? "\"" + table + "\"" : table;
    }

    public String keyspace()
    {
        return keyspace;
    }

    public String maybeQuotedKeyspace()
    {
        return maybeQuotedKeyspace;
    }

    public String table()
    {
        return table;
    }

    public String maybeQuotedTable()
    {
        return maybeQuotedTable;
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (object == null || getClass() != object.getClass())
        {
            return false;
        }
        QualifiedName that = (QualifiedName) object;
        return Objects.equals(keyspace, that.keyspace) && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspace, table);
    }

    @Override
    public String toString()
    {
        return String.format("%s.%s", maybeQuotedKeyspace(), maybeQuotedTable());
    }
}
