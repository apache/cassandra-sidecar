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

package org.apache.cassandra.sidecar.db;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Component allowing to read CQL schema by executing {@code DESCRIBE} statement.
 * TODO: Remove after upgrade to Java driver 4.x (CASSSIDECAR-421).
 */
@Singleton
public class CQLSchemaAccessor
{
    private final CQLSessionProvider sessionProvider;

    public CQLSchemaAccessor(CQLSessionProvider sessionProvider)
    {
        this.sessionProvider = sessionProvider;
    }

    @NotNull
    public Set<Name> getKeyspaces()
    {
        Session session = sessionProvider.get();
        Set<Name> keyspaces = new HashSet<>();
        List<Row> rows = session.execute("DESCRIBE KEYSPACES").all();
        for (Row row : rows)
        {
            Name keyspaceName = new Name(row.getString("keyspace_name"));
            keyspaces.add(keyspaceName);
        }
        return keyspaces;
    }

    @Nullable
    public List<String> getKeyspaceSchema(@NotNull Name keyspace)
    {
        Session session = sessionProvider.get();
        String statement = String.format("DESCRIBE KEYSPACE %s", keyspace.maybeQuotedName());
        return describe(session, statement);
    }

    @Nullable
    public List<String> getTableSchema(@NotNull Name keyspace, @NotNull Name table)
    {
        Session session = sessionProvider.get();
        String statement = String.format("DESCRIBE TABLE %s.%s", keyspace.maybeQuotedName(), table.maybeQuotedName());
        return describe(session, statement);
    }

    private List<String> describe(Session session, String describeStatement)
    {
        try
        {
            List<Row> rows = session.execute(describeStatement).all();
            List<String> result = new ArrayList<>(rows.size());
            for (Row row : rows)
            {
                String createStatement = row.getString("create_statement");
                result.add(createStatement);
            }
            return result;
        }
        catch (InvalidQueryException e)
        {
            // keyspace or table not found
            return null;
        }
    }
}
