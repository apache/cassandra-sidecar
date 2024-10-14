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

package org.apache.cassandra.sidecar.adapters.base.db.schema;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.db.schema.TableSchema;
import org.jetbrains.annotations.NotNull;

/**
 * Holds the prepared statements for operations related to client connection stats retrieved from
 * the "clients" virtual table
 */
public class ConnectedClientsSchema extends TableSchema
{
    private static final String TABLE_NAME = "clients";
    private static final String KEYSPACE_NAME = "system_views";

    private PreparedStatement statsStatement;
    private PreparedStatement connectionsByUserStatement;

    @Override
    protected String keyspaceName()
    {
        return KEYSPACE_NAME;
    }

    public void prepareStatements(@NotNull Session session)
    {
        statsStatement = prepare(statsStatement, session, statsStatement());
        connectionsByUserStatement = prepare(connectionsByUserStatement, session, selectConnectionsByUserStatement());
    }

    @Override
    protected boolean initializeInternal(@NotNull Session session)
    {
        prepareStatements(session);
        return true;
    }

    @Override
    protected String createSchemaStatement()
    {
        return null;
    }

    @Override
    protected String tableName()
    {
        return TABLE_NAME;
    }

    public PreparedStatement stats()
    {
        return statsStatement;
    }

    public PreparedStatement connectionsByUser()
    {
        return connectionsByUserStatement;
    }

    private String statsStatement()
    {
        return String.format("SELECT * FROM %s.%s;", KEYSPACE_NAME, TABLE_NAME);
    }

    static String selectConnectionsByUserStatement()
    {
        return String.format("SELECT username, COUNT(*) AS connection_count " +
                             "FROM %s.%s;",
                             KEYSPACE_NAME,
                             TABLE_NAME);
    }
}
