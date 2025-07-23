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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.sidecar.common.server.ICassandraAdapter;
import org.apache.cassandra.sidecar.db.schema.TableSchema;

/**
 * Encapsulates the database operations (i.e. CRUD) of objects to a specific
 * local Cassandra node that ICassandraAdapter associates with.
 * @param <T> TableSchema
 */
public abstract class LocalDatabaseAccessor<T extends TableSchema> extends DatabaseAccessor<T>
{
    private final ICassandraAdapter cassandraAdapter;

    protected LocalDatabaseAccessor(T tableSchema, ICassandraAdapter cassandraAdapter)
    {
        super(tableSchema, null);
        this.cassandraAdapter = cassandraAdapter;
    }

    @Override
    public Session session()
    {
        throw new UnsupportedOperationException("LocalDatabaseAccessor does not expose Session object");
    }

    @Override
    protected ResultSet execute(Statement statement)
    {
        return cassandraAdapter.executeLocal(statement);
    }
}
