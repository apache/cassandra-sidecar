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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.TableSchema;
import org.jetbrains.annotations.NotNull;

/**
 * Encapsulates the database operations (i.e. CRUD) of objects.
 * @param <T> TableSchema
 *
 */
public abstract class DatabaseAccessor<T extends TableSchema>
{
    public final CQLSessionProvider cqlSessionProvider;
    protected final T tableSchema;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected DatabaseAccessor(T tableSchema,
                               CQLSessionProvider sessionProvider)
    {
        this.tableSchema = tableSchema;
        this.cqlSessionProvider = sessionProvider;
    }

    @NotNull
    public Session session()
    {
        Session session;
        try
        {
            session = cqlSessionProvider.get();
        }
        catch (Exception e)
        {
            throw new IllegalStateException("Instance is not ready", e);
        }
        if (session == null)
        {
            logger.error("Unable to obtain session");
            throw new IllegalStateException("Could not obtain session");
        }
        return session;
    }

    protected ResultSet execute(Statement statement)
    {
        return session().execute(statement);
    }
}
