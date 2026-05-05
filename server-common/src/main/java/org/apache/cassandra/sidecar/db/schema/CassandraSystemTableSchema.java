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

package org.apache.cassandra.sidecar.db.schema;

import java.util.function.Predicate;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import org.jetbrains.annotations.NotNull;

/**
 * Schema for existing tables in Cassandra.
 */
public abstract class CassandraSystemTableSchema extends TableSchema
{
    @Override
    protected boolean exists(@NotNull Metadata metadata)
    {
        return true;
    }

    @Override
    protected boolean initializeInternal(@NotNull CqlSession session,
                                         @NotNull Predicate<AbstractSchema> shouldCreateSchema)
    {
        prepareStatements(session);
        logger.info("{} Cassandra system table schema is initialized", this.getClass().getSimpleName());
        return true;
    }

    /**
     * Should not create new tables, since schema is for existing tables.
     */
    @Override
    protected String createSchemaStatement()
    {
        return null;
    }

    @Override
    public ConsistencyLevel getConsistencyLevel()
    {
        return ConsistencyLevel.ONE;
    }
}
