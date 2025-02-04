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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.exceptions.SidecarSchemaModificationException;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract schema
 */
public abstract class AbstractSchema
{
    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    private volatile boolean initialized = false;

    public synchronized boolean initialize(@NotNull Session session, @NotNull Predicate<AbstractSchema> shouldCreateSchema)
    {
        initialized = initialized || initializeInternal(session, shouldCreateSchema);
        return initialized;
    }

    /**
     * @return whether schema is initialized
     */
    public boolean isInitialized()
    {
        return initialized;
    }

    protected PreparedStatement prepare(PreparedStatement cached, Session session, String cqlLiteral)
    {
        return cached == null ? session.prepare(cqlLiteral).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM) : cached;
    }

    protected boolean initializeInternal(@NotNull Session session,
                                         @NotNull Predicate<AbstractSchema> shouldCreateSchema)
    {
        if (!exists(session.getCluster().getMetadata()))
        {
            if (shouldCreateSchema.test(this))
            {
                try
                {
                    ResultSet res = session.execute(createSchemaStatement());
                    if (!res.getExecutionInfo().isSchemaInAgreement())
                    {
                        logger.warn("Schema is not yet in agreement.");
                        return false;
                    }
                }
                catch (Exception exception)
                {
                    String schemaName = this.getClass().getSimpleName();
                    throw new SidecarSchemaModificationException("Failed to modify schema for " + schemaName, exception);
                }
            }
            else
            {
                // We wait until the schema is created by the single instance executor for example
                return false;
            }
        }

        prepareStatements(session);
        logger.debug("{} is initialized!", this.getClass().getSimpleName());
        return true;
    }

    /**
     * @return the name of the Cassandra keyspace
     */
    protected abstract String keyspaceName();

    /**
     * Prepares the statements needed during the lifecycle of the application
     *
     * @param session the CQL session
     */
    protected abstract void prepareStatements(@NotNull Session session);

    /**
     * @param metadata the cluster metadata
     * @return {@code true} if the schema already exists in the database, {@code false} otherwise
     */
    protected abstract boolean exists(@NotNull Metadata metadata);

    /**
     * @return the statement to create the schema
     */
    protected abstract String createSchemaStatement();
}
