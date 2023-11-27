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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.sidecar.exceptions.SidecarSchemaModificationException;
import org.jetbrains.annotations.NotNull;

abstract class AbstractSchema
{
    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    private boolean initialized = false;

    public synchronized boolean initialize(@NotNull Session session)
    {
        initialized = initialized || initializeInternal(session);
        return initialized;
    }

    protected PreparedStatement prepare(PreparedStatement cached, Session session, String cqlLiteral)
    {
        return cached == null ? session.prepare(cqlLiteral) : cached;
    }

    protected boolean initializeInternal(@NotNull Session session)
    {
        if (!exists(session.getCluster().getMetadata()))
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

        prepareStatements(session);
        return true;
    }

    protected abstract void prepareStatements(@NotNull Session session);

    protected abstract boolean exists(@NotNull Metadata metadata);

    protected abstract String createSchemaStatement();

    /**
     * Abstract base schema class for table schema
     */
    public abstract static class TableSchema extends AbstractSchema
    {

    }
}
