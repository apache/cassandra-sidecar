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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Table schema definition and operations for the {@code Sidecar lease}
 */
@Singleton
public class SidecarLeaseSchema extends TableSchema
{
    private static final String TABLE_NAME = "sidecar_lease_v1";

    private final SchemaKeyspaceConfiguration keyspaceConfig;

    // prepared statements
    private PreparedStatement claimLease;
    private PreparedStatement extendLease;

    @Inject
    public SidecarLeaseSchema(ServiceConfiguration configuration)
    {
        this(configuration.schemaKeyspaceConfiguration());
    }

    public SidecarLeaseSchema(SchemaKeyspaceConfiguration keyspaceConfig)
    {
        this.keyspaceConfig = keyspaceConfig;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String tableName()
    {
        return TABLE_NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String keyspaceName()
    {
        return keyspaceConfig.keyspace();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @VisibleForTesting
    public void prepareStatements(@NotNull Session session)
    {
        // TODO: revisit decision to make TTL a bind parameter instead of burning into the prepared statement
        //       which means it cannot be changed dynamically during the lifetime of the Sidecar process.
        claimLease = prepare(claimLease, session,
                             String.format("INSERT INTO %s.%s (name,owner) VALUES ('cluster_lease_holder',?) " +
                                           "IF NOT EXISTS USING TTL %d",
                                           keyspaceName(), tableName(), keyspaceConfig.leaseSchemaTTLSeconds()));
        extendLease = prepare(extendLease, session,
                              String.format("UPDATE %s.%s USING TTL %d SET owner = ? " +
                                            "WHERE name = 'cluster_lease_holder' IF owner = ?",
                                            keyspaceName(), tableName(), keyspaceConfig.leaseSchemaTTLSeconds()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @VisibleForTesting
    public String createSchemaStatement()
    {
        return String.format("CREATE TABLE IF NOT EXISTS %s.%s ("
                             + "name text PRIMARY KEY,"
                             + "owner text) "
                             + "WITH gc_grace_seconds = 86400",
                             keyspaceName(), tableName());
    }

    public PreparedStatement claimLeaseStatement()
    {
        return claimLease;
    }

    public PreparedStatement extendLeaseStatement()
    {
        return extendLease;
    }
}
