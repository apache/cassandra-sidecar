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

import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
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
    public SidecarLeaseSchema(SidecarConfiguration configuration)
    {
        this.keyspaceConfig = configuration.serviceConfiguration().schemaKeyspaceConfiguration();
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
        claimLease = prepare(claimLease, session,
                             String.format("INSERT INTO %s.%s (name,owner) VALUES ('single_sidecar_instance_executor',?) IF NOT EXISTS",
                                           keyspaceName(), tableName()));
        extendLease = prepare(extendLease, session,
                              String.format("UPDATE %s.%s SET owner = ? WHERE name = 'single_sidecar_instance_executor' IF owner = ?",
                                            keyspaceName(), tableName()));
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
                             + "WITH default_time_to_live = %d",
                             keyspaceName(), tableName(), TimeUnit.MINUTES.toSeconds(10));
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
