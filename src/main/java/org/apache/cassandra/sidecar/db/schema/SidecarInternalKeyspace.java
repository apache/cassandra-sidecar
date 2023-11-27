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

import java.util.HashSet;
import java.util.Set;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * Manages table setup needed for features provided by Sidecar. For e.g. creates schema needed for
 * {@link org.apache.cassandra.sidecar.db.RestoreJob} table and {@link org.apache.cassandra.sidecar.db.RestoreSlice}
 * table
 */
@Singleton
public class SidecarInternalKeyspace extends AbstractSchema
{
    private final SchemaKeyspaceConfiguration keyspaceConfig;
    private final boolean isEnabled;
    private final Set<TableSchema> tableSchemas = new HashSet<>();

    public SidecarInternalKeyspace(SidecarConfiguration config)
    {
        this.keyspaceConfig = config.serviceConfiguration().schemaKeyspaceConfiguration();
        this.isEnabled = keyspaceConfig.isEnabled();
    }

    public synchronized void registerTableSchema(TableSchema schema)
    {
        if (!isEnabled)
        {
            logger.warn("Sidecar schema is disabled!");
            return;
        }

        tableSchemas.add(schema);
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
    }

    @Override
    protected boolean exists(@NotNull Metadata metadata)
    {
        return metadata.getKeyspace(keyspaceConfig.keyspace()) != null;
    }

    @Override
    protected boolean initializeInternal(@NotNull Session session)
    {
        super.initializeInternal(session);

        for (AbstractSchema schema : tableSchemas)
        {
            if (!schema.initialize(session))
                return false;
        }

        return true;
    }

    @Override
    protected String createSchemaStatement()
    {
        return String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s",
                             keyspaceConfig.keyspace(), keyspaceConfig.createReplicationStrategyString());
    }
}
