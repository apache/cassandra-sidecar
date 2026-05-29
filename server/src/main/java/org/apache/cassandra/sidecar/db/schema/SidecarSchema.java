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

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

/**
 * Encapsulates all related operations for features provided by Sidecar
 */
public class SidecarSchema implements TableSchemaFetcher
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarSchema.class);

    private final SchemaKeyspaceConfiguration schemaKeyspaceConfiguration;
    private final SidecarInternalKeyspace sidecarInternalKeyspace;
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);

    public SidecarSchema(SidecarConfiguration config,
                         SidecarInternalKeyspace sidecarInternalKeyspace)
    {
        this.schemaKeyspaceConfiguration = config.serviceConfiguration().schemaKeyspaceConfiguration();
        this.sidecarInternalKeyspace = sidecarInternalKeyspace;
        if (!this.schemaKeyspaceConfiguration.isEnabled())
        {
            LOGGER.info("Sidecar schema is disabled!");
        }
    }

    void markInitialized()
    {
        isInitialized.set(true);
    }

    public boolean isInitialized()
    {
        return schemaKeyspaceConfiguration.isEnabled() && isInitialized.get();
    }

    public void ensureInitialized()
    {
        if (!isInitialized())
        {
            throw new IllegalStateException("Sidecar schema is not initialized!");
        }
    }

    public SidecarInternalKeyspace sidecarInternalKeyspace()
    {
        return sidecarInternalKeyspace;
    }

    @Override
    public <T extends TableSchema> T tableSchema(Class<T> type)
    {
        return sidecarInternalKeyspace.tableSchema(type);
    }
}
