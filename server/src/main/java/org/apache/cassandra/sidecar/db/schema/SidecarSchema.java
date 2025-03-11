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

import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.utils.EventBusUtils;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_SCHEMA_INITIALIZED;

/**
 * Encapsulates all related operations for features provided by Sidecar
 */
public class SidecarSchema
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarSchema.class);

    private final SchemaKeyspaceConfiguration schemaKeyspaceConfiguration;
    private final SidecarInternalKeyspace sidecarInternalKeyspace;
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);

    public SidecarSchema(Vertx vertx,
                         SidecarConfiguration config,
                         SidecarInternalKeyspace sidecarInternalKeyspace)
    {
        this.schemaKeyspaceConfiguration = config.serviceConfiguration().schemaKeyspaceConfiguration();
        this.sidecarInternalKeyspace = sidecarInternalKeyspace;
        if (this.schemaKeyspaceConfiguration.isEnabled())
        {
            EventBusUtils.onceLocalConsumer(vertx.eventBus(),
                                            ON_SIDECAR_SCHEMA_INITIALIZED.address(),
                                            ignored -> isInitialized.set(true));
        }
        else
        {
            LOGGER.info("Sidecar schema is disabled!");
        }
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

    public <T extends TableSchema> T tableSchema(Class<T> type)
    {
        return sidecarInternalKeyspace.tableSchema(type);
    }
}
