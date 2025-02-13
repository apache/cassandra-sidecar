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

import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.ConfigsSchema;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.routes.cdc.ValidServices;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Configurations for CDC feature are stored inside a table "config" in an internal sidecar keyspace.
 * {@link ConfigAccessorImpl} is an accessor for the above-mentioned table and encapsulates database
 * access operations of the "config" table.
 */
public abstract class ConfigAccessorImpl extends DatabaseAccessor<ConfigsSchema> implements ConfigAccessor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigAccessorImpl.class);
    private final ValidServices service = service();
    private final SidecarSchema sidecarSchema;

    protected ConfigAccessorImpl(InstanceMetadataFetcher instanceMetadataFetcher,
                                 ConfigsSchema configsSchema,
                                 CQLSessionProvider sessionProvider,
                                 SidecarSchema sidecarSchema)
    {
        super(configsSchema, sessionProvider);
        this.sidecarSchema = sidecarSchema;
    }

    public abstract ValidServices service();

    @Override
    public ServiceConfig getConfig()
    {
        sidecarSchema.ensureInitialized();
        BoundStatement statement = tableSchema
                .selectConfig()
                .bind(service.serviceName);
        Row row = execute(statement).one();
        if (row == null || row.isNull(0))
        {
            LOGGER.debug(String.format("No %s configs are present in the table C* table", service.serviceName));
            return new ServiceConfig(Map.of());
        }
        return ServiceConfig.from(row);
    }

    @Override
    public ServiceConfig storeConfig(Map<String, String> config)
    {
        sidecarSchema.ensureInitialized();
        BoundStatement statement = tableSchema
                .insertConfig()
                .bind(service.serviceName, config);
        execute(statement);
        return new ServiceConfig(config);
    }

    @Override
    public Optional<ServiceConfig> storeConfigIfNotExists(Map<String, String> config)
    {
        sidecarSchema.ensureInitialized();
        BoundStatement statement = tableSchema
                .insertConfigIfNotExists()
                .bind(service.serviceName, config);
        ResultSet resultSet = execute(statement);
        if (resultSet.wasApplied())
        {
            return Optional.of(new ServiceConfig(config));
        }
        return Optional.empty();
    }

    @Override
    public void deleteConfig()
    {
        sidecarSchema.ensureInitialized();
        BoundStatement deleteStatement = tableSchema
                .deleteConfig()
                .bind(service.serviceName);
        execute(deleteStatement);
    }

    @Override
    public boolean isSchemaInitialized()
    {
        return sidecarSchema.isInitialized();
    }
}
