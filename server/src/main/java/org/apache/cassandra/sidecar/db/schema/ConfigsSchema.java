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

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ConfigsSchema} holds all prepared statements needed for talking to Cassandra for various actions
 * related to storing and updating CDC and Kafka configs.
 */
@Singleton
public class ConfigsSchema extends TableSchema
{
    private static final String CONFIGS_TABLE_NAME = "configs";
    private final SchemaKeyspaceConfiguration keyspaceConfig;

    // prepared statements
    private PreparedStatement selectConfig;
    private PreparedStatement insertConfig;
    private PreparedStatement insertConfigIfNotExist;
    private PreparedStatement deleteConfig;

    @Inject
    public ConfigsSchema(ServiceConfiguration configuration)
    {
        this.keyspaceConfig = configuration.schemaKeyspaceConfiguration();
    }

    @Override
    protected String keyspaceName()
    {
        return keyspaceConfig.keyspace();
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
        selectConfig = prepare(selectConfig, session, CqlLiterals.selectConfig(keyspaceConfig));
        insertConfig = prepare(insertConfig, session, CqlLiterals.insertConfig(keyspaceConfig));
        insertConfigIfNotExist = prepare(insertConfigIfNotExist, session, CqlLiterals.insertConfigIfNotExist(keyspaceConfig));
        deleteConfig = prepare(deleteConfig, session, CqlLiterals.deleteConfig(keyspaceConfig));
    }

    @Override
    protected String tableName()
    {
        return CONFIGS_TABLE_NAME;
    }

    @Override
    protected boolean exists(@NotNull Metadata metadata)
    {
        KeyspaceMetadata ksMetadata = metadata.getKeyspace(keyspaceConfig.keyspace());
        if (ksMetadata == null)
            return false;
        return ksMetadata.getTable(CONFIGS_TABLE_NAME) != null;
    }

    @Override
    protected String createSchemaStatement()
    {
        return String.format("CREATE TABLE IF NOT EXISTS %s.%s (" +
                             " service text," +
                             " config map<text, text>," +
                             " PRIMARY KEY (service))",
                             keyspaceConfig.keyspace(), CONFIGS_TABLE_NAME);
    }

    public PreparedStatement selectConfig()
    {
        return selectConfig;
    }

    public PreparedStatement insertConfig()
    {
        return insertConfig;
    }

    public PreparedStatement insertConfigIfNotExists()
    {
        return insertConfig;
    }

    public PreparedStatement deleteConfig()
    {
        return deleteConfig;
    }

    private static class CqlLiterals
    {
        static String selectConfig(SchemaKeyspaceConfiguration config)
        {
            return withTable("SELECT config from %s.%s" +
                             " WHERE service=?", config);
        }

        static String insertConfig(SchemaKeyspaceConfiguration config)
        {
            return withTable("INSERT INTO %s.%s (service, config)" +
                             " VALUES (?, ?)", config);
        }

        static String insertConfigIfNotExist(SchemaKeyspaceConfiguration config)
        {
            return insertConfig(config) + " IF NOT EXISTS";
        }

        static String deleteConfig(SchemaKeyspaceConfiguration config)
        {
            return withTable("DELETE FROM %s.%s" +
                             " WHERE service=?", config);
        }

        private static String withTable(String format, SchemaKeyspaceConfiguration config)
        {
            return String.format(format, config.keyspace(), CONFIGS_TABLE_NAME);
        }
    }
}
