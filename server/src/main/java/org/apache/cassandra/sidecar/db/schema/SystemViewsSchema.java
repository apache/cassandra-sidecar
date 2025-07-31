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
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.exceptions.SchemaUnavailableException;
import org.jetbrains.annotations.NotNull;

/**
 * Schema for getting information stored in system_views keyspace.
 */
@Singleton
public class SystemViewsSchema extends CassandraSystemTableSchema
{
    protected static final String SYSTEM_VIEWS_KEYSPACE_NAME = "system_views";
    protected static final String SYSTEM_VIEWS_SETTINGS_TABLE_NAME = "settings";
    private PreparedStatement selectSettings;
    private PreparedStatement selectAllSettings;

    @Override
    protected String keyspaceName()
    {
        return SYSTEM_VIEWS_KEYSPACE_NAME;
    }

    @Override
    protected String tableName()
    {
        return SYSTEM_VIEWS_SETTINGS_TABLE_NAME;
    }

    public PreparedStatement selectSettings() throws SchemaUnavailableException
    {
        ensureSchemaAvailable();
        return selectSettings;
    }

    public PreparedStatement selectAllSettings() throws SchemaUnavailableException
    {
        ensureSchemaAvailable();
        return selectAllSettings;
    }

    @Override
    protected void prepareStatements(@NotNull Session session)
    {
        this.selectSettings = session.prepare("SELECT name, value FROM system_views.settings WHERE name IN ?");
        this.selectAllSettings = session.prepare("SELECT name, value FROM system_views.settings");
    }

    protected void ensureSchemaAvailable() throws SchemaUnavailableException
    {
        if (selectSettings == null || selectAllSettings == null)
        {
            throw new SchemaUnavailableException(keyspaceName(), tableName());
        }
    }
}
