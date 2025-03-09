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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.SystemViewsSchema;
import org.apache.cassandra.sidecar.exceptions.SchemaUnavailableException;
import org.apache.cassandra.sidecar.utils.FileUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Database Accessor that queries cassandra to get information maintained under system_auth keyspace.
 */
@Singleton
public class SystemViewsDatabaseAccessor extends DatabaseAccessor<SystemViewsSchema>
{
    static final String YAML_PROP_PREVIOUS = "cdc_total_space_in_mb";
    static final String YAML_PROP_CURRENT = "cdc_total_space";

    @Inject
    public SystemViewsDatabaseAccessor(SystemViewsSchema systemViewsSchema,
                                       CQLSessionProvider sessionProvider)
    {
        super(systemViewsSchema, sessionProvider);
    }

    @Nullable
    public Long getCdcTotalSpaceSetting() throws SchemaUnavailableException
    {
        // attempt to parse Cassandra v4.0 'cdc_total_space_in_mb' yaml prop
        String cdcTotalSpaceInMb = getSetting(YAML_PROP_PREVIOUS);
        if (cdcTotalSpaceInMb != null)
        {
            return FileUtils.mbStringToBytes(cdcTotalSpaceInMb);
        }

        // otherwise parse current (v5.0+) 'cdc_total_space' yaml prop
        String storageStringToBytes = getSetting(YAML_PROP_CURRENT);
        if (storageStringToBytes != null)
        {
            return FileUtils.storageStringToBytes(storageStringToBytes);
        }

        return null;
    }

    /**
     * Load a setting value from the `system_views.settings` table.
     *
     * @param name name of setting
     * @return setting value for a given `name` loaded from the `system_views.settings` table.
     */
    @Nullable
    public String getSetting(String name) throws SchemaUnavailableException
    {
        BoundStatement statement = tableSchema.selectSettings().bind(name);
        ResultSet result = execute(statement);
        Row row = result.one();
        return row != null && !row.isNull(0) ? row.getString(0) : null;
    }
}
