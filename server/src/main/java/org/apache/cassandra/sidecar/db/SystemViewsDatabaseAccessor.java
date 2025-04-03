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

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.SystemViewsSchema;
import org.apache.cassandra.sidecar.exceptions.SchemaUnavailableException;
import org.apache.cassandra.sidecar.utils.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Database Accessor that queries cassandra to get information maintained under system_auth keyspace.
 */
@Singleton
public class SystemViewsDatabaseAccessor extends DatabaseAccessor<SystemViewsSchema>
{
    private static final String YAML_PROP_IN_MB = "cdc_total_space_in_mb";
    private static final String YAML_PROP_WITH_UNIT = "cdc_total_space"; // expects value with units e.g. "5MiB"

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
        Map<String, String> settings = getSettings(YAML_PROP_IN_MB, YAML_PROP_WITH_UNIT);
        String cdcTotalSpaceInMb = settings.get(YAML_PROP_IN_MB);
        if (cdcTotalSpaceInMb != null)
        {
            return FileUtils.mbStringToBytes(cdcTotalSpaceInMb);
        }

        // otherwise parse current (v5.0+) 'cdc_total_space' yaml prop
        String storageStringToBytes = settings.get(YAML_PROP_WITH_UNIT);
        if (storageStringToBytes != null)
        {
            return FileUtils.storageStringToBytes(storageStringToBytes);
        }

        return null;
    }

    /**
     * Load a setting values from the `system_views.settings` table.
     *
     * @param names names of settings
     * @return map of setting values keyed on `name` loaded from the `system_views.settings` table.
     */
    @NotNull
    public Map<String, String> getSettings(String... names) throws SchemaUnavailableException
    {
        BoundStatement statement = tableSchema.selectSettings().bind(Arrays.asList(names));
        ResultSet result = execute(statement);
        return result.all()
                     .stream()
                     .collect(Collectors.toMap(
                              row -> row.getString(0),
                              row -> row.getString(1))
                     );
    }
}
