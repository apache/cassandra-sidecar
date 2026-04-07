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

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.TableSchema;

/**
 * Database accessor for querying Cassandra virtual tables in the system_views keyspace.
 */
@Singleton
public class VirtualTablesDatabaseAccessor extends DatabaseAccessor<TableSchema>
{
    public static final String SYSTEM_VIEWS_KS = "system_views";
    public static final String SYSTEM_VIEWS_SETTINGS_TBL = "settings";
    public static final String CDC_ON_REPAIR_ENABLED_FLAG = "cdc_on_repair_enabled";

    /**
     * Creates a new virtual tables database accessor.
     */
    @Inject
    public VirtualTablesDatabaseAccessor(TableSchema tableSchema, CQLSessionProvider sessionProvider)
    {
        super(tableSchema, sessionProvider);
    }

    /**
     * Checks if CDC on repair is enabled in the system settings.
     */
    public boolean isCdcOnRepairEnabled()
    {
        Select query = QueryBuilder.selectFrom(SYSTEM_VIEWS_KS, SYSTEM_VIEWS_SETTINGS_TBL)
                                   .column("value")
                                   .where(Relation.column("name").isEqualTo(QueryBuilder.literal(CDC_ON_REPAIR_ENABLED_FLAG)));
        Row row = session().execute(query.build()).one();
        return row != null && !row.isNull(0) && "true".equalsIgnoreCase(row.getString(0));
    }
}
