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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.data.DatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.RestoreRangesSchema;
import org.apache.cassandra.sidecar.db.schema.SidecarSchemaInitializer;

/**
 * {@link RestoreSliceDatabaseAccessor} is a data accessor to Cassandra.
 * It encapsulates the CRUD operations for restore_range table
 */
@Singleton
public class RestoreRangeDatabaseAccessor extends DatabaseAccessor<RestoreRangesSchema>
{
    private final SidecarSchemaInitializer sidecarSchemaInitializer;

    @Inject
    protected RestoreRangeDatabaseAccessor(SidecarSchemaInitializer sidecarSchemaInitializer,
                                           RestoreRangesSchema tableSchema,
                                           CQLSessionProvider sessionProvider)
    {
        super(tableSchema, sessionProvider);
        this.sidecarSchemaInitializer = sidecarSchemaInitializer;
    }

    public RestoreRange create(RestoreRange range)
    {
        sidecarSchemaInitializer.ensureInitialized();

        BoundStatement statement = tableSchema.insert()
                                              .bind(range.jobId(),
                                                    range.bucketId(),
                                                    range.startToken(),
                                                    range.endToken(),
                                                    range.sliceId(),
                                                    range.sliceBucket(),
                                                    range.sliceKey(),
                                                    range.statusTextByReplica());
        execute(statement);
        return range;
    }

    public RestoreRange updateStatus(RestoreRange range)
    {
        sidecarSchemaInitializer.ensureInitialized();

        BoundStatement statement = tableSchema.updateStatus()
                                              .bind(range.statusTextByReplica(),
                                                    range.jobId(),
                                                    range.bucketId(),
                                                    range.startToken(),
                                                    range.endToken());
        execute(statement);
        return range;
    }

    // todo: change to stream api and paginate
    public List<RestoreRange> findAll(UUID jobId, short bucketId)
    {
        sidecarSchemaInitializer.ensureInitialized();

        BoundStatement statement = tableSchema.findAll()
                                              .bind(jobId,
                                                    bucketId);
        ResultSet result = execute(statement);
        List<RestoreRange> ranges = new ArrayList<>();
        for (Row row : result)
        {
            ranges.add(RestoreRange.from(row));
        }
        return ranges;
    }
}
