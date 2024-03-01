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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Token;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.db.schema.RestoreSlicesSchema;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;

/**
 * {@link RestoreSliceDatabaseAccessor} is a data accessor to Cassandra.
 * It encapsulates the CRUD operations for restore_slice table
 */
@Singleton
public class RestoreSliceDatabaseAccessor extends DatabaseAccessor<RestoreSlicesSchema>
{
    @Inject
    protected RestoreSliceDatabaseAccessor(SidecarSchema sidecarSchema,
                                           RestoreSlicesSchema restoreSlicesSchema,
                                           CQLSessionProvider cqlSessionProvider)
    {
        super(sidecarSchema, restoreSlicesSchema, cqlSessionProvider);
    }

    public RestoreSlice create(RestoreSlice slice)
    {
        sidecarSchema.ensureInitialized();

        BoundStatement statement = tableSchema.insertSlice()
                                              .bind(slice.jobId(),
                                                    slice.bucketId(),
                                                    slice.sliceId(),
                                                    slice.bucket(),
                                                    slice.key(),
                                                    slice.checksum(),
                                                    slice.startToken(),
                                                    slice.endToken(),
                                                    slice.compressedSize(),
                                                    slice.uncompressedSize());
        execute(statement);
        return slice;
    }

    /**
     * Find all {@link RestoreSlice} that overlaps the range of the job and bucket.
     * @param restoreJob restore job
     * @param bucketId bucket id
     * @param range range to check the overlaps
     * @return list of overlapping {@link RestoreSlice}
     */
    public List<RestoreSlice> selectByJobByBucketByTokenRange(RestoreJob restoreJob, short bucketId, TokenRange range)
    {
        sidecarSchema.ensureInitialized();
        Token firstToken = range.firstToken();
        Preconditions.checkArgument(firstToken != null, "range cannot be empty");

        BoundStatement statement = tableSchema.findAllByTokenRange()
                                              .bind(restoreJob.jobId,
                                                    bucketId,
                                                    firstToken.toBigInteger(),
                                                    range.end().toBigInteger());
        ResultSet result = execute(statement);
        List<RestoreSlice> slices = new ArrayList<>();
        for (Row row : result)
        {
            slices.add(RestoreSlice.from(row, restoreJob));
        }
        return slices;
    }
}
