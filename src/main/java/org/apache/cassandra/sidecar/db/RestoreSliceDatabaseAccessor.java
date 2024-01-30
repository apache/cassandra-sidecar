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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.RestoreSlicesSchema;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;

/**
 * {@link RestoreSliceDatabaseAccessor} is a data accessor to Cassandra.
 * It encapsulates the CRUD operations for restore_slice table
 */
@Singleton
public class RestoreSliceDatabaseAccessor extends DatabaseAccessor
{
    private final RestoreSlicesSchema restoreSlicesSchema;
    @Inject
    protected RestoreSliceDatabaseAccessor(SidecarSchema sidecarSchema,
                                           RestoreSlicesSchema restoreSlicesSchema,
                                           CQLSessionProvider cqlSessionProvider)
    {
        super(sidecarSchema, cqlSessionProvider);
        this.restoreSlicesSchema = restoreSlicesSchema;
    }

    public RestoreSlice create(RestoreSlice slice)
    {
        BoundStatement statement = restoreSlicesSchema.insertSlice()
                                                      .bind(slice.jobId(),
                                                            slice.bucketId(),
                                                            slice.sliceId(),
                                                            slice.bucket(),
                                                            slice.key(),
                                                            slice.checksum(),
                                                            slice.startToken(),
                                                            slice.endToken(),
                                                            slice.compressedSize(),
                                                            slice.uncompressedSize(),
                                                            slice.statusByReplica(),
                                                            slice.replicas());
        execute(statement);
        return slice;
    }

    public RestoreSlice updateStatus(RestoreSlice slice)
    {
        sidecarSchema.ensureInitialized();

        BoundStatement statement = restoreSlicesSchema.updateStatus()
                                                      .bind(slice.statusByReplica(),
                                                            slice.replicas(),
                                                            slice.jobId(),
                                                            slice.bucketId(),
                                                            slice.startToken(),
                                                            slice.sliceId());
        Row row = execute(statement).one();
        if (row == null)
        {
            throw new RuntimeException("Unexpected result while updating slice information for slice_id="
                                       + slice.sliceId());
        }
        return slice;
    }

    public List<RestoreSlice> selectByJobByBucketByTokenRange(UUID jobId, short bucketId,
                                                              BigInteger startToken, BigInteger endToken)
    {
        sidecarSchema.ensureInitialized();

        BoundStatement statement = restoreSlicesSchema.findAllByTokenRange()
                                                      .bind(jobId,
                                                            bucketId,
                                                            startToken,
                                                            endToken);
        ResultSet result = execute(statement);
        List<RestoreSlice> slices = new ArrayList<>();
        for (Row row : result)
        {
            slices.add(RestoreSlice.from(row));
        }
        return slices;
    }
}
