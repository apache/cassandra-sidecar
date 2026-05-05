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

package org.apache.cassandra.sidecar.cdc;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.cdc.sidecar.SidecarCdcCassandraClient;
import org.apache.cassandra.sidecar.db.CdcDatabaseAccessor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Class managing the CDC state through the database accessor
 */
public class StateSidecarCdcCassandraClient implements SidecarCdcCassandraClient
{
    final CdcDatabaseAccessor cdcDatabaseAccessor;

    public StateSidecarCdcCassandraClient(CdcDatabaseAccessor cdcDatabaseAccessor)
    {
        this.cdcDatabaseAccessor = cdcDatabaseAccessor;
    }

    public List<CompletableFuture<AsyncResultSet>> storeStateAsync(@NotNull String jobId, @NotNull TokenRange range, @NotNull ByteBuffer buf, long timestamp)
    {
        return cdcDatabaseAccessor.storeStateAsync(jobId, range, buf, timestamp);
    }

    public Stream<byte[]> loadStateForRange(String jobId, @Nullable TokenRange tokenRange)
    {
        if (tokenRange == null)
        {
            return Stream.empty();
        }
        return cdcDatabaseAccessor.loadStateForRange(jobId, tokenRange);
    }
}
