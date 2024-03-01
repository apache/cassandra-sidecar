/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.common.request.data;

import java.util.UUID;

import org.apache.cassandra.sidecar.common.data.RestoreJobProgressFetchPolicy;
import org.jetbrains.annotations.NotNull;

/**
 * Parameters to fetch the restore job progress
 */
public class RestoreJobProgressRequestParams
{
    public final String keyspace;
    public final String table;
    public final UUID jobId;
    public final RestoreJobProgressFetchPolicy fetchPolicy;

    public RestoreJobProgressRequestParams(@NotNull String keyspace,
                                           @NotNull String table,
                                           @NotNull UUID jobId)
    {
        this(keyspace, table, jobId, RestoreJobProgressFetchPolicy.FIRST_FAILED);
    }

    public RestoreJobProgressRequestParams(@NotNull String keyspace,
                                           @NotNull String table,
                                           @NotNull UUID jobId,
                                           @NotNull RestoreJobProgressFetchPolicy fetchPolicy)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.jobId = jobId;
        this.fetchPolicy = fetchPolicy;
    }
}
