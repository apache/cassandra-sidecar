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

package org.apache.cassandra.sidecar.client.request;

import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.jetbrains.annotations.Nullable;

/**
 * An abstract class that access the {@link ApiEndpointsV1#SNAPSHOTS_ROUTE}
 *
 * @param <T> the type of the decodable request
 */
abstract class SnapshotRequest<T> extends DecodableRequest<T>
{
    SnapshotRequest(String keyspace, String table, String snapshotName)
    {
        super(requestURI(keyspace, table, snapshotName, false, null));
    }

    SnapshotRequest(String keyspace, String table, String snapshotName, boolean includeSecondaryIndexFiles,
                    @Nullable String snapshotTTL)
    {
        super(requestURI(keyspace, table, snapshotName, includeSecondaryIndexFiles, snapshotTTL));
    }

    static String requestURI(String keyspace, String tableName, String snapshotName,
                             boolean includeSecondaryIndexFiles, @Nullable String snapshotTTL)
    {
        String requestUri = ApiEndpointsV1.SNAPSHOTS_ROUTE
                            .replaceAll(ApiEndpointsV1.KEYSPACE_PATH_PARAM, keyspace)
                            .replaceAll(ApiEndpointsV1.TABLE_PATH_PARAM, tableName)
                            .replaceAll(ApiEndpointsV1.SNAPSHOT_PATH_PARAM, snapshotName);

        if (!includeSecondaryIndexFiles && snapshotTTL == null)
        {
            return requestUri;
        }

        requestUri = requestUri + '?';

        if (includeSecondaryIndexFiles)
        {
            requestUri = requestUri + "includeSecondaryIndexFiles=true";
        }

        if (snapshotTTL != null)
        {
            requestUri = requestUri + "?ttl=" + snapshotTTL;
        }

        return requestUri;
    }
}
