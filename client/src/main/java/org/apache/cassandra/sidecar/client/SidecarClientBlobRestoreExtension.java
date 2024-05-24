/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.client;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.sidecar.common.request.data.AbortRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.request.data.CreateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.request.data.CreateRestoreJobResponsePayload;
import org.apache.cassandra.sidecar.common.request.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.common.request.data.RestoreJobSummaryResponsePayload;
import org.apache.cassandra.sidecar.common.request.data.UpdateRestoreJobRequestPayload;

/**
 * An extension to sidecar client interface.
 * It includes the APIs for invoking blob based restore.
 */
public interface SidecarClientBlobRestoreExtension
{
    /**
     * Create a new restore job
     *
     * @param keyspace name of the keyspace in the cluster
     * @param table    name of the table in the cluster
     * @param payload  request payload
     * @return a completable future of {@link CreateRestoreJobResponsePayload}
     */
    CompletableFuture<CreateRestoreJobResponsePayload> createRestoreJob(String keyspace, String table,
                                                                        CreateRestoreJobRequestPayload payload);

    /**
     * Update an existing restore job
     *
     * @param keyspace name of the keyspace in the cluster
     * @param table    name of the table in the cluster
     * @param jobId    job ID of the restore job to be updated
     * @param payload  request payload
     * @return a completable future
     */
    CompletableFuture<Void> updateRestoreJob(String keyspace, String table, UUID jobId,
                                             UpdateRestoreJobRequestPayload payload);

    /**
     * Abort an existing restore job
     *
     * @param keyspace name of the keyspace in the cluster
     * @param table    name of the table in the cluster
     * @param jobId    job ID of the restore job to be updated
     * @param payload  request payload
     * @return a completable future
     */
    CompletableFuture<Void> abortRestoreJob(String keyspace, String table, UUID jobId,
                                            AbortRestoreJobRequestPayload payload);

    /**
     * Abort an existing restore job with no reason
     * See {@link #abortRestoreJob(String, String, UUID, AbortRestoreJobRequestPayload)}
     */
    default CompletableFuture<Void> abortRestoreJob(String keyspace, String table, UUID jobId)
    {
        return abortRestoreJob(keyspace, table, jobId, null);
    }

    /**
     * Get the summary of an existing restore job
     *
     * @param keyspace name of the keyspace in the cluster
     * @param table    name of the table in the cluster
     * @param jobId    job ID of the restore job to be updated
     * @return a completable future of {@link RestoreJobSummaryResponsePayload}
     */
    CompletableFuture<RestoreJobSummaryResponsePayload> restoreJobSummary(String keyspace, String table, UUID jobId);

    /**
     * Create a new slice in the restore job
     * or check the status of restore of an existing slice identified by the {@link CreateSliceRequestPayload}
     *
     * @param instance the instance where the request will be executed
     * @param keyspace name of the keyspace in the cluster
     * @param table    name of the table in the cluster
     * @param jobId    job ID of the restore job to create slice
     * @param payload  request payload
     * @return a completable future
     */
    CompletableFuture<Void> createRestoreJobSlice(SidecarInstance instance,
                                                  String keyspace,
                                                  String table,
                                                  UUID jobId,
                                                  CreateSliceRequestPayload payload);
}
