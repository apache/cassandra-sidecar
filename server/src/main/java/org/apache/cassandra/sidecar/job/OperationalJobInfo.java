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

package org.apache.cassandra.sidecar.job;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A read-only view of operational job data. Provides state accessors needed by handlers
 * and utilities to inspect job status and report results, without exposing execution logic.
 */
public interface OperationalJobInfo
{
    /**
     * @return the unique identifier for the job
     */
    UUID jobId();

    /**
     * @return the node UUID associated with the job, or {@code null} for jobs spanning multiple nodes
     */
    @Nullable
    UUID nodeId();

    /**
     * @return the name of the operation the job performs
     */
    String name();

    /**
     * @return the current status of the job
     */
    OperationalJobStatus status();

    /**
     * @return unix timestamp of the job creation time in milliseconds
     */
    long creationTime();

    /**
     * @return the time this job started execution, or {@code null} if not yet started
     */
    @Nullable
    Instant startTime();

    /**
     * @return list of node UUIDs pending execution of the job
     */
    @NotNull
    List<UUID> nodesPending();

    /**
     * @return list of node UUIDs currently executing the job
     */
    @NotNull
    List<UUID> nodesExecuting();

    /**
     * @return list of node UUIDs that have succeeded executing the job
     */
    @NotNull
    List<UUID> nodesSucceeded();

    /**
     * @return list of node UUIDs that have failed executing the job
     */
    @NotNull
    List<UUID> nodesFailed();

    /**
     * @return the time of the last status update, or {@code null} if not yet started
     */
    @Nullable
    Instant lastUpdate();

    /**
     * @return whether the job is currently executing
     */
    boolean isExecuting();

    /**
     * @return the failure reason if the job has failed, or {@code null} if the job is still in progress or succeeded
     */
    @Nullable
    String failureReason();
}
