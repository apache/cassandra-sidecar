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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.jetbrains.annotations.NotNull;

/**
 * Tracks and stores the results of long-running jobs running on the sidecar.
 * Implementations can use different storage backends (in-memory, persistent, etc.)
 */
public interface OperationalJobTracker
{
    /**
     * Retrieve a job by its ID, or compute and store it if absent.
     * <p>
     * The mapping function is only called if the job does not exist.
     * This ensures that job creation and scheduling logic is only executed once.
     *
     * @param jobId the job identifier
     * @param mappingFunction function to create the job if absent
     * @return the job (either existing or newly created)
     */
    OperationalJob computeIfAbsent(UUID jobId, Function<UUID, OperationalJob> mappingFunction);

    /**
     * Retrieve a job by its ID.
     *
     * @param jobId the job identifier
     * @return the job info, or null if not found
     */
    OperationalJobInfo get(UUID jobId);

    /**
     * Returns an immutable view of all tracked jobs.
     * <p>
     * This provides a consistent snapshot of the jobs being tracked,
     * minimizing contention with concurrent operations.
     *
     * @return an immutable map of job IDs to jobs
     */
    @NotNull
    Map<UUID, OperationalJob> jobsView();

    /**
     * Filters inflight (CREATED or RUNNING) jobs matching the operation name.
     *
     * @param operation the operation name to filter by
     * @return list of inflight jobs for the operation
     */
    @NotNull
    List<OperationalJob> inflightJobsByOperation(String operation);
}
