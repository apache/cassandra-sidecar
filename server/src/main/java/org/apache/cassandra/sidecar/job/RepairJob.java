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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.adapters.base.RepairOptions;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.request.data.RepairPayload;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.RepairJobsConfiguration;
import org.apache.cassandra.sidecar.handlers.data.RepairRequestParam;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of {@link OperationalJob} to perform repair operation.
 * <p>
 * Cassandra repair operations are asynchronous and can run for extended periods. When a repair is
 * detected to be IN_PROGRESS, this job sets its status to RUNNING and completes its promise.
 * <p>
 * The job's status can be checked at any time using the {@link #status()} method, which directly
 * queries the current repair status from Cassandra, regardless of whether the promise has been completed.
 * <p>
 * This design ensures timely responses to clients while allowing accurate status reporting for
 * long-running repair operations.
 */
public class RepairJob extends OperationalJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RepairJob.class);
    private static final String OPERATION = "repair";
    private static final String PREVIEW_KIND_REPAIRED = "REPAIRED";

    private final RepairRequestParam repairParams;
    private final RepairJobsConfiguration config;
    private final TaskExecutorPool internalPool;
    protected StorageOperations storageOperations;
    private volatile OperationalJobStatus currentStatus;
    private volatile int commandId = -1; // Store the command ID for status checks

    /**
     * Enum representing the status of a parent repair session
     */
    public enum ParentRepairStatus
    {
        IN_PROGRESS, COMPLETED, FAILED
    }

    /**
     * Constructor for creation of RepairJob
     *
     * @param taskExecutorPool    TaskExecutorPool instance (for testing)
     * @param config              Repair job configuration
     * @param jobId               UUID representing the Job to be created
     * @param storageOps          Reference to the storage operations interface
     * @param repairParams        Repair request parameters
     */
    public RepairJob(TaskExecutorPool taskExecutorPool,
                     RepairJobsConfiguration config,
                     UUID jobId,
                     StorageOperations storageOps,
                     RepairRequestParam repairParams)
    {
        super(jobId);
        this.internalPool = taskExecutorPool;
        this.config = config;
        this.storageOperations = storageOps;
        this.repairParams = repairParams;
        this.currentStatus = OperationalJobStatus.CREATED;
    }

    /**
     * {@inheritDoc}
     * RepairJob allows parallel executions since multiple repairs can run concurrently
     * on different tables or with different parameters.
     */
    @Override
    public boolean hasConflict(@NotNull List<OperationalJob> sameOperationJobs)
    {
        // For now, we simply allow all repair jobs to run in parallel
        // A more sophisticated implementation could check the repair parameters
        // against currently running repairs to detect potential conflicts
        return false;
    }

    @Override
    protected Future<Void> executeInternal()
    {
        try
        {
            Map<String, String> options = generateRepairOptions(repairParams.requestPayload());
            String keyspace = repairParams.keyspace().name();

            LOGGER.info("Executing repair operation for keyspace {} jobId={}", keyspace, this.jobId());

            try
            {
                commandId = storageOperations.repair(keyspace, options);
            }
            catch (Exception e)
            {
                LOGGER.error("Failed to initiate repair for keyspace {} jobId={}", keyspace, this.jobId(), e);
                currentStatus = OperationalJobStatus.FAILED;
                return Future.failedFuture(e);
            }

            if (commandId <= 0)
            {
                // When there are no relevant token ranges for the keyspace or RF is 1, the repair is inapplicable
                LOGGER.info("Repair is not applicable for the provided options and keyspace '{}' jobId '{}'", keyspace, this.jobId());
                currentStatus = OperationalJobStatus.SUCCEEDED;
                return Future.succeededFuture();
            }

            // Create promise for job submission response - this completes when we have a definitive
            // status to return to the client (either repair is running, completed, or failed)
            final Promise<Void> jobSubmissionPromise = Promise.promise();
            int maxAttempts = config.repairStatusMaxAttempts();
            final AtomicInteger attemptCounter = new AtomicInteger(0);

            // Periodic timer that checks for a valid repair status for a specified no. attempts to make a best-effort attempt
            // to validate that the repair has been kicked-off before returning.
            internalPool.setPeriodic(0, config.repairPollInterval().toIntMillis(), id -> {
                try
                {
                    int currentAttempt = attemptCounter.incrementAndGet();
                    if (currentAttempt > maxAttempts)
                    {
                        internalPool.cancelTimer(id);
                        String msg = String.format("Failed to obtain repair status after %d attempts.", maxAttempts);
                        LOGGER.warn(msg);
                        // Set status to RUNNING and complete the promise to ensure the job is properly handled
                        currentStatus = OperationalJobStatus.RUNNING;
                        jobSubmissionPromise.tryComplete();
                        return;
                    }

                    // Check the repair status
                    List<String> status;
                    try
                    {
                        status = storageOperations.getParentRepairStatus(commandId);
                    }
                    catch (Exception e)
                    {
                        LOGGER.warn("Failed to get repair status for cmd: {} (attempt {}/{})",
                                    commandId, currentAttempt, maxAttempts, e);
                        // Continue polling on exception
                        return;
                    }

                    // If status is empty, continue polling
                    if (status == null || status.isEmpty())
                    {
                        LOGGER.debug("No parent repair session status found for cmd: {} - repair may be initializing (attempt {}/{})",
                                     commandId, currentAttempt, maxAttempts);
                        return;
                    }

                    internalPool.cancelTimer(id);
                    updateRepairJobStatus(jobSubmissionPromise, status);
                }
                catch (Exception e)
                {
                    LOGGER.error("Unexpected error in repair status check", e);
                    internalPool.cancelTimer(id);
                    currentStatus = OperationalJobStatus.FAILED;
                    jobSubmissionPromise.tryFail(e);
                }
            });

            return jobSubmissionPromise.future();
        }
        catch (Exception e)
        {
            // Catch any exceptions in the overall method
            LOGGER.error("Failed to execute repair job", e);
            currentStatus = OperationalJobStatus.FAILED;
            return Future.failedFuture(e);
        }
    }


    @Override
    public OperationalJobStatus status()
    {
        // If we have a valid command ID, check the actual repair status
        if (commandId > 0)
        {
            List<String> status = storageOperations.getParentRepairStatus(commandId);
            if (status != null && !status.isEmpty())
            {
                try
                {
                    ParentRepairStatus parentRepairStatus = ParentRepairStatus.valueOf(status.get(0));
                    switch (parentRepairStatus)
                    {
                        case COMPLETED:
                            return OperationalJobStatus.SUCCEEDED;
                        case FAILED:
                            return OperationalJobStatus.FAILED;
                        case IN_PROGRESS:
                            return OperationalJobStatus.RUNNING;
                        default:
                            LOGGER.warn("Encountered unexpected repair status: {}", parentRepairStatus);
                            // Don't update currentStatus here, fall back to parent implementation
                    }
                }
                catch (IllegalArgumentException e)
                {
                    LOGGER.warn("Invalid parent repair status: {}", status.get(0), e);
                    // Don't update currentStatus here, fall back to parent implementation
                }
            }
        }
        
        // If we have a current status, return it
        if (currentStatus != null)
        {
            return currentStatus;
        }
        
        // Otherwise, fall back to the parent implementation
        return super.status();
    }

    /**
     * Updates the repair job status based on the parent repair status from Cassandra.
     * <p>
     * When the parent repair status is IN_PROGRESS, this method sets the currentStatus
     * to RUNNING and completes the promise
     * <p>
     * This approach ensures that resources are properly managed while still providing
     * accurate status reporting through the {@link #status()} method.
     *
     * @param jobSubmissionPromise the promise to complete
     * @param status the parent repair status from Cassandra
     */
    private void updateRepairJobStatus(Promise<Void> jobSubmissionPromise, List<String> status)
    {
        ParentRepairStatus parentRepairStatus = ParentRepairStatus.valueOf(status.get(0));
        List<String> messages = status.subList(1, status.size());

        LOGGER.info("Parent repair session {} has {} status. Messages: {}",
                    parentRepairStatus.name().toLowerCase(),
                    parentRepairStatus,
                    String.join("\n", messages));
        switch (parentRepairStatus)
        {
            case COMPLETED:
                currentStatus = OperationalJobStatus.SUCCEEDED;
                jobSubmissionPromise.tryComplete();
                break;
            case FAILED:
                currentStatus = OperationalJobStatus.FAILED;
                String reason = !messages.isEmpty() ? messages.get(0) :
                                "Repair failed with no error message";
                jobSubmissionPromise.tryFail(new IOException(reason));
                break;
            case IN_PROGRESS:
                currentStatus = OperationalJobStatus.RUNNING;
                jobSubmissionPromise.tryComplete();
                break;
            default:
                String message = String.format("Encountered unexpected repair status: %s Messages: %s",
                                               parentRepairStatus, String.join("\n", messages));
                LOGGER.error(message);
                currentStatus = OperationalJobStatus.FAILED;
                jobSubmissionPromise.tryFail(message);
                break;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name()
    {
        return OPERATION;
    }

    private Map<String, String> generateRepairOptions(RepairPayload repairPayload)
    {
        Map<String, String> options = new HashMap<>();

        List<String> tables = repairPayload.tables();
        if (tables != null && !tables.isEmpty())
        {
            options.put(RepairOptions.COLUMNFAMILIES.optionName(), String.join(",", tables));
        }

        Boolean isPrimaryRange = repairPayload.isPrimaryRange();
        if (isPrimaryRange != null)
        {
            options.put(RepairOptions.PRIMARY_RANGE.optionName(), String.valueOf(isPrimaryRange));
        }
        // TODO: Verify use-cases involving multiple DCs

        String dc = repairPayload.datacenter();
        if (dc != null)
        {
            options.put(RepairOptions.DATACENTERS.optionName(), dc);
        }

        List<String> hosts = repairPayload.hosts();
        if (hosts != null && !hosts.isEmpty())
        {
            options.put(RepairOptions.HOSTS.optionName(), String.join(",", hosts));
        }

        if (repairPayload.startToken() != null && repairPayload.endToken() != null)
        {
            // Use original string values for range construction
            options.put(RepairOptions.RANGES.optionName(), repairPayload.startToken() + ":" + repairPayload.endToken());
        }

        if (repairPayload.repairType() == RepairPayload.RepairType.INCREMENTAL)
        {
            options.put(RepairOptions.INCREMENTAL.optionName(), Boolean.TRUE.toString());
        }

        if (repairPayload.force() != null)
        {
            options.put(RepairOptions.FORCE_REPAIR.optionName(), String.valueOf(repairPayload.force()));
        }

        if (repairPayload.shouldValidate() != null)
        {
            options.put(RepairOptions.PREVIEW.optionName(), PREVIEW_KIND_REPAIRED);
        }
        return options;
    }
}
