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
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.request.data.RepairPayload;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.RepairJobsConfiguration;
import org.apache.cassandra.sidecar.handlers.data.RepairRequestParam;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
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
    private final PeriodicTaskExecutor periodicTaskExecutor;
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
     * Container for status parsing results
     */
    private static class StatusResult
    {
        final OperationalJobStatus operationalStatus;
        final List<String> messages;

        StatusResult(OperationalJobStatus operationalStatus, List<String> messages)
        {
            this.operationalStatus = operationalStatus;
            this.messages = messages;
        }
    }

    /**
     * Constructor for creation of RepairJob
     *
     * @param periodicTaskExecutor PeriodicTaskExecutor for status checking
     * @param config               Repair job configuration
     * @param jobId                UUID representing the Job to be created
     * @param storageOps           Reference to the storage operations interface
     * @param repairParams         Repair request parameters
     */
    public RepairJob(PeriodicTaskExecutor periodicTaskExecutor,
                     RepairJobsConfiguration config,
                     UUID jobId,
                     StorageOperations storageOps,
                     RepairRequestParam repairParams)
    {
        super(jobId);
        this.periodicTaskExecutor = periodicTaskExecutor;
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
        // Let Cassandra handle conflict detection - no pre-emptive conflict checking
        // Conflicts will be handled through exception handling during repair execution
        return false;
    }

    @Override
    protected Future<Void> executeInternal()
    {
        try
        {
            Map<String, String> options = generateRepairOptions(repairParams.requestPayload());
            String keyspace = repairParams.keyspace().maybeQuotedName();

            LOGGER.info("Executing repair operation for keyspace='{}' jobId={}", keyspace, this.jobId());

            try
            {
                commandId = storageOperations.repairAsync(keyspace, options);
            }
            catch (Exception e)
            {
                LOGGER.error("Failed to initiate repair for keyspace='{}' jobId={}", keyspace, this.jobId(), e);
                currentStatus = OperationalJobStatus.FAILED;
                return Future.failedFuture(e);
            }

            if (commandId <= 0)
            {
                // When there are no relevant token ranges for the keyspace or RF is 1, the repair is inapplicable
                LOGGER.info("Replication factor is 1. No repair is needed for keyspace='{}' jobId={}", keyspace, this.jobId());
                currentStatus = OperationalJobStatus.SUCCEEDED;
                return Future.succeededFuture();
            }

            Promise<Void> jobSubmissionPromise = Promise.promise();
            int maxAttempts = config.repairStatusMaxAttempts();
            AtomicInteger attemptCounter = new AtomicInteger(0);

            // Use PeriodicTaskExecutor for proper scheduling with delay between executions
            RepairStatusCheckTask statusCheckTask = new RepairStatusCheckTask(keyspace, jobSubmissionPromise, attemptCounter, maxAttempts);
            periodicTaskExecutor.schedule(statusCheckTask);

            Future<Void> jobSubmissionFuture = jobSubmissionPromise.future();
            jobSubmissionFuture.onComplete(ignore -> periodicTaskExecutor.unschedule(statusCheckTask));
            return jobSubmissionFuture;
        }
        catch (Exception e)
        {
            // Catch any exceptions in the overall method
            LOGGER.error("Failed to execute repair job jobId={}", this.jobId(), e);
            currentStatus = OperationalJobStatus.FAILED;
            return Future.failedFuture(e);
        }
    }

    @Override
    public OperationalJobStatus status()
    {
        refreshStatusFromCassandra();
        return currentStatus != null ? currentStatus : super.status();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OperationType operationType()
    {
        return OperationType.REPAIR;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name()
    {
        return OPERATION;
    }

    /**
     * Refreshes the repair status from Cassandra and updates the internal currentStatus.
     *
     * @return StatusResult containing the operational status and messages, or null if no status is available yet
     */
    private StatusResult refreshStatusFromCassandra()
    {
        // If we don't have a valid command ID, return current status
        if (commandId <= 0)
        {
            return null;
        }

        List<String> status;
        try
        {
            status = storageOperations.getParentRepairStatus(commandId);
        }
        catch (Exception e)
        {
            LOGGER.warn("Failed to get repair status for cmd: {}", commandId, e);
            return null;
        }

        // If no status available yet, return null
        if (status == null || status.isEmpty())
        {
            return null;
        }

        try
        {
            ParentRepairStatus parentRepairStatus = ParentRepairStatus.valueOf(status.get(0));

            switch (parentRepairStatus)
            {
                case COMPLETED:
                    currentStatus = OperationalJobStatus.SUCCEEDED;
                    break;
                case FAILED:
                    currentStatus = OperationalJobStatus.FAILED;
                    break;
                case IN_PROGRESS:
                    currentStatus = OperationalJobStatus.RUNNING;
                    break;
                default:
                    LOGGER.warn("Encountered unexpected repair status: {}", parentRepairStatus);
                    return null;
            }


            // Extract messages (everything after the first element)
            List<String> messages = status.subList(1, status.size());

            return new StatusResult(currentStatus, messages);
        }
        catch (IllegalArgumentException e)
        {
            LOGGER.warn("Invalid parent repair status={}", status.get(0), e);
            return null;
        }
    }

    /**
     * Handles the job submission promise based on the repair status result.
     *
     * @param jobSubmissionPromise the promise to complete or fail
     * @param statusResult         the status result containing operational status and messages
     */
    private void handleJobSubmissionPromise(Promise<Void> jobSubmissionPromise, StatusResult statusResult)
    {
        LOGGER.info("Parent repair session has {} status. Messages: {}",
                    statusResult.operationalStatus, String.join("\n", statusResult.messages));

        switch (statusResult.operationalStatus)
        {
            case SUCCEEDED:
            case RUNNING:
                jobSubmissionPromise.tryComplete();
                break;
            case FAILED:
                String reason = !statusResult.messages.isEmpty() ? statusResult.messages.get(0) :
                                "Repair failed with no error message";
                jobSubmissionPromise.tryFail(new IOException(reason));
                break;
            default:
                String message = String.format("Encountered unexpected repair status: %s Messages: %s",
                                               statusResult.operationalStatus, String.join("\n", statusResult.messages));
                LOGGER.error(message);
                jobSubmissionPromise.tryFail(new IOException(message));
                break;
        }
    }

    private Map<String, String> generateRepairOptions(RepairPayload repairPayload)
    {
        Map<String, String> options = new HashMap<>();

        List<String> tables = repairPayload.tables();
        if (tables != null && !tables.isEmpty())
        {
            options.put(RepairOptions.COLUMN_FAMILIES.optionName(), String.join(",", tables));
        }

        Boolean isPrimaryRange = repairPayload.isPrimaryRange();
        if (isPrimaryRange != null)
        {
            options.put(RepairOptions.PRIMARY_RANGE.optionName(), String.valueOf(isPrimaryRange));
        }

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

        if (Boolean.TRUE.equals(repairPayload.force()))
        {
            options.put(RepairOptions.FORCE_REPAIR.optionName(), Boolean.TRUE.toString());
        }

        if (Boolean.TRUE.equals(repairPayload.shouldValidate()))
        {
            options.put(RepairOptions.PREVIEW.optionName(), PREVIEW_KIND_REPAIRED);
        }
        return options;
    }

    /**
     * PeriodicTask implementation for checking repair status with proper scheduling.
     * This task ensures delay between executions and prevents overlapping status checks.
     */
    private class RepairStatusCheckTask implements PeriodicTask
    {
        private final String keyspace;
        private final Promise<Void> jobSubmissionPromise;
        private final AtomicInteger attemptCounter;
        private final int maxAttempts;
        private volatile boolean completed = false;

        RepairStatusCheckTask(String keyspace, Promise<Void> jobSubmissionPromise, AtomicInteger attemptCounter, int maxAttempts)
        {
            this.keyspace = keyspace;
            this.jobSubmissionPromise = jobSubmissionPromise;
            this.attemptCounter = attemptCounter;
            this.maxAttempts = maxAttempts;
        }

        @Override
        public void execute(Promise<Void> promise)
        {
            try
            {
                int currentAttempt = attemptCounter.incrementAndGet();
                if (currentAttempt > maxAttempts)
                {
                    LOGGER.warn("Failed to obtain repair status after {} attempts for keyspace='{}' jobId={}",
                                maxAttempts, keyspace, jobId());
                    // Set status to RUNNING and complete the promise to ensure the job is properly handled
                    currentStatus = OperationalJobStatus.RUNNING;
                    jobSubmissionPromise.tryComplete();
                    completed = true;
                    promise.complete();
                    return;
                }

                StatusResult statusResult = refreshStatusFromCassandra();

                // If no status available yet, continue polling
                if (statusResult == null)
                {
                    LOGGER.debug("No parent repair session status found for cmd: {} - repair may be initializing (attempt {}/{})" +
                                 " for keyspace='{}' jobId={}", commandId, currentAttempt, maxAttempts, keyspace, jobId());
                    promise.complete(); // Continue to next execution
                    return;
                }

                // We have a definitive status, handle the promise and stop the task
                handleJobSubmissionPromise(jobSubmissionPromise, statusResult);
                completed = true;
                promise.complete();
            }
            catch (Exception e)
            {
                LOGGER.error("Unexpected error in repair status check for keyspace='{}' jobId={}", keyspace, jobId(), e);
                currentStatus = OperationalJobStatus.FAILED;
                jobSubmissionPromise.tryFail(e);
                completed = true;
                promise.fail(e);
            }
        }

        @Override
        public DurationSpec delay()
        {
            return config.repairPollInterval();
        }

        @Override
        public DurationSpec initialDelay()
        {
            // Start immediately for the first check
            return MillisecondBoundConfiguration.ZERO;
        }

        @Override
        public ScheduleDecision scheduleDecision()
        {
            // Stop scheduling if we've completed
            return completed ? ScheduleDecision.SKIP : ScheduleDecision.EXECUTE;
        }

        @Override
        public String identifier()
        {
            return "repair-status-check-" + jobId();
        }

        @Override
        public String name()
        {
            return "RepairStatusCheck for job=" + jobId();
        }
    }
}
