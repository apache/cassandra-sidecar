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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.adapters.base.RepairOptions;
import org.apache.cassandra.sidecar.common.request.data.RepairPayload;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.RepairJobsConfiguration;
import org.apache.cassandra.sidecar.handlers.data.RepairRequestParam;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of {@link OperationalJob} to perform repair operation.
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

    /**
     * Enum representing the status of a parent repair session
     */
    public enum ParentRepairStatus
    {
        IN_PROGRESS, COMPLETED, FAILED, NEW_STATUS
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
        Map<String, String> options = generateRepairOptions(repairParams.requestPayload());
        String keyspace = repairParams.keyspace().name();

        LOGGER.info("Executing repair operation for keyspace {} jobId={} maxRuntime={}",
                    keyspace, this.jobId(), config.maxRepairJobRuntime());

        int cmd = storageOperations.repair(keyspace, options);
        if (cmd <= 0)
        {
            // When there are no relevant token ranges for the keyspace or RF is 1, the repair is inapplicable
            LOGGER.info("Repair is not applicable for the provided options and keyspace '{}' jobId '{}'", keyspace, this.jobId());
            return Future.succeededFuture();
        }
        Promise<Void> repairPromise = Promise.promise();
        queryForCompletedRepair(repairPromise, cmd);

        if (!repairPromise.future().isComplete())
        {
            Promise<Boolean> maxWaitTimePromise = Promise.promise();
            long timerId = internalPool.setTimer(config.maxRepairJobRuntime().toMillis(),
                                                 d -> maxWaitTimePromise.tryComplete(true));

            long periodicId = internalPool.setPeriodic(config.repairPollInterval().toMillis(),
                                                       id -> queryForCompletedRepair(repairPromise, cmd));

            repairPromise.future().onComplete(ar -> {
                internalPool.cancelTimer(timerId);
                internalPool.cancelTimer(periodicId);
                maxWaitTimePromise.tryComplete(false);
            });

            return Future.any(maxWaitTimePromise.future(), repairPromise.future())
                         .compose(f -> {
                             boolean isTimeout = maxWaitTimePromise.future().result();
                             if (isTimeout)
                             {
                                 LOGGER.error("Timer ran out before the repair job completed. Repair took too long");
                                 return Future.failedFuture("Repair job taking too long");
                             }
                             return repairPromise.future();
                         });
        }
        return repairPromise.future();
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
            options.put(RepairOptions.COLUMNFAMILIES.getValue(), String.join(",", tables));
        }

        Boolean isPrimaryRange = repairPayload.isPrimaryRange();
        if (isPrimaryRange != null)
        {
            options.put(RepairOptions.PRIMARY_RANGE.getValue(), String.valueOf(isPrimaryRange));
        }
        // TODO: Verify use-cases involving multiple DCs

        String dc = repairPayload.datacenter();
        if (dc != null)
        {
            options.put(RepairOptions.DATACENTERS.getValue(), dc);
        }

        List<String> hosts = repairPayload.hosts();
        if (hosts != null && !hosts.isEmpty())
        {
            options.put(RepairOptions.HOSTS.getValue(), String.join(",", hosts));
        }

        if (repairPayload.startToken() != null && repairPayload.endToken() != null)
        {
            try
            {
                long startToken = Long.parseLong(repairPayload.startToken());
                long endToken = Long.parseLong(repairPayload.endToken());
                if (startToken >= endToken)
                {
                    throw new IllegalArgumentException("Start token must be less than end token. " +
                                                      "Got start: " + startToken + ", end: " + endToken);
                }
                options.put(RepairOptions.RANGES.getValue(), repairPayload.startToken() + ":" + repairPayload.endToken());
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException("Invalid token format. Tokens must be numeric values.", e);
            }
        }

        if (repairPayload.repairType() == RepairPayload.RepairType.INCREMENTAL)
        {
            options.put(RepairOptions.INCREMENTAL.getValue(), Boolean.TRUE.toString());
        }

        if (repairPayload.force() != null)
        {
            options.put(RepairOptions.FORCE_REPAIR.getValue(), String.valueOf(repairPayload.force()));
        }

        if (repairPayload.shouldValidate() != null)
        {
            options.put(RepairOptions.PREVIEW.getValue(), PREVIEW_KIND_REPAIRED);
        }
        return options;
    }

    private void queryForCompletedRepair(Promise<Void> promise, int cmd)
    {
        LOGGER.info("Polling repair operation for jobId={} status={}", this.jobId(), promise.future().isComplete());
        List<String> status = storageOperations.getParentRepairStatus(cmd);
        String queriedString = "queried for parent session status and";
        if (status == null || status.isEmpty())
        {
            LOGGER.error("{} couldn't find repair status for cmd: {}", queriedString, cmd);
            promise.tryFail("Couldn't find repair status for cmd: " + cmd);
        }
        else
        {
            ParentRepairStatus parentRepairStatus = ParentRepairStatus.valueOf(status.get(0));
            List<String> messages = status.subList(1, status.size());
            switch (parentRepairStatus)
            {
                case COMPLETED:
                case FAILED:
                    LOGGER.info("{} discovered repair {}", queriedString, parentRepairStatus.name().toLowerCase());
                    if (parentRepairStatus == ParentRepairStatus.FAILED && !messages.isEmpty())
                    {
                        promise.tryFail(new IOException(messages.get(0)));
                    }
                    LOGGER.info("Repair {} Messages: {}", parentRepairStatus.name().toLowerCase(), String.join("\n", messages));
                    promise.tryComplete();
                    break;
                case IN_PROGRESS:
                    LOGGER.info("Repair in progress. Messages:{}", String.join("\n", messages));
                    break;
                default:
                    String message = String.format("Encountered unexpected repair status: %s Messages: %s", parentRepairStatus, String.join("\n", messages));
                    LOGGER.error(message);
                    promise.tryFail(message);
                    break;
            }
        }
    }
}
