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
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.adapters.base.RepairOptions;
import org.apache.cassandra.sidecar.common.request.data.RepairPayload;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.config.RepairConfiguration;
import org.apache.cassandra.sidecar.handlers.data.RepairRequestParam;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link OperationalJob} to perform repair operation.
 */
public class RepairJob extends OperationalJob
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RepairJob.class);
    private static final String OPERATION = "repair";
    private static final String PREVIEW_KIND_REPAIRED = "REPAIRED";

    private final RepairRequestParam repairParams;
    private final Vertx vertx;
    private final RepairConfiguration repairConfiguration;
    protected StorageOperations storageOperations;

    /**
     * Enum representing the status of a parent repair session
     */
    public enum ParentRepairStatus
    {
        IN_PROGRESS, COMPLETED, FAILED
    }

    /**
     * Constructs a job with a unique UUID, in Pending state
     *
     * @param vertx
     * @param repairConfiguration
     * @param jobId               UUID representing the Job to be created
     * @param storageOps
     * @param repairParams
     */
    public RepairJob(Vertx vertx, RepairConfiguration repairConfiguration, UUID jobId, StorageOperations storageOps, RepairRequestParam repairParams)
    {
        super(jobId);
        this.vertx = vertx;
        this.repairConfiguration = repairConfiguration;
        this.storageOperations = storageOps;
        this.repairParams = repairParams;
    }

    @Override
    public boolean isRunningOnCassandra()
    {
        // TODO: Leverage repair vtables to fail-fast on conflicting repairs (overlapping token-ranges or replica-sets)
        // Currently does not check for concurrent repairs
        return false;
    }

    @Override
    protected void executeInternal()
    {
        Map<String, String> options = generateRepairOptions(repairParams.requestpayload());
        String keyspace = repairParams.keyspace().name();

        LOGGER.info("Executing repair operation for keyspace {} jobId={} maxRuntime={}",
                    keyspace, this.jobId(), repairConfiguration.maxRepairJobRuntimeMillis());

        int cmd = storageOperations.repair(keyspace, options);
        if (cmd <= 0)
        {
            // repairAsync can only return 0 for replication factor 1.
            LOGGER.info("Replication factor is 1. No repair is needed for keyspace '{}'", keyspace);
        }
        else
        {
            // complete the max wait time promise either when exceeding the wait time, or the result is available
            Promise<Boolean> maxWaitTimePromise = Promise.promise();
            vertx.setTimer(repairConfiguration.maxRepairJobRuntimeMillis(), d -> {
                LOGGER.info("Timer Poll");
                maxWaitTimePromise.tryComplete(true);
            });

            // Promise for completion of repair operation
            Promise<Void> promise = Promise.promise();
            Future<Void> resultFut = promise.future();

            // main event loop checks periodically (10s) for completion
            vertx.setPeriodic(repairConfiguration.repairPollIntervalMillis(), id -> queryForCompletedRepair(promise, cmd));
            resultFut.onComplete(res -> maxWaitTimePromise.tryComplete(false));
            Future<Boolean> maxWaitTimeFut = maxWaitTimePromise.future();

            Future<Void> compositeFut = Future.any(maxWaitTimeFut, resultFut)
                                              // If this lambda below is evaluated, either one of the futures have completed;
                                              // In either case, the future corresponding to the job execution is returned
                                              .compose(f -> {
                                                  LOGGER.info("One of the futures ended waitStatus={} resultStatus={}",
                                                              maxWaitTimeFut.isComplete(), resultFut.isComplete());
                                                  boolean isTimeout = (maxWaitTimeFut.succeeded()) ? maxWaitTimeFut.result() : false;
                                                  if (isTimeout)
                                                  {
                                                      LOGGER.error("Timer ran out before the repair job completed. Repair took too long");
                                                      // TODO: Cancel repair? (Nice to have)
                                                      // We free up the thread (job fails) and stop polling for completion
                                                      return Future.failedFuture("Repair job taking too long");
                                                  }
                                                  // otherwise, the result of the job is available
                                                  return resultFut;
                                              });
            try
            {
                compositeFut.toCompletionStage().toCompletableFuture().get(); // wait
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
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
            options.put(RepairOptions.HOSTS.getValue(), String.join(",", requireNonNull(hosts)));
        }

        if (repairPayload.startToken() != null && repairPayload.endToken() != null)
        {
            options.put(RepairOptions.RANGES.getValue(), repairPayload.startToken() + ":" + repairPayload.endToken());
        }

        if (repairPayload.repairType() == RepairPayload.RepairType.INCREMENTAL)
        {
            options.put(RepairOptions.INCREMENTAL.getValue(), Boolean.TRUE.toString());
        }

        if (repairPayload.force() != null)
        {
            options.put(RepairOptions.FORCE_REPAIR.getValue(), String.valueOf(repairPayload.force()));
        }

        if (repairPayload.isValidate() != null)
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
        if (status == null)
        {
            LOGGER.error("{} couldn't find repair status for cmd: {}",
                         queriedString, cmd);
        }
        else
        {
            ParentRepairStatus parentRepairStatus = ParentRepairStatus.valueOf(status.get(0));
            LOGGER.info("Status from polling: {}", parentRepairStatus);
            List<String> messages = status.subList(1, status.size());
            switch (parentRepairStatus)
            {
                case COMPLETED:
                case FAILED:
                    LOGGER.info("{} discovered repair {}", queriedString, parentRepairStatus.name().toLowerCase());
                    if (parentRepairStatus == ParentRepairStatus.FAILED)
                    {
                        promise.fail(new IOException(messages.get(0)));
                    }
                    LOGGER.info("Messages:\n{}", String.join("\n", messages));
                    promise.tryComplete();
                    vertx.cancelTimer(cmd);
                    break;
                case IN_PROGRESS:
                    LOGGER.info("Repair in progress. Messages:\n{}", String.join("\n", messages));
                    break;
                default:
                    String message = String.format("Encountered unexpected repair status: %s Messages: %s", parentRepairStatus, String.join("\n", messages));
                    LOGGER.error(message);
                    promise.fail(message);
                    break;
            }
        }
    }
}
