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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.OperationalJobConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.apache.cassandra.sidecar.utils.EventBusUtils;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.TimeProvider;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_READY;

/**
 * Polls for active cluster-wide operations and coordinates local job execution.
 * Implements {@link PeriodicTask} for scheduled polling and {@link LocalJobCoordinator}
 * for predecessor checking, status reporting, and job completion detection.
 */
@Singleton
public class StorageBackedLocalJobCoordinator implements LocalJobCoordinator, PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageBackedLocalJobCoordinator.class);
    private static final int MAX_NODE_STATUS_RETRIES = 10;
    private static final long NODE_STATUS_RETRY_DELAY_MS = 5000;

    private final OperationalJobCoordinator operationalJobCoordinator;
    private final StorageProvider storageProvider;
    private final InstanceMetadataFetcher instanceMetadataFetcher;
    private final LocalJobManager localJobManager;
    private final OperationalJobConfiguration config;
    private final LocalJobFactory localJobFactory;
    private final long defaultNodeExecutionTimeoutMs;

    // When this Sidecar first saw a group's predecessors all reach a terminal state, keyed by operation and group.
    private final Map<GroupKey, Long> predecessorsCompletedAtNanos = new ConcurrentHashMap<>();
    private final DurationSpec randomizedInitialDelay;
    private final ExecutorPools executorPools;
    private final TimeProvider timeProvider;

    @Inject
    public StorageBackedLocalJobCoordinator(ExecutorPools executorPools,
                                            OperationalJobCoordinator operationalJobCoordinator,
                                            StorageProvider storageProvider,
                                            InstanceMetadataFetcher instanceMetadataFetcher,
                                            LocalJobManager localJobManager,
                                            SidecarConfiguration configuration,
                                            LocalJobFactory localJobFactory,
                                            TimeProvider timeProvider)
    {
        this.executorPools = executorPools;
        this.timeProvider = timeProvider;
        this.operationalJobCoordinator = operationalJobCoordinator;
        this.storageProvider = storageProvider;
        this.instanceMetadataFetcher = instanceMetadataFetcher;
        this.localJobManager = localJobManager;
        this.config = configuration.operationalJobConfiguration();
        this.localJobFactory = localJobFactory;
        this.defaultNodeExecutionTimeoutMs = config.nodeExecutionTimeout().toMillis();

        long delaySeconds = config.localJobCoordinationDelay().toSeconds();
        long randomSeconds = ThreadLocalRandom.current().nextLong(delaySeconds + 1);
        this.randomizedInitialDelay = SecondBoundConfiguration.parse(Math.max(1, randomSeconds) + "s");
    }

    @Override
    public void deploy(Vertx vertx, PeriodicTaskExecutor executor)
    {
        EventBusUtils.onceLocalConsumer(vertx.eventBus(), ON_CASSANDRA_CQL_READY.address(),
                                        ignored -> {
                                            storageProvider.initialize();
                                            executor.schedule(this);
                                        });
    }

    @Override
    public DurationSpec delay()
    {
        return config.localJobCoordinationDelay();
    }

    @Override
    public DurationSpec initialDelay()
    {
        return randomizedInitialDelay;
    }

    @Override
    public ScheduleDecision scheduleDecision()
    {
        if (!config.coordinationEnabled())
        {
            return ScheduleDecision.SKIP;
        }
        return storageProvider.isAvailable() ? ScheduleDecision.EXECUTE : ScheduleDecision.SKIP;
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        try
        {
            if (!storageProvider.isAvailable())
            {
                promise.complete();
                return;
            }

            Map<OperationType, UUID> activeOps = operationalJobCoordinator.getActiveOperations();
            if (activeOps.isEmpty())
            {
                resetState();
                promise.complete();
                return;
            }

            for (Map.Entry<OperationType, UUID> entry : activeOps.entrySet())
            {
                OperationType operationType = entry.getKey();
                UUID operationId = entry.getValue();

                OperationalJobRecord jobRecord = storageProvider.findJob(operationId);
                if (jobRecord == null)
                {
                    continue;
                }

                List<List<UUID>> nodeExecutionOrder = jobRecord.nodeExecutionOrder();
                if (nodeExecutionOrder == null || nodeExecutionOrder.isEmpty())
                {
                    continue;
                }

                // nodeStatuses is a snapshot from storage. Local mutations, such as during crash recovery,
                // may make it partially stale, but this approach is conservative. Finalization requires all
                // nodes to be terminal, so stale CREATED entries can only delay finalization.
                Map<UUID, OperationalJobStatus> nodeStatuses = storageProvider.getNodeStatusesForOperation(operationId);

                trySubmitJobsForLocalInstances(operationId, operationType, jobRecord, nodeExecutionOrder, nodeStatuses);

                checkAndFinalizeJobIfComplete(operationId, operationType, jobRecord, nodeStatuses);
            }

            promise.complete();
        }
        catch (Exception e)
        {
            LOGGER.error("Error during local job coordination poll", e);
            promise.complete();
        }
    }

    private void trySubmitJobsForLocalInstances(UUID operationId,
                                                 OperationType operationType,
                                                 OperationalJobRecord jobRecord,
                                                 List<List<UUID>> nodeExecutionOrder,
                                                 Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        for (InstanceMetadata instance : instanceMetadataFetcher.allLocalInstances())
        {
            UUID hostId;
            String host;
            try
            {
                NodeSettings nodeSettings = instance.delegate().nodeSettings();
                hostId = nodeSettings.hostId();
                host = instance.host();
            }
            catch (Exception e)
            {
                LOGGER.debug("Skipping instance {} — delegate unavailable", instance.host(), e);
                continue;
            }

            int groupIndex = findGroupIndex(hostId, nodeExecutionOrder);
            if (groupIndex < 0)
            {
                continue;
            }

            OperationalJobStatus currentStatus = nodeStatuses.get(hostId);

            if (currentStatus == OperationalJobStatus.SUCCEEDED || currentStatus == OperationalJobStatus.FAILED)
            {
                continue;
            }

            // If the job is RUNNING but we don't have a local handle, Sidecar might have crashed
            if (currentStatus == OperationalJobStatus.RUNNING && localJobManager.getJob(operationId, hostId) == null)
            {
                handleCrashRecovery(operationId, operationType, hostId, host, nodeExecutionOrder, nodeStatuses,
                                    jobRecord.operationMetadata());
                continue;
            }

            if (localJobManager.getJob(operationId, hostId) != null)
            {
                continue;
            }

            boolean requiresPredecessorSuccess = localJobFactory.requiresPredecessorSuccess();
            if (!arePredecessorsComplete(hostId, nodeExecutionOrder, nodeStatuses, requiresPredecessorSuccess))
            {
                if (requiresPredecessorSuccess && hasFailedPredecessor(hostId, nodeExecutionOrder, nodeStatuses))
                {
                    LOGGER.warn("Predecessor failed and operation requires predecessor success — marking node {} " +
                                "as FAILED. operationId={}", hostId, operationId);
                    reportTerminalNodeStatusWithRetry(operationId, hostId, OperationalJobStatus.FAILED, 1);
                }
                // Else, a predecessor is still running, so wait for the next poll
                continue;
            }

            if (!isGroupSettleElapsed(operationId, groupIndex, jobRecord.operationMetadata()))
            {
                continue;
            }

            submitLocalJob(operationId, operationType, hostId, host, jobRecord.operationMetadata());
        }
    }

    /**
     * Whether the settle window before execution group {@code groupIndex} has elapsed, so the nodes in the
     * previous group have time to become ready before the next group starts.
     *
     * <p>The window starts when this Sidecar first observes that the previous group is complete, because the
     * actual completion time is not recorded. Observation always comes after completion, so the real wait is
     * never shorter than the configured wait.</p>
     *
     * <p>After a Sidecar restart, nodes in one group can start at different times. {@link #handleCrashRecovery}
     * resubmits a {@code RUNNING} node without this check, because that node passed the check before the Sidecar
     * restart. A {@code CREATED} node in the same group waits a new full window. This is safe, because running
     * the nodes of a group one after another is more conservative than running them together.</p>
     *
     * @return {@code true} if the next group may start, {@code false} to re-check on a later poll
     */
    private boolean isGroupSettleElapsed(UUID operationId, int groupIndex,
                                         Map<String, String> operationMetadata)
    {
        if (groupIndex == 0)
        {
            return true;
        }

        SecondBoundConfiguration wait = localJobFactory.waitBetweenExecutionGroups(operationMetadata);
        if (wait == null || wait.toMillis() <= 0)
        {
            return true;
        }

        long nowNanos = timeProvider.nanoTime();
        long observedAtNanos = predecessorsCompletedAtNanos.computeIfAbsent(new GroupKey(operationId, groupIndex),
                                                                           key -> nowNanos);
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(nowNanos - observedAtNanos);
        if (elapsedMs >= wait.toMillis())
        {
            return true;
        }

        long remainingSeconds = (long) Math.ceil((wait.toMillis() - elapsedMs) / 1000.0);
        LOGGER.info("Waiting {}s more before starting execution group {} — {}s of the {}s settle window has "
                    + "elapsed. operationId={}", remainingSeconds, groupIndex,
                    TimeUnit.MILLISECONDS.toSeconds(elapsedMs), wait.toSeconds(), operationId);
        return false;
    }

    private void handleCrashRecovery(UUID operationId, OperationType operationType,
                                      UUID hostId, String host,
                                      List<List<UUID>> nodeExecutionOrder,
                                      Map<UUID, OperationalJobStatus> nodeStatuses,
                                      Map<String, String> operationMetadata)
    {
        if (localJobFactory.canRecover())
        {
            LOGGER.info("Node {} can recover — resetting to CREATED and resubmitting. operationId={}",
                         hostId, operationId);
            reportNonTerminalNodeStatus(operationId, hostId, OperationalJobStatus.CREATED);
            nodeStatuses.put(hostId, OperationalJobStatus.CREATED);
            submitLocalJob(operationId, operationType, hostId, host, operationMetadata);
        }
        else
        {
            LOGGER.warn("Node {} cannot recover from RUNNING state — marking as FAILED. operationId={}",
                         hostId, operationId);
            reportTerminalNodeStatusWithRetry(operationId, hostId, OperationalJobStatus.FAILED, 1);
            LocalJob job = localJobFactory.createJob(operationId, hostId, host, operationType, operationMetadata);
            recoverFailedNode(job, operationId, hostId);
        }
    }

    private void submitLocalJob(UUID operationId, OperationType operationType, UUID hostId, String host,
                                Map<String, String> operationMetadata)
    {
        LocalJob localJob = localJobFactory.createJob(operationId, hostId, host, operationType, operationMetadata);
        reportNonTerminalNodeStatus(operationId, hostId, OperationalJobStatus.RUNNING);

        // Bound every RUNNING node so a hanging job can't stall the operation. Whichever of execution completes or
        // timeout fires happens first reports the terminal status, the other is a no-op via terminalReported to avoid
        // a race condition between the two.
        // Failing jobs when Sidecar is permanently down will be handled by (https://issues.apache.org/jira/browse/CASSSIDECAR-485).
        AtomicBoolean terminalReported = new AtomicBoolean(false);
        long timeoutMs = resolveNodeExecutionTimeoutMs(operationType);
        long timerId = executorPools.internal().setTimer(timeoutMs, id -> {
            if (terminalReported.compareAndSet(false, true))
            {
                LOGGER.warn("Node {} exceeded execution timeout of {}ms — marking as FAILED and cancelling the job. "
                            + "operationId={}", hostId, timeoutMs, operationId);
                reportTerminalNodeStatusWithRetry(operationId, hostId, OperationalJobStatus.FAILED, 1);
                localJob.cancel();
            }
        });

        localJobManager.submitJob(localJob).onComplete(ar -> executorPools.internal().runBlocking(() -> {
            executorPools.internal().cancelTimer(timerId);
            if (terminalReported.compareAndSet(false, true))
            {
                boolean failed = ar.failed();
                reportTerminalNodeStatusWithRetry(operationId, hostId,
                                                  failed ? OperationalJobStatus.FAILED : OperationalJobStatus.SUCCEEDED,
                                                  1);
                if (failed)
                {
                    LOGGER.error("Local job failed for node {}. operationId={}", hostId, operationId, ar.cause());
                    recoverFailedNode(localJob, operationId, hostId);
                }
            }
            else
            {
                recoverFailedNode(localJob, operationId, hostId);
            }
        }));
    }

    /**
     * Runs the job's own recovery on its FAILED transition. Invoked only after the job has stopped. Failures
     * are logged but ignored so recovery can never stall the poll loop.
     */
    private void recoverFailedNode(LocalJob localJob, UUID operationId, UUID hostId)
    {
        localJob.onJobFailed()
                .onFailure(e -> LOGGER.error("onJobFailed recovery failed for node {}. operationId={}",
                                             hostId, operationId, e));
    }

    private long resolveNodeExecutionTimeoutMs(OperationType operationType)
    {
        SecondBoundConfiguration override = localJobFactory.nodeExecutionTimeout();
        return override != null ? override.toMillis() : defaultNodeExecutionTimeoutMs;
    }

    /**
     * Reports a terminal node status, retrying with backoff on failure.
     */
    private void reportTerminalNodeStatusWithRetry(UUID operationId, UUID nodeId,
                                                   OperationalJobStatus status, int attempt)
    {
        try
        {
            reportNodeStatus(operationId, nodeId, status);
        }
        catch (Exception e)
        {
            LOGGER.warn("Failed to report status {} for node {} (attempt {}/{}). operationId={}",
                        status, nodeId, attempt, MAX_NODE_STATUS_RETRIES, operationId, e);
            if (attempt < MAX_NODE_STATUS_RETRIES)
            {
                executorPools.internal().setTimer(NODE_STATUS_RETRY_DELAY_MS * attempt,
                                                  id -> reportTerminalNodeStatusWithRetry(operationId, nodeId, status,
                                                                                          attempt + 1));
            }
            else
            {
                LOGGER.error("Exhausted retries reporting status {} for node {}. operationId={}. " +
                             "Manual intervention may be required.", status, nodeId, operationId);
            }
        }
    }

    @Override
    public boolean arePredecessorsComplete(UUID nodeId,
                                            List<List<UUID>> nodeExecutionOrder,
                                            Map<UUID, OperationalJobStatus> nodeStatuses,
                                            boolean requiresPredecessorSuccess)
    {
        int nodeGroupIndex = findGroupIndex(nodeId, nodeExecutionOrder);
        if (nodeGroupIndex < 0)
        {
            LOGGER.error("Node {} not found in execution order — cannot determine predecessor status", nodeId);
            return false;
        }
        if (nodeGroupIndex == 0)
        {
            return true;
        }
        // Only need to check the immediate predecessor group. A node executes only after that group is terminal,
        // so every earlier group has already finished. A node failed by cascade makes the groups after it fail by
        // cascade too, so they never execute.
        for (UUID predecessorId : nodeExecutionOrder.get(nodeGroupIndex - 1))
        {
            OperationalJobStatus status = nodeStatuses.get(predecessorId);
            if (status == null)
            {
                LOGGER.error("Predecessor node {} has null status — expected a status entry", predecessorId);
                return false;
            }
            if (status == OperationalJobStatus.CREATED || status == OperationalJobStatus.RUNNING)
            {
                return false;
            }
            if (status == OperationalJobStatus.FAILED && requiresPredecessorSuccess)
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void reportNodeStatus(UUID operationId, UUID nodeId, OperationalJobStatus status)
    {
        storageProvider.updateNodeStatus(operationId, nodeId, status);
    }

    @Override
    public void checkAndFinalizeJobIfComplete(UUID operationId,
                                               OperationType operationType,
                                               OperationalJobRecord jobRecord,
                                               Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        List<List<UUID>> nodeExecutionOrder = jobRecord.nodeExecutionOrder();
        if (nodeExecutionOrder == null)
        {
            return;
        }

        boolean anyFailed = nodeStatuses.containsValue(OperationalJobStatus.FAILED);
        boolean allTerminal = true;

        for (List<UUID> group : nodeExecutionOrder)
        {
            for (UUID nodeId : group)
            {
                OperationalJobStatus status = nodeStatuses.get(nodeId);
                if (status == null)
                {
                    if (anyFailed)
                    {
                        LOGGER.warn("Node {} has null status but other nodes have failed — treating as FAILED. " +
                                    "operationId={}", nodeId, operationId);
                        continue;
                    }
                    LOGGER.error("Node {} has null status during finalization check — expected a status entry", nodeId);
                    allTerminal = false;
                    break;
                }
                if (!status.isCompleted())
                {
                    allTerminal = false;
                    break;
                }
                if (status == OperationalJobStatus.FAILED)
                {
                    anyFailed = true;
                }
            }
            if (!allTerminal)
            {
                break;
            }
        }

        if (!allTerminal)
        {
            return;
        }

        OperationalJobStatus overallStatus = anyFailed ? OperationalJobStatus.FAILED : OperationalJobStatus.SUCCEEDED;
        String failureReason = anyFailed ? "One or more nodes failed during operation" : null;
        storageProvider.updateJobStatus(operationId, operationType, overallStatus, failureReason);
        operationalJobCoordinator.clearActive(operationType, operationId);
        LOGGER.info("Job finalized. operationId={} operationType={} status={}", operationId, operationType, overallStatus);
        cleanupOperationState(operationId);
    }

    /**
     * Not retried, as a delayed retry could land after a terminal write and roll the node status back.
     */
    private void reportNonTerminalNodeStatus(UUID operationId, UUID nodeId, OperationalJobStatus status)
    {
        try
        {
            reportNodeStatus(operationId, nodeId, status);
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to report status {} for node {}. operationId={}", status, nodeId, operationId, e);
        }
    }

    private int findGroupIndex(UUID nodeId, List<List<UUID>> nodeExecutionOrder)
    {
        for (int i = 0; i < nodeExecutionOrder.size(); i++)
        {
            if (nodeExecutionOrder.get(i).contains(nodeId))
            {
                return i;
            }
        }
        return -1;
    }

    private boolean hasFailedPredecessor(UUID nodeId,
                                         List<List<UUID>> nodeExecutionOrder,
                                         Map<UUID, OperationalJobStatus> nodeStatuses)
    {
        int nodeGroupIndex = findGroupIndex(nodeId, nodeExecutionOrder);
        if (nodeGroupIndex <= 0)
        {
            return false;
        }
        for (UUID predecessorId : nodeExecutionOrder.get(nodeGroupIndex - 1))
        {
            if (nodeStatuses.get(predecessorId) == OperationalJobStatus.FAILED)
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Identifies an execution group within an operation
     */
    private static final class GroupKey
    {
        private final UUID operationId;
        private final int groupIndex;

        private GroupKey(UUID operationId, int groupIndex)
        {
            this.operationId = operationId;
            this.groupIndex = groupIndex;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
            {
                return true;
            }
            if (!(other instanceof GroupKey))
            {
                return false;
            }
            GroupKey that = (GroupKey) other;
            return groupIndex == that.groupIndex && operationId.equals(that.operationId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(operationId, groupIndex);
        }
    }

    private void cleanupJobsForOperation(UUID operationId)
    {
        for (LocalJob job : localJobManager.activeJobs())
        {
            if (operationId.equals(job.operationId()))
            {
                localJobManager.removeJob(job.operationId(), job.nodeId(), job);
            }
        }
    }

    /**
     * Clears the state and local job handles for a single operation.
     */
    private void cleanupOperationState(UUID operationId)
    {
        predecessorsCompletedAtNanos.keySet().removeIf(key -> key.operationId.equals(operationId));
        cleanupJobsForOperation(operationId);
    }

    /**
     * Clears all local state. Only safe when no operations are active
     */
    private void resetState()
    {
        predecessorsCompletedAtNanos.clear();
        for (LocalJob job : localJobManager.activeJobs())
        {
            localJobManager.removeJob(job.operationId(), job.nodeId(), job);
        }
    }
}
