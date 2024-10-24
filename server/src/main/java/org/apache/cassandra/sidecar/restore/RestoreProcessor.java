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

package org.apache.cassandra.sidecar.restore;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.cluster.locator.LocalTokenRangesProvider;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.concurrent.ConcurrencyLimiter;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.exceptions.RestoreJobException;
import org.apache.cassandra.sidecar.exceptions.RestoreJobExceptions;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.instance.InstanceRestoreMetrics;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.utils.SSTableImporter;

/**
 * Handles processing of all {@link RestoreRange} s related to {@link org.apache.cassandra.sidecar.db.RestoreJob}
 */
@Singleton
public class RestoreProcessor implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreProcessor.class);

    private final TaskExecutorPool pool;
    private final StorageClientPool s3ClientPool;
    private final SidecarSchema sidecarSchema;
    private final SSTableImporter importer;
    private final ConcurrencyLimiter processMaxConcurrency;
    private final WorkQueue workQueue = new WorkQueue();
    private final double requiredUsableSpacePercentage; // value range: [0.0, 1.0)
    private final RestoreRangeDatabaseAccessor rangeDatabaseAccessor;
    private final RestoreJobUtil restoreJobUtil;
    // mapping of task to the time when it should be reported as 'slow' if it is still active
    // using concurrent data structure because the map is accessed from multiple threads
    private final Map<RestoreRangeHandler, Long> activeTasks = new ConcurrentHashMap<>();
    private final long slowTaskThresholdInSeconds;
    private final long slowTaskReportDelayInSeconds;
    private final LocalTokenRangesProvider localTokenRangesProvider;
    private final SidecarMetrics metrics;

    private volatile boolean isClosed = false; // OK to run close twice, so relax the control to volatile

    @Inject
    public RestoreProcessor(ExecutorPools executorPools,
                            SidecarConfiguration config,
                            SidecarSchema sidecarSchema,
                            StorageClientPool s3ClientPool,
                            SSTableImporter importer,
                            RestoreRangeDatabaseAccessor rangeDatabaseAccessor,
                            RestoreJobUtil restoreJobUtil,
                            LocalTokenRangesProvider localTokenRangesProvider,
                            SidecarMetrics metrics)
    {
        this.pool = executorPools.internal();
        this.s3ClientPool = s3ClientPool;
        this.sidecarSchema = sidecarSchema;
        this.processMaxConcurrency = new ConcurrencyLimiter(() -> config.restoreJobConfiguration()
                                                                        .processMaxConcurrency());
        this.requiredUsableSpacePercentage
        = config.serviceConfiguration().sstableUploadConfiguration().minimumSpacePercentageRequired() / 100.0;
        this.slowTaskThresholdInSeconds = config.restoreJobConfiguration().slowTaskThresholdSeconds();
        this.slowTaskReportDelayInSeconds = config.restoreJobConfiguration().slowTaskReportDelaySeconds();
        this.importer = importer;
        this.rangeDatabaseAccessor = rangeDatabaseAccessor;
        this.restoreJobUtil = restoreJobUtil;
        this.localTokenRangesProvider = localTokenRangesProvider;
        this.metrics = metrics;
    }

    /**
     * Enqueue a {@link RestoreRange} to be processed in the future.
     * If the processor has been closed, it won't accept more submissions.
     */
    void submit(RestoreRange range)
    {
        if (isClosed)
            return;

        workQueue.offer(range);
    }

    /**
     * Remove the restore range from work queue
     * @param range restore range to be removed
     */
    void remove(RestoreRange range)
    {
        if (isClosed)
            return;

        workQueue.remove(range);
    }

    @Override
    public boolean shouldSkip()
    {
        boolean shouldSkip = !sidecarSchema().isInitialized();
        if (shouldSkip)
        {
            LOGGER.trace("Skipping restore job processing");
        }
        return shouldSkip;
    }

    @Override
    public long delay()
    {
        // try to run the loop every 1 second.
        return 1000;
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        while (workQueue.peek() != null // exit early when no pending slice and avoid acquire permits
               && processMaxConcurrency.tryAcquire())
        {
            RestoreRange range = workQueue.poll();
            if (range == null) // It should never happen because it peeks before polling. It is only to make IDE happy
            {
                processMaxConcurrency.releasePermit();
                break; // break in order to complete promise
            }

            // only create task when the restore job status is import ready for the staged ranges
            // otherwise, putting the range back to the staged queue and exit early.
            if (range.hasStaged() && range.job().status != RestoreJobStatus.IMPORT_READY)
            {
                workQueue.offerStaged(range);
                processMaxConcurrency.releasePermit();
                break;
            }

            Preconditions.checkState(range.canProduceTask(),
                                     "RestoreRangeTask cannot be produced by range " + range.shortDescription());

            // capture the new queue length after polling
            workQueue.captureImportQueueLength();
            RestoreRangeHandler task = range.toAsyncTask(s3ClientPool, pool, importer,
                                                         requiredUsableSpacePercentage,
                                                         rangeDatabaseAccessor,
                                                         restoreJobUtil,
                                                         localTokenRangesProvider,
                                                         metrics);

            activeTasks.put(task, slowTaskThresholdInSeconds);
            pool.executeBlocking(task, false) // unordered; run in parallel
                // wrap success/failure handling in compose to catch any exception thrown
                .compose(taskSuccessHandler(task),
                         taskFailureHandler(range))
                // release counter
                .onComplete(ignored -> {
                    processMaxConcurrency.releasePermit();
                    // decrement the active slices and capture the new queue length
                    workQueue.decrementActiveSliceCount(range);
                    workQueue.captureImportQueueLength();
                    activeTasks.remove(task);
                });
        }
        checkForLongRunningTasks();
        workQueue.capturePendingSliceCount();
        promise.tryComplete();
    }

    @Override
    public void close()
    {
        isClosed = true;
        s3ClientPool.close();
        workQueue.close();
    }

    private void checkForLongRunningTasks()
    {
        for (RestoreRangeHandler t : activeTasks.keySet())
        {
            long elapsedInNanos = t.elapsedInNanos();
            if (elapsedInNanos == -1) // not started
            {
                continue;
            }

            long elapsedInSeconds = TimeUnit.NANOSECONDS.toSeconds(elapsedInNanos);

            // Read the current map and update the existing entries if needed.
            // We do not want to put new entry to the map in this method.
            activeTasks.computeIfPresent(t, (task, timeToReport) -> {
                if (elapsedInSeconds > timeToReport)
                {
                    LOGGER.warn("Long-running restore slice task detected. " +
                                "elapsedSeconds={} thresholdSeconds={} sliceKey={} jobId={} status={}",
                                elapsedInSeconds,
                                slowTaskThresholdInSeconds,
                                task.range().sliceKey(),
                                task.range().jobId(),
                                task.range().job().status);
                    task.range()
                        .owner()
                        .metrics()
                        .restore().slowRestoreTaskTime.metric.update(elapsedInNanos, TimeUnit.NANOSECONDS);
                    return timeToReport + slowTaskReportDelayInSeconds; // increment by the delay
                }

                return timeToReport; // do not update
            });
        }
    }

    private Function<RestoreRange, Future<Void>> taskSuccessHandler(RestoreRangeHandler task)
    {
        return range -> {
            InstanceRestoreMetrics restoreMetrics = range.owner().metrics().restore();
            if (range.hasImported())
            {
                restoreMetrics.sliceCompletionTime.metric.update(System.nanoTime() - range.sliceCompressedSize(), TimeUnit.NANOSECONDS);
                LOGGER.info("Restore range completes successfully. sliceKey={}", range.sliceKey());
                range.complete();
            }
            else if (range.hasStaged())
            {
                restoreMetrics.sliceStageTime.metric.update(task.elapsedInNanos(), TimeUnit.NANOSECONDS);
                LOGGER.info("Restore range has been staged successfully. sliceKey={}", range.sliceKey());
                // the slice is not fully complete yet. Re-enqueue the slice to the staged queue.
                workQueue.offerStaged(range);
            }
            else // log a warning and retry. It should not reach here.
            {
                LOGGER.warn("Unexpected state of slice. It is neither staged nor imported. sliceKey={}",
                            range.sliceKey());
                if (range.hasStaged())
                {
                    workQueue.offerStaged(range);
                }
                else
                {
                    workQueue.offer(range);
                }
            }
            return Future.succeededFuture();
        };
    }

    private Function<Throwable, Future<Void>> taskFailureHandler(RestoreRange range)
    {
        return cause -> {
            if (cause instanceof RestoreJobException && ((RestoreJobException) cause).retryable())
            {
                LOGGER.warn("Slice failed with recoverable failure. sliceKey={}", range.sliceKey(), cause);
                // re-enqueue the retryable failed slice
                if (range.hasStaged())
                {
                    workQueue.offerStaged(range);
                }
                else
                {
                    workQueue.offer(range);
                }
            }
            else
            {
                LOGGER.error("Slice failed with unrecoverable failure. sliceKey={}", range.sliceKey(), cause);
                range.fail(RestoreJobExceptions.toFatal(cause));
                if (range.job().isManagedBySidecar())
                {
                    rangeDatabaseAccessor.updateStatus(range);
                }
                // revoke the s3 credentials of the job too
                s3ClientPool.revokeCredentials(range.jobId());
            }
            return Future.succeededFuture();
        };
    }

    @VisibleForTesting
    int activeSlices()
    {
        return workQueue.activeRangesCount();
    }

    @VisibleForTesting
    int activeTasks()
    {
        return activeTasks.size();
    }

    @VisibleForTesting
    int pendingStartSlices()
    {
        return workQueue.size();
    }

    @VisibleForTesting
    SidecarSchema sidecarSchema()
    {
        return sidecarSchema;
    }

    /**
     * A facade that encapsulates the queuing of restore ranges of 2 phases, newly created and staged, as well as related metrics aggregation.
     * <ul>
     * <li>
     * When the restore job is managed by Spark/external controller, restore ranges are only enqueued in the restoreRanges queue, as the job
     * has only 1 phase. The stagedRestoreRanges queue is not used at all.
     * </li>
     * <li>
     * When the restore job is managed by Sidecar, there are 2 phases in the import/restore procedure. The newly created restore ranges are
     * enqueued in the restoreRanges queue first, once the slices/objects have been downloaded, the restore ranges are enqueued into the
     * stagedRestoreRanges queue. When it is ready to import, RestoreProcessor polls restore ranges from the staged queue and import.
     * </li>
     * </ul>
     */
    private class WorkQueue
    {
        // use concurrent collection for non-blocking read operations
        // the work queue for all newly create restore ranges
        private final Queue<RestoreRange> restoreRanges = new ConcurrentLinkedQueue<>();
        // the work queue for staged restore ranges
        private final Queue<RestoreRange> stagedRestoreRanges = new ConcurrentLinkedQueue<>();
        // use non-concurrent map since all the update operations are (required to)
        // synchronized for restoreRanges and pendingRangesPerInstance
        private final Map<Integer, AtomicInteger> pendingRangesPerInstance = new HashMap<>();
        // use concurrent map to read latest map content, e.g. capture stats, count size, etc.
        private final Map<Integer, AtomicInteger> activeRangesPerInstance = new ConcurrentHashMap<>();

        synchronized boolean offer(RestoreRange range)
        {
            increment(pendingRangesPerInstance, range);
            return restoreRanges.offer(range);
        }

        synchronized boolean offerStaged(RestoreRange range)
        {
            increment(pendingRangesPerInstance, range);
            return stagedRestoreRanges.offer(range);
        }

        synchronized void remove(RestoreRange range)
        {
            decrementIfPresent(pendingRangesPerInstance, range);
            restoreRanges.remove(range);
        }

        synchronized RestoreRange poll()
        {
            RestoreRange slice = pollInternal();
            if (slice == null)
            {
                return null;
            }

            decrementIfPresent(pendingRangesPerInstance, slice);
            increment(activeRangesPerInstance, slice);
            return slice;
        }

        synchronized void close()
        {
            for (RestoreRange range : restoreRanges)
            {
                range.cancel();
                LOGGER.debug("Cancelled restore ranges on closing. jobId={} sliceId={} startToken={} endToken={}",
                             range.jobId(), range.sliceId(), range.startToken(), range.endToken());
            }
            restoreRanges.clear();
            pendingRangesPerInstance.clear();
            activeRangesPerInstance.clear();
        }

        void decrementActiveSliceCount(RestoreRange range)
        {
            decrementIfPresent(activeRangesPerInstance, range);
        }

        void captureImportQueueLength()
        {
            activeRangesPerInstance.forEach((instanceId, counter) ->
                                            instanceRestoreMetrics(instanceId).sliceImportQueueLength.metric.setValue(counter.get()));
        }

        void capturePendingSliceCount()
        {
            pendingRangesPerInstance.forEach((instanceId, counter) ->
                                             instanceRestoreMetrics(instanceId).pendingSliceCount.metric.setValue(counter.get()));
        }

        private void increment(Map<Integer, AtomicInteger> map, RestoreRange range)
        {
            map.compute(range.owner().id(), (key, counter) -> {
                if (counter == null)
                {
                    counter = new AtomicInteger();
                }
                counter.incrementAndGet();
                return counter;
            });
        }

        private void decrementIfPresent(Map<Integer, AtomicInteger> map, RestoreRange range)
        {
            map.computeIfPresent(range.owner().id(), (key, counter) -> {
                if (counter.get() < 0) // The condition is not expected. Log it if it happens
                {
                    // create an IllegalStateException to capture stacktrace
                    LOGGER.warn("Slice counter dropped below 0. sliceKey={}",
                                range.sliceKey(), new IllegalStateException("Unexpected slice counter state"));
                    counter.set(0); // repair anomaly
                    return counter;
                }
                counter.decrementAndGet();
                return counter;
            });
        }

        private RestoreRange peek()
        {
            RestoreRange range = restoreRanges.peek();
            if (range == null)
            {
                range = stagedRestoreRanges.peek();
            }
            return range;
        }

        private RestoreRange pollInternal()
        {
            RestoreRange range = restoreRanges.poll();
            if (range == null)
            {
                range = stagedRestoreRanges.poll();
            }

            return range;
        }

        private InstanceRestoreMetrics instanceRestoreMetrics(int instanceId)
        {
            return metrics.instance(instanceId).restore();
        }

        @VisibleForTesting
        int size()
        {
            return restoreRanges.size();
        }

        @VisibleForTesting
        int activeRangesCount()
        {
            return activeRangesPerInstance.values().stream().mapToInt(AtomicInteger::get).sum();
        }
    }
}
