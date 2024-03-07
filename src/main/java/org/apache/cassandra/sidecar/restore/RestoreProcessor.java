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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.concurrent.ConcurrencyLimiter;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.db.RestoreSliceDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.exceptions.RestoreJobException;
import org.apache.cassandra.sidecar.exceptions.RestoreJobExceptions;
import org.apache.cassandra.sidecar.stats.RestoreJobStats;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.utils.SSTableImporter;

/**
 * Handles processing of all {@link RestoreSlice} s related to {@link org.apache.cassandra.sidecar.db.RestoreJob}
 */
@Singleton
public class RestoreProcessor implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreProcessor.class);

    private final ExecutorPools.TaskExecutorPool pool;
    private final StorageClientPool s3ClientPool;
    private final SidecarSchema sidecarSchema;
    private final SSTableImporter importer;
    private final ConcurrencyLimiter processMaxConcurrency;
    private final SliceQueue sliceQueue = new SliceQueue();
    private final double requiredUsableSpacePercentage; // value range: [0.0, 1.0)
    private final RestoreSliceDatabaseAccessor sliceDatabaseAccessor;
    private final RestoreJobStats stats;
    private final RestoreJobUtil restoreJobUtil;
    private final List<RestoreSliceHandler> activeHandlers = Collections.synchronizedList(new ArrayList<>());
    private final long longRunningHandlerThresholdInSeconds;
    
    private volatile boolean isClosed = false; // OK to run close twice, so relax the control to volatile

    @Inject
    public RestoreProcessor(ExecutorPools executorPools,
                            SidecarConfiguration config,
                            SidecarSchema sidecarSchema,
                            StorageClientPool s3ClientPool,
                            SSTableImporter importer,
                            RestoreSliceDatabaseAccessor sliceDatabaseAccessor,
                            RestoreJobStats stats,
                            RestoreJobUtil restoreJobUtil)
    {
        this.pool = executorPools.internal();
        this.s3ClientPool = s3ClientPool;
        this.sidecarSchema = sidecarSchema;
        this.processMaxConcurrency = new ConcurrencyLimiter(() -> config.restoreJobConfiguration()
                                                                        .processMaxConcurrency());
        this.requiredUsableSpacePercentage
        = config.serviceConfiguration().ssTableUploadConfiguration().minimumSpacePercentageRequired() / 100.0;
        this.longRunningHandlerThresholdInSeconds = config.restoreJobConfiguration()
                                                          .restoreJobLongRunningHandlerThresholdSeconds();
        this.importer = importer;
        this.sliceDatabaseAccessor = sliceDatabaseAccessor;
        this.stats = stats;
        this.restoreJobUtil = restoreJobUtil;
    }

    /**
     * Enqueue a {@link RestoreSlice} to be processed in the future.
     * If the processor has been closed, it won't accept more slices.
     */
    void submit(RestoreSlice slice)
    {
        if (isClosed)
            return;

        sliceQueue.offer(slice);
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
        while (sliceQueue.peek() != null // exit early when no pending slice and avoid acquire permits
               && processMaxConcurrency.tryAcquire())
        {
            RestoreSlice slice = sliceQueue.poll();
            if (slice == null) // it should never happen, and is only to make ide happy
            {
                processMaxConcurrency.releasePermit();
                break;
            }

            // capture the new queue length after polling
            sliceQueue.captureImportQueueLength();
            RestoreSliceHandler sliceHandler = slice.toAsyncTask(s3ClientPool, pool, importer,
                                                                 requiredUsableSpacePercentage,
                                                                 sliceDatabaseAccessor, stats,
                                                                 restoreJobUtil);
            activeHandlers.add(sliceHandler);
            pool.executeBlocking(sliceHandler, false) // unordered
            .onSuccess(restoreSlice -> {
                int instanceId = slice.owner().id();
                if (slice.hasImported())
                {
                    stats.captureSliceCompletionTime(instanceId, System.nanoTime() - slice.creationTimeNanos());
                    LOGGER.info("Slice completes successfully. sliceKey={}", slice.key());
                    slice.complete();
                }
                else if (slice.hasStaged())
                {
                    stats.captureSliceStageTime(instanceId, sliceHandler.getDuration());
                    LOGGER.info("Slice has been staged successfully. sliceKey={}", slice.key());
                    // the slice is not fully complete yet. Re-enqueue the slice.
                    sliceQueue.offer(slice);
                }
                else // log a warning and retry. It should not reach here.
                {
                    LOGGER.warn("Unexpected state of slice. It is neither staged nor imported. sliceKey={}",
                                slice.key());
                    sliceQueue.offer(slice);
                }
            })
            .onFailure(cause -> {
                if (cause instanceof RestoreJobException && ((RestoreJobException) cause).retryable())
                {
                    LOGGER.warn("Slice failed with recoverable failure. sliceKey={}", slice.key(), cause);
                    // re-enqueue the retryable failed slice
                    sliceQueue.offer(slice);
                }
                else
                {
                    LOGGER.error("Slice failed with unrecoverable failure. sliceKey={}", slice.key(), cause);
                    // fail the slice and mark the slice has failed on its owning instance.
                    // In the phase 1 implementation, all slices of the job get aborted
                    slice.fail(RestoreJobExceptions.toFatal(cause));
                    if (slice.job().isManagedBySidecar())
                    {
                        sliceDatabaseAccessor.updateStatus(slice);
                    }
                    // revoke the s3 credentials of the job too
                    s3ClientPool.revokeCredentials(slice.jobId());
                }
            })
            // release counter
            .onComplete(res -> {
                processMaxConcurrency.releasePermit();
                // decrement the active slices and capture the new queue length
                sliceQueue.decrementActiveSliceCount(slice);
                sliceQueue.captureImportQueueLength();
                activeHandlers.remove(sliceHandler);
            });
        }
        checkForLongRunningTasks();
        promise.tryComplete();
        sliceQueue.capturePendingSliceCount();
    }

    private void checkForLongRunningTasks()
    {
        for (RestoreSliceHandler handler: activeHandlers)
        {
            long handlerDuration = handler.getDuration();
            long runtimeInSeconds = TimeUnit.SECONDS.convert(handlerDuration, TimeUnit.NANOSECONDS);
            if (runtimeInSeconds > longRunningHandlerThresholdInSeconds)
            {
                LOGGER.warn("Restore Handler has been running for {} seconds. limit {} sliceKey {}, job status {}",
                            runtimeInSeconds,
                            longRunningHandlerThresholdInSeconds,
                            handler.slice().key(),
                            handler.slice().job().status);
                stats.captureLongRunningRestoreHandler(handler.slice().owner().id(), handlerDuration);
            }
        }
    }

    @Override
    public void close()
    {
        isClosed = true;
        s3ClientPool.close();
        sliceQueue.close();
    }

    @VisibleForTesting
    int activeSlices()
    {
        return sliceQueue.activeSliceCount();
    }

    @VisibleForTesting
    int activeHandlers()
    {
        return activeHandlers.size();
    }

    @VisibleForTesting
    int pendingStartSlices()
    {
        return sliceQueue.size();
    }

    @VisibleForTesting
    SidecarSchema sidecarSchema()
    {
        return sidecarSchema;
    }

    private class SliceQueue
    {
        // use concurrent collection for non-blocking read operations
        private Queue<RestoreSlice> sliceQueue = new ConcurrentLinkedQueue<>();
        // use non-concurrent map since all the update operations are (required to)
        // synchronized for sliceQueue and sliceCounterPerInstance
        private Map<Integer, AtomicInteger> sliceCounterPerInstance = new HashMap<>();
        // use concurrent map to read latest map content, e.g. capture stats, count size, etc.
        private Map<Integer, AtomicInteger> activeSliceCounterPerInstance = new ConcurrentHashMap<>();

        synchronized boolean offer(RestoreSlice slice)
        {
            increment(sliceCounterPerInstance, slice);
            return sliceQueue.offer(slice);
        }

        synchronized RestoreSlice poll()
        {
            RestoreSlice slice = sliceQueue.poll();
            if (slice == null)
            {
                return null;
            }

            decrementIfPresent(sliceCounterPerInstance, slice);
            increment(activeSliceCounterPerInstance, slice);
            return slice;
        }

        synchronized void close()
        {
            for (RestoreSlice slice : sliceQueue)
            {
                slice.cancel();
                LOGGER.debug("Cancelled slice on closing. jobId={}, sliceId={}", slice.jobId(), slice.sliceId());
            }
            sliceQueue.clear();
            sliceCounterPerInstance.clear();
            activeSliceCounterPerInstance.clear();
        }

        RestoreSlice peek()
        {
            return sliceQueue.peek();
        }

        void decrementActiveSliceCount(RestoreSlice slice)
        {
            decrementIfPresent(activeSliceCounterPerInstance, slice);
        }

        void captureImportQueueLength()
        {
            activeSliceCounterPerInstance.forEach((instanceId, counter) ->
                                                  stats.captureSliceImportQueueLength(instanceId, counter.get()));
        }

        void capturePendingSliceCount()
        {
            sliceCounterPerInstance.forEach((instanceId, counter) ->
                                            stats.capturePendingSliceCount(instanceId, counter.get()));
        }

        private void increment(Map<Integer, AtomicInteger> map, RestoreSlice slice)
        {
            map.compute(slice.owner().id(), (key, counter) -> {
                if (counter == null)
                {
                    counter = new AtomicInteger();
                }
                counter.incrementAndGet();
                return counter;
            });
        }

        private void decrementIfPresent(Map<Integer, AtomicInteger> map, RestoreSlice slice)
        {
            map.computeIfPresent(slice.owner().id(), (key, counter) -> {
                if (counter != null)
                {
                    if (counter.get() < 0) // The condition is not expected. Log it if it happens
                    {
                        // create an IllegalStateException to capture stacktrace
                        LOGGER.warn("Slice counter dropped below 0. sliceKey={}",
                                    slice.key(), new IllegalStateException("Unexpected slice counter state"));
                        counter.set(0); // repair anomaly
                        return counter;
                    }
                    counter.decrementAndGet();
                }
                return counter;
            });
        }

        @VisibleForTesting
        int size()
        {
            return sliceQueue.size();
        }

        @VisibleForTesting
        int activeSliceCount()
        {
            return activeSliceCounterPerInstance.values().stream().mapToInt(AtomicInteger::get).sum();
        }
    }
}
