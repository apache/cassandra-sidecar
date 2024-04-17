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

package org.apache.cassandra.sidecar.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.StopWatch;

/**
 * A pool of executors backed by {@link WorkerExecutor} and {@link Vertx}
 */
public abstract class TaskExecutorPool implements WorkerExecutor
{
    private final Vertx vertx;
    private final WorkerExecutor workerExecutor;

    private TaskExecutorPool(Vertx vertx, WorkerPoolConfiguration config)
    {
        this.vertx = vertx;
        this.workerExecutor = vertx.createSharedWorkerExecutor(config.workerPoolName(),
                                                               config.workerPoolSize(),
                                                               config.workerMaxExecutionTimeMillis(),
                                                               TimeUnit.MILLISECONDS);
    }

    /**
     * Like {@link #setPeriodic(long, Handler, boolean)} with the handler guaranteed to be executed in a
     * worker thread and ordered = false.
     *
     * @param delay   the delay in milliseconds, after which the timer will fire
     * @param handler the handler that will be called with the timer ID when the timer fires
     * @return the unique identifier for the timer
     */
    public long setPeriodic(long delay, Handler<Long> handler)
    {
        return setPeriodic(delay, handler, false);
    }

    /**
     * Like {@link Vertx#setPeriodic(long, Handler)} with the handler guaranteed to be executed in a worker thread.
     *
     * @param delay   the delay in milliseconds, after which the timer will fire
     * @param handler the handler that will be called with the timer ID when the timer fires
     * @param ordered if true and if executeBlocking is called several times on the same context, the executions
     *                for that context will be executed serially, not in parallel. The periodic scheduled
     *                executions could be delayed if the prior execution on the same context is taking longer.
     *                If false then they will be no ordering guarantees
     * @return the unique identifier for the timer
     */
    public long setPeriodic(long delay, Handler<Long> handler, boolean ordered)
    {
        return setPeriodic(delay, delay, handler, ordered);
    }

    /**
     * Set a periodic timer to fire every {@code delay} milliseconds with initial delay, at which point
     * {@code handler} will be called with the id of the timer.
     *
     * @param initialDelay the initial delay in milliseconds
     * @param delay        the delay in milliseconds, after which the timer will fire
     * @param handler      the handler that will be called with the timer ID when the timer fires
     * @return the unique ID of the timer
     */
    public long setPeriodic(long initialDelay, long delay, Handler<Long> handler)
    {
        return setPeriodic(initialDelay, delay, handler, false);
    }

    /**
     * Set a periodic timer to fire every {@code delay} milliseconds with initial delay, at which point
     * {@code handler} will be called with the id of the timer.
     *
     * @param initialDelay the initial delay in milliseconds
     * @param delay        the delay in milliseconds, after which the timer will fire
     * @param handler      the handler that will be called with the timer ID when the timer fires
     * @param ordered      if true then executeBlocking is called several times on the same context, the
     *                     executions for that context will be executed serially, not in parallel. if false
     *                     then they will be no ordering guarantees
     * @return the unique ID of the timer
     */
    public long setPeriodic(long initialDelay, long delay, Handler<Long> handler, boolean ordered)
    {
        return vertx.setPeriodic(initialDelay,
                                 delay,
                                 id -> workerExecutor.executeBlocking(() -> {
                                     handler.handle(id);
                                     return id;
                                 }, ordered));
    }

    /**
     * Like {@link #setTimer(long, Handler)} with the handler guaranteed to be executed in a
     * worker thread and ordered = false.
     *
     * @param delay   the delay in milliseconds, after which the timer will fire
     * @param handler the handler that will be called with the timer ID when the timer fires
     * @return the unique identifier for the timer
     */
    public long setTimer(long delay, Handler<Long> handler)
    {
        return setTimer(delay, handler, false);
    }

    /**
     * Like {@link Vertx#setTimer(long, Handler)} with the handler guaranteed to be executed in a worker thread.
     *
     * @param delay   the delay in milliseconds, after which the timer will fire
     * @param handler the handler that will be called with the timer ID when the timer fires
     * @param ordered if true and if executeBlocking is called several times on the same context, the executions
     *                for that context will be executed serially, not in parallel. The periodic scheduled
     *                executions could be delayed if the prior execution on the same context is taking longer.
     *                If false then they will be no ordering guarantees
     * @return the unique identifier for the timer
     */
    public long setTimer(long delay, Handler<Long> handler, boolean ordered)
    {
        return vertx.setTimer(delay, id -> workerExecutor.executeBlocking(() -> {
            handler.handle(id);
            return id;
        }, ordered));
    }

    /**
     * Delegate to {@link Vertx#cancelTimer(long)}
     *
     * @param id The id of the timer to cancel
     * @return {@code true} if the timer was successfully cancelled, {@code false} otherwise
     */
    public boolean cancelTimer(long id)
    {
        return vertx.cancelTimer(id);
    }

    @Override
    public <T> void executeBlocking(Handler<Promise<T>> blockingCodeHandler,
                                    boolean ordered,
                                    Handler<AsyncResult<T>> asyncResultHandler)
    {
        // No existing use cases; and do not expect new use cases of this deprecated API
        throw new UnsupportedOperationException("Operation is unsupported!");
    }

    @Override
    public <T> Future<T> executeBlocking(Handler<Promise<T>> blockingCodeHandler,
                                         boolean ordered)
    {
        // TODO: migrate to org.apache.cassandra.sidecar.concurrent.TaskExecutorPool.executeBlocking(java.util.concurrent.Callable<T>, boolean)
        return StopWatch.measureTimeTaken(workerExecutor.executeBlocking(blockingCodeHandler), this::recordTimeTaken);
    }

    @Override
    public <T> Future<T> executeBlocking(Callable<T> blockingCodeHandler, boolean ordered)
    {
        return StopWatch.measureTimeTaken(workerExecutor.executeBlocking(blockingCodeHandler, ordered), this::recordTimeTaken);
    }

    /**
     * Records time taken for tasks executed by {@link TaskExecutorPool}
     * @param durationNanos time taken by a task
     */
    protected abstract void recordTimeTaken(long durationNanos);

    @Override
    public void close(Handler<AsyncResult<Void>> handler)
    {
        throw new UnsupportedOperationException("Closing TaskExecutorPool is not supported!");
    }

    @Override
    public Future<Void> close()
    {
        throw new UnsupportedOperationException("Closing TaskExecutorPool is not supported!");
    }

    Future<Void> closeInternal()
    {
        return workerExecutor == null
               ? Future.succeededFuture()
               : workerExecutor.close();
    }

    /**
     * {@link ServiceTaskExecutorPool} is used for executing tasks that are short lived and not expected to block for
     * too long, therefore will free up resources more quickly
     */
    static class ServiceTaskExecutorPool extends TaskExecutorPool
    {
        private final SidecarMetrics metrics;

        ServiceTaskExecutorPool(Vertx vertx, WorkerPoolConfiguration config, SidecarMetrics metrics)
        {
            super(vertx, config);
            this.metrics = metrics;
        }

        @Override
        protected void recordTimeTaken(long durationNanos)
        {
            if (metrics == null)
            {
                return;
            }
            metrics.server().resource().serviceTaskTime.metric.update(durationNanos, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * {@link InternalTaskExecutorPool} is used for executing tasks that are expected to block for a long time, and
     * the nature of these tasks is that they are not expected to complete immediately and can be queued if all the
     * threads are busy
     */
    static class InternalTaskExecutorPool extends TaskExecutorPool
    {
        private final SidecarMetrics metrics;

        InternalTaskExecutorPool(Vertx vertx, WorkerPoolConfiguration config, SidecarMetrics metrics)
        {
            super(vertx, config);
            this.metrics = metrics;
        }

        @Override
        protected void recordTimeTaken(long durationNanos)
        {
            if (metrics == null)
            {
                return;
            }
            metrics.server().resource().internalTaskTime.metric.update(durationNanos, TimeUnit.NANOSECONDS);
        }
    }
}
