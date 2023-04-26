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

import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;

/**
 * Manages dedicated worker pools to schedule and execute _blocking_ tasks to avoid blocking netty eventloop.
 * It is a facade to handle one-off and periodic blocking execution.
 */
@Singleton
public class ExecutorPools
{
    // a pool for tasks that are not expected to block for too long, therefore will free up more quickly
    private final TaskExecutorPool taskExecutors;
    // a pool for tasks that are expected to block for a long time, and the nature of these tasks is that
    // they are not expected to complete immediately and can be queued if all the threads are busy
    private final TaskExecutorPool internalTaskExecutors;

    @Inject
    public ExecutorPools(Vertx vertx, Configuration configuraion)
    {
        this.taskExecutors = new TaskExecutorPool(vertx, configuraion.serverWorkerPoolConfiguration());
        this.internalTaskExecutors = new TaskExecutorPool(vertx, configuraion.serverInternalWorkerPoolConfiguration());
    }

    /**
     * @return the executor pool to run blocking code for client facing http requests.
     * The blocking code should not block the thread for too long.
     */
    public TaskExecutorPool service()
    {
        return taskExecutors;
    }

    /**
     * @return the executor pool to run blocking code for internal.
     * Unlike the pool {@link #service()}, the blocking code can block the thread for a longer duration.
     * Tasks submitted to this pool can be queued if all threads are busy.
     */
    public TaskExecutorPool internal()
    {
        return internalTaskExecutors;
    }

    public Future<Void> close()
    {
        return taskExecutors.closeInternal().onComplete(v -> internalTaskExecutors.closeInternal());
    }

    /**
     * A pool of executors backed by {@link WorkerExecutor} and {@link Vertx}
     */
    public static class TaskExecutorPool implements WorkerExecutor
    {
        private final Vertx vertx;
        private final WorkerExecutor workerExecutor;

        private TaskExecutorPool(Vertx vertx, WorkerPoolConfiguration config)
        {
            this.vertx = vertx;
            this.workerExecutor = vertx.createSharedWorkerExecutor(config.workerPoolName,
                                                                   config.workerPoolSize,
                                                                   config.workerMaxExecutionTimeMillis,
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
            return vertx.setPeriodic(delay,
                                     id -> workerExecutor.executeBlocking(promise -> {
                                         handler.handle(id);
                                         promise.complete();
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
            return vertx.setTimer(delay, id -> workerExecutor.executeBlocking(promise -> handler.handle(id), false));
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
            return vertx.setTimer(delay, id -> workerExecutor.executeBlocking(promise -> handler.handle(id), ordered));
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
            workerExecutor.executeBlocking(blockingCodeHandler, ordered, asyncResultHandler);
        }

        @Override
        public <T> Future<T> executeBlocking(Handler<Promise<T>> blockingCodeHandler,
                                             boolean ordered)
        {
            return workerExecutor.executeBlocking(blockingCodeHandler, ordered);
        }

        @Override
        public <T> void executeBlocking(Handler<Promise<T>> blockingCodeHandler,
                                        Handler<AsyncResult<T>> asyncResultHandler)
        {
            workerExecutor.executeBlocking(blockingCodeHandler, asyncResultHandler);
        }

        @Override
        public <T> Future<T> executeBlocking(Handler<Promise<T>> blockingCodeHandler)
        {
            return workerExecutor.executeBlocking(blockingCodeHandler);
        }

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

        private Future<Void> closeInternal()
        {
            return workerExecutor == null
                   ? Future.succeededFuture()
                   : workerExecutor.close();
        }
    }
}

