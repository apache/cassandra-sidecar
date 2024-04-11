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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.metrics.ResourceMetrics;

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

    public ExecutorPools(Vertx vertx, ServiceConfiguration configuration)
    {
        this(vertx, configuration, null);
    }

    @Inject
    public ExecutorPools(Vertx vertx, ServiceConfiguration configuration, ResourceMetrics metrics)
    {
        this.taskExecutors
        = new TaskExecutorPool.ServiceTaskExecutorPool(vertx, configuration.serverWorkerPoolConfiguration(), metrics);
        this.internalTaskExecutors
        = new TaskExecutorPool.InternalTaskExecutorPool(vertx, configuration.serverInternalWorkerPoolConfiguration(), metrics);
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
}

