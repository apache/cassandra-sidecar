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

import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;
import org.apache.cassandra.sidecar.common.utils.OperationalJobResult.OperationalJobStatus;
import org.apache.cassandra.sidecar.tasks.Task;

/**
 * An abstract class representing a Operational job managed by the sidecar.
 *
 */
public abstract class OperationalJob implements Task
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OperationalJob.class);

    protected UUID jobId;
    protected Future<OperationalJobStatus> status;
    protected long creationTime;

    private final Vertx vertx;

    protected OperationalJob(Vertx vertx)
    {
        this.vertx = vertx;
        this.creationTime = System.nanoTime();
        Promise<OperationalJobStatus> promise = Promise.promise();
        this.status = promise.future();

    }
    /**
     * Constructs a job with a unique UUID, in Pending state
     * @param jobId UUID representing the Job to be created
     */
    protected OperationalJob(Vertx vertx, UUID jobId)
    {
        this.vertx = vertx;
        this.jobId = jobId;
        Promise<OperationalJobStatus> promise = Promise.promise();
        this.status = promise.future();
        this.creationTime = System.nanoTime();
    }

    @VisibleForTesting
    protected OperationalJob(Vertx vertx, UUID jobId, OperationalJobStatus status)
    {
        this.vertx = vertx;
        this.jobId = jobId;
        this.status = Future.succeededFuture(status);
        this.creationTime = System.nanoTime();
    }

    public void setStatus(Future<OperationalJobStatus> status)
    {
        this.status = status;
    }
    public Future<OperationalJobStatus> status()
    {
        return status;
    }

    public UUID jobId()
    {
        return jobId;
    }

    public long creationTime()
    {
        return creationTime;
    }

    /**
     * Supplier specifying the functionality of the job to be triggered when the job is executed. Subclasses to
     * provide operation-specific implementations
     * @return a function with the operation implementation that returns a {@code JobResult}
     * @throws OperationalJobException
     */
    protected abstract OperationalJobStatus executeInternal() throws OperationalJobException;

    /**
     * Provide a meaningful name of the operation executed by the concrete subclass.
     * @return the name of the operation. eg. nodetool command name
     */
    public abstract String operation();

    /**
     * Specifies the operation to be performed to check if the job is running on the Cassandra node/cluster.
     * This functionality is provided by the Job when it is asynchronously triggering the job via the {@code JobManager}
     * For synchronous jobs this should always return false.
     * @return true if the job is running downstream
     */
    public abstract boolean isRunningDownstream();

    /**
     * Execute the job behavior as specified in the internal execution {@link #executeInternal()},
     * while tracking the status of the job's lifecycle.
     *
     * @return
     */
    public final void execute(Promise promise)
    {
        OperationalJobStatus status;
        try
        {
            LOGGER.info("Executing job with ID: {}", jobId);
            // Blocking call to perform concrete job-specific execution, returning the status
            status = executeInternal();
            LOGGER.debug("Job with ID: {} returned with status: {}", jobId, status);
            promise.complete(status);
        }
        catch (Exception e)
        {
            String reason = (e.getCause() != null) ? e.getCause().getMessage() : e.getMessage();
            LOGGER.error("Failed to execute job {} with reason: {}", jobId, reason);
            promise.fail(e);
        }
    }
}
