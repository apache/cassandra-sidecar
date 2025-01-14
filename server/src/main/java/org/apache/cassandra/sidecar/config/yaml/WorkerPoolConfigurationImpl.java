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

package org.apache.cassandra.sidecar.config.yaml;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;

/**
 * Encapsulates configurations for the worker pool
 */
public class WorkerPoolConfigurationImpl implements WorkerPoolConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerPoolConfigurationImpl.class);

    @JsonProperty("name")
    protected final String workerPoolName;

    @JsonProperty("size")
    protected final int workerPoolSize;

    // WorkerExecutor logs a warning if the blocking execution exceeds the max time configured.
    // It does not abort the execution. The warning messages look like this.
    // "Thread xxx has been blocked for yyy ms, time limit is zzz ms"
    protected MillisecondBoundConfiguration workerMaxExecutionTime;

    public WorkerPoolConfigurationImpl()
    {
        this(null, 20, MillisecondBoundConfiguration.parse("60s"));
    }

    public WorkerPoolConfigurationImpl(String workerPoolName,
                                       int workerPoolSize,
                                       MillisecondBoundConfiguration workerMaxExecutionTime)
    {
        this.workerPoolName = workerPoolName;
        this.workerPoolSize = workerPoolSize;
        this.workerMaxExecutionTime = workerMaxExecutionTime;
    }

    /**
     * @return the name of the worker pool
     */
    @Override
    @JsonProperty("name")
    public String workerPoolName()
    {
        return workerPoolName;
    }

    /**
     * @return the size of the worker pool
     */
    @Override
    @JsonProperty("size")
    public int workerPoolSize()
    {
        return workerPoolSize;
    }

    /**
     * @return the maximum execution time for the worker pool
     */
    @Override
    @JsonProperty("max_execution_time")
    public MillisecondBoundConfiguration workerMaxExecutionTime()
    {
        return workerMaxExecutionTime;
    }

    @JsonProperty("max_execution_time")
    public void setWorkerMaxExecutionTime(MillisecondBoundConfiguration workerMaxExecutionTime)
    {
        this.workerMaxExecutionTime = workerMaxExecutionTime;
    }

    /**
     * Legacy property {@code max_execution_time_millis}
     *
     * @param workerMaxExecutionTimeMillis max execution time in milliseconds
     * @deprecated in favor of {@code max_execution_time}
     */
    @JsonProperty("max_execution_time_millis")
    @Deprecated
    public void setWorkerMaxExecutionTimeMillis(long workerMaxExecutionTimeMillis)
    {
        LOGGER.warn("'max_execution_time_millis' is deprecated, use 'max_execution_time' instead");
        setWorkerMaxExecutionTime(new MillisecondBoundConfiguration(workerMaxExecutionTimeMillis, TimeUnit.MILLISECONDS));
    }
}
