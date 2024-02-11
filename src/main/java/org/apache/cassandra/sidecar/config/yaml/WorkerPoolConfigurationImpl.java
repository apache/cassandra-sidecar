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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;

/**
 * Encapsulates configurations for the worker pool
 */
@Binds(to = WorkerPoolConfiguration.class)
public class WorkerPoolConfigurationImpl implements WorkerPoolConfiguration
{
    @JsonProperty("name")
    protected final String workerPoolName;

    @JsonProperty("size")
    protected final int workerPoolSize;

    // WorkerExecutor logs a warning if the blocking execution exceeds the max time configured.
    // It does not abort the execution. The warning messages look like this.
    // "Thread xxx has been blocked for yyy ms, time limit is zzz ms"
    @JsonProperty("max_execution_time_millis")
    protected final long workerMaxExecutionTimeMillis;

    public WorkerPoolConfigurationImpl()
    {
        this(null, 20, TimeUnit.SECONDS.toMillis(60));
    }

    public WorkerPoolConfigurationImpl(String workerPoolName,
                                       int workerPoolSize,
                                       long workerMaxExecutionTimeMillis)
    {
        this.workerPoolName = workerPoolName;
        this.workerPoolSize = workerPoolSize;
        this.workerMaxExecutionTimeMillis = workerMaxExecutionTimeMillis;
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
     * @return the maximum execution time for the worker pool in milliseconds
     */
    @Override
    @JsonProperty("max_execution_time_millis")
    public long workerMaxExecutionTimeMillis()
    {
        return workerMaxExecutionTimeMillis;
    }
}
