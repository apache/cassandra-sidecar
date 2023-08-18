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

package org.apache.cassandra.sidecar.config;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;

/**
 * Encapsulates configurations for the worker pool
 */
public class WorkerPoolConfiguration
{
    @JsonProperty("name")
    private final String workerPoolName;

    @JsonProperty("size")
    private final int workerPoolSize;

    // WorkerExecutor logs a warning if the blocking execution exceeds the max time configured.
    // It does not abort the execution. The warning messages look like this.
    // "Thread xxx has been blocked for yyy ms, time limit is zzz ms"
    @JsonProperty("max_execution_time_millis")
    private final long workerMaxExecutionTimeMillis;

    public WorkerPoolConfiguration()
    {
        this.workerPoolName = null;
        this.workerPoolSize = 20;
        this.workerMaxExecutionTimeMillis = TimeUnit.SECONDS.toMillis(60);
    }

    protected WorkerPoolConfiguration(Builder builder)
    {
        workerPoolName = builder.workerPoolName;
        workerPoolSize = builder.workerPoolSize;
        workerMaxExecutionTimeMillis = builder.workerMaxExecutionTimeMillis;
    }

    /**
     * @return the name of the worker pool
     */
    @JsonProperty("name")
    public String workerPoolName()
    {
        return workerPoolName;
    }

    /**
     * @return the size of the worker pool
     */
    @JsonProperty("size")
    public int workerPoolSize()
    {
        return workerPoolSize;
    }

    /**
     * @return the maximum execution time for the worker pool in milliseconds
     */
    @JsonProperty("max_execution_time_millis")
    public long workerMaxExecutionTimeMillis()
    {
        return workerMaxExecutionTimeMillis;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code WorkerPoolConfiguration} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, WorkerPoolConfiguration>
    {
        private String workerPoolName;
        private int workerPoolSize;
        private long workerMaxExecutionTimeMillis;

        protected Builder()
        {
        }

        /**
         * Sets the {@code workerPoolName} and returns a reference to this Builder enabling method chaining.
         *
         * @param workerPoolName the {@code workerPoolName} to set
         * @return a reference to this Builder
         */
        public Builder workerPoolName(String workerPoolName)
        {
            this.workerPoolName = workerPoolName;
            return this;
        }

        /**
         * Sets the {@code workerPoolSize} and returns a reference to this Builder enabling method chaining.
         *
         * @param workerPoolSize the {@code workerPoolSize} to set
         * @return a reference to this Builder
         */
        public Builder workerPoolSize(int workerPoolSize)
        {
            this.workerPoolSize = workerPoolSize;
            return this;
        }

        /**
         * Sets the {@code workerMaxExecutionTimeMillis} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param workerMaxExecutionTimeMillis the {@code workerMaxExecutionTimeMillis} to set
         * @return a reference to this Builder
         */
        public Builder workerMaxExecutionTimeMillis(long workerMaxExecutionTimeMillis)
        {
            this.workerMaxExecutionTimeMillis = workerMaxExecutionTimeMillis;
            return this;
        }

        /**
         * Returns a {@code WorkerPoolConfiguration} built from the parameters previously set.
         *
         * @return a {@code WorkerPoolConfiguration} built with parameters of this
         * {@code WorkerPoolConfiguration.Builder}
         */
        @Override
        public WorkerPoolConfiguration build()
        {
            return new WorkerPoolConfiguration(this);
        }
    }
}
