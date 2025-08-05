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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.RepairJobsConfiguration;

/**
 * Configuration for Repair jobs
 */
public class RepairJobsConfigurationImpl implements RepairJobsConfiguration
{
    // 1 day in milliseconds
    public static final long DEFAULT_MAX_REPAIR_RUNTIME_MILLIS = 24 * 60 * 60 * 1000L;
    public static final long DEFAULT_REPAIR_POLLING_INTERVAL_MILLIS = 2_000L;

    @JsonProperty(value = "max_repair_runtime", defaultValue = DEFAULT_MAX_REPAIR_RUNTIME_MILLIS + "")
    protected final long maxRepairRuntimeMillis;

    @JsonProperty(value = "repair_polling_interval", defaultValue = DEFAULT_REPAIR_POLLING_INTERVAL_MILLIS + "")
    protected final long repairPollIntervalMillis;

    /**
     * Default constructor that sets default values
     */
    public RepairJobsConfigurationImpl()
    {
        this(DEFAULT_MAX_REPAIR_RUNTIME_MILLIS, DEFAULT_REPAIR_POLLING_INTERVAL_MILLIS);
    }

    /**
     * Constructor with parameters for JSON deserialization
     *
     * @param maxRepairRuntimeMillis the maximum runtime for repair jobs in milliseconds
     * @param repairPollIntervalMillis the polling interval for repair jobs in milliseconds
     */
    @JsonCreator
    public RepairJobsConfigurationImpl(
        @JsonProperty(value = "max_repair_runtime", defaultValue = DEFAULT_MAX_REPAIR_RUNTIME_MILLIS + "") long maxRepairRuntimeMillis,
        @JsonProperty(value = "repair_polling_interval", defaultValue = DEFAULT_REPAIR_POLLING_INTERVAL_MILLIS + "") long repairPollIntervalMillis)
    {
        this.maxRepairRuntimeMillis = maxRepairRuntimeMillis;
        this.repairPollIntervalMillis = repairPollIntervalMillis;
    }

    @Override
    public MillisecondBoundConfiguration maxRepairJobRuntime()
    {
        return new MillisecondBoundConfiguration(maxRepairRuntimeMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public MillisecondBoundConfiguration repairPollInterval()
    {
        return new MillisecondBoundConfiguration(repairPollIntervalMillis, TimeUnit.MILLISECONDS);
    }

    
}
