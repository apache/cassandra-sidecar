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
    public static final int DEFAULT_VALID_REPAIR_STATUS_ATTEMPTS = 5;
    public static final long DEFAULT_REPAIR_STATUS_POLLING_INTERVAL_MILLIS = 2_000L;

    @JsonProperty(value = "repair_status_attempts", defaultValue = DEFAULT_VALID_REPAIR_STATUS_ATTEMPTS + "")
    protected final int validRepairStatusAttempts;

    @JsonProperty(value = "repair_status_polling_interval", defaultValue = DEFAULT_REPAIR_STATUS_POLLING_INTERVAL_MILLIS + "")
    protected final long repairStatusPollIntervalMillis;

    /**
     * Default constructor that sets default values
     */
    public RepairJobsConfigurationImpl()
    {
        this(DEFAULT_VALID_REPAIR_STATUS_ATTEMPTS, DEFAULT_REPAIR_STATUS_POLLING_INTERVAL_MILLIS);
    }

    /**
     * Constructor with parameters for JSON deserialization
     *
     * @param validRepairStatusAttempts the max retry attempts for the repair job status to be valid
     * @param repairStatusPollIntervalMillis the polling interval for repair job status in milliseconds
     */
    @JsonCreator
    public RepairJobsConfigurationImpl(
        @JsonProperty(value = "repair_status_attempts", defaultValue = DEFAULT_VALID_REPAIR_STATUS_ATTEMPTS + "")
        int validRepairStatusAttempts,
        @JsonProperty(value = "repair_status_polling_interval", defaultValue = DEFAULT_REPAIR_STATUS_POLLING_INTERVAL_MILLIS + "")
        long repairStatusPollIntervalMillis)
    {
        this.validRepairStatusAttempts = validRepairStatusAttempts;
        this.repairStatusPollIntervalMillis = repairStatusPollIntervalMillis;
    }

    @Override
    public int repairStatusMaxAttempts()
    {
        return validRepairStatusAttempts;
    }

    @Override
    public MillisecondBoundConfiguration repairPollInterval()
    {
        return new MillisecondBoundConfiguration(repairStatusPollIntervalMillis, TimeUnit.MILLISECONDS);
    }

    
}
