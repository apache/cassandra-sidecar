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
    public static final MillisecondBoundConfiguration DEFAULT_REPAIR_STATUS_POLLING_INTERVAL = MillisecondBoundConfiguration.parse("2s");

    @JsonProperty(value = "status_attempts")
    protected final int validRepairStatusAttempts;

    @JsonProperty(value = "status_polling_interval")
    protected final MillisecondBoundConfiguration repairStatusPollInterval;

    /**
     * Default constructor that sets default values
     */
    public RepairJobsConfigurationImpl()
    {
        this(DEFAULT_VALID_REPAIR_STATUS_ATTEMPTS, DEFAULT_REPAIR_STATUS_POLLING_INTERVAL);
    }

    /**
     * Constructor with parameters for JSON deserialization
     *
     * @param validRepairStatusAttempts the max retry attempts for the repair job status to be valid
     * @param repairStatusPollInterval  the polling interval for repair job status
     */
    @JsonCreator
    public RepairJobsConfigurationImpl(@JsonProperty(value = "status_attempts") int validRepairStatusAttempts,
                                       @JsonProperty(value = "status_polling_interval") MillisecondBoundConfiguration repairStatusPollInterval)
    {
        this.validRepairStatusAttempts = validRepairStatusAttempts;
        this.repairStatusPollInterval = repairStatusPollInterval;
    }

    @Override
    public int repairStatusMaxAttempts()
    {
        return validRepairStatusAttempts;
    }

    @Override
    public MillisecondBoundConfiguration repairPollInterval()
    {
        return repairStatusPollInterval;
    }
}
