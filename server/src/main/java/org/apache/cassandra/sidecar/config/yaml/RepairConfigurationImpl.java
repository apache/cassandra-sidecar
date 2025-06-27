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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.RepairConfiguration;

/**
 * Configuration for Repair jobs
 */
public class RepairConfigurationImpl implements RepairConfiguration
{
    public static final long DEFAULT_MAX_REPAIR_RUNTIME_MILLIS = 100_000L;
    public static final long DEFAULT_REPAIR_POLLING_INTERVAL_MILLIS = 2_000L;

    @JsonProperty(value = "max_repair_runtime", defaultValue = DEFAULT_MAX_REPAIR_RUNTIME_MILLIS + "")
    protected long maxRepairRuntimeMillis;

    @JsonProperty(value = "repair_polling_interval", defaultValue = DEFAULT_REPAIR_POLLING_INTERVAL_MILLIS + "")
    protected long repairPollIntervalMillis;

    public RepairConfigurationImpl()
    {
        this.maxRepairRuntimeMillis = DEFAULT_MAX_REPAIR_RUNTIME_MILLIS;
        this.repairPollIntervalMillis = DEFAULT_REPAIR_POLLING_INTERVAL_MILLIS;
    }

    public RepairConfigurationImpl(long maxRepairRuntimeMillis, long repairPollIntervalMillis)
    {
        this.maxRepairRuntimeMillis = maxRepairRuntimeMillis;
        this.repairPollIntervalMillis = repairPollIntervalMillis;
    }

    @Override
    public long maxRepairJobRuntimeMillis()
    {
        return maxRepairRuntimeMillis;
    }

    @Override
    public long repairPollIntervalMillis()
    {
        return repairPollIntervalMillis;
    }

    @JsonProperty(value = "repair_polling_interval")
    public void setRepairPollIntervalMillis(long repairPollIntervalMillis)
    {
        this.repairPollIntervalMillis = repairPollIntervalMillis;
    }

    @JsonProperty(value = "max_repair_runtime")
    public void setMaxRepairJobRuntime(long maxRepairRuntimeMillis)
    {
        this.maxRepairRuntimeMillis = maxRepairRuntimeMillis;
    }
}
