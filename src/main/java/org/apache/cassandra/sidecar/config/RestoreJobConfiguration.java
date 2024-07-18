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

/**
 * Configuration needed for restore jobs restoring data through sidecar
 */
public interface RestoreJobConfiguration
{
    /**
     * @return the delay in milliseconds for job discovery active loop, e.g. there are active jobs discovered
     */
    long jobDiscoveryActiveLoopDelayMillis();

    /**
     * @return the delay in milliseconds for job discovery idle loop, e.g. no active job discovered at the moment
     * jobDiscoveryIdleLoopDelayMillis should be configured larger than jobDiscoveryActiveLoopDelayMillis
     */
    long jobDiscoveryIdleLoopDelayMillis();

    /**
     * @return the number of days in the past to look up the restore jobs
     */
    int jobDiscoveryRecencyDays();

    /**
     * @return the maximum number of slices to be processed concurrently
     */
    int processMaxConcurrency();

    /**
     * @return time to live for restore job tables: restore_job and restore_slice
     */
    long restoreJobTablesTtlSeconds();

    /**
     * @return the number of seconds above which a restore task is considered slow
     */
    long slowTaskThresholdSeconds();

    /**
     * @return the delay in seconds between each report of the same slow task
     */
    long slowTaskReportDelaySeconds();

    /**
     * @return the delay in milliseconds for {@link org.apache.cassandra.sidecar.restore.RingTopologyRefresher}
     */
    long ringTopologyRefreshDelayMillis();
}
