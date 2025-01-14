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

import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;

/**
 * Configuration needed for restore jobs restoring data through sidecar
 */
public interface RestoreJobConfiguration
{
    /**
     * @return the delay for job discovery active loop, e.g. there are active jobs discovered
     */
    MillisecondBoundConfiguration jobDiscoveryActiveLoopDelay();

    /**
     * @return the delay for job discovery idle loop, e.g. no active job discovered at the moment
     * jobDiscoveryIdleLoopDelay should be configured larger than jobDiscoveryActiveLoopDelay
     */
    MillisecondBoundConfiguration jobDiscoveryIdleLoopDelay();

    /**
     * @return the minimum number of days in the past to look up the restore jobs
     */
    int jobDiscoveryMinimumRecencyDays();

    /**
     * @return the maximum number of slices to be processed concurrently
     */
    int processMaxConcurrency();

    /**
     * @return time to live for restore job tables: restore_job and restore_slice
     */
    SecondBoundConfiguration restoreJobTablesTtl();

    /**
     * @return the amount of time above which a restore task is considered slow
     */
    SecondBoundConfiguration slowTaskThreshold();

    /**
     * @return the delay between each report of the same slow task
     */
    SecondBoundConfiguration slowTaskReportDelay();

    /**
     * @return the delay for {@link org.apache.cassandra.sidecar.restore.RingTopologyRefresher}
     */
    MillisecondBoundConfiguration ringTopologyRefreshDelay();
}
