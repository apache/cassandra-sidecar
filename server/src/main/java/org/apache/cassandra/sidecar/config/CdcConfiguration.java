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
 * This class encapsulates configuration values for cdc.
 */
public interface CdcConfiguration
{
    /**
     * @return segment hard link cache expiration time used in {@link org.apache.cassandra.sidecar.cdc.CdcLogCache}
     */
    SecondBoundConfiguration segmentHardLinkCacheExpiry();

    /**
     * @return true if cdc feature is enabled
     */
    boolean isEnabled();

    /**
     * @return how frequently CDC configs are to be refreshed
     */
    MillisecondBoundConfiguration cdcConfigRefreshTime();

    /* CdcRawDirectorySpaceCleaner Configuration */

    /**
     * @return the cadence at which the CdcRawDirectorySpaceCleaner period task should run to check and clean-up old `cdc_raw` log segments.
     */
    SecondBoundConfiguration cdcRawDirectorySpaceCleanerFrequency();

    /**
     * @return `true` if CdcRawDirectorySpaceCleaner should monitor the `cdc_raw` directory and clean up the oldest commit log segments.
     */
    boolean enableCdcRawDirectoryRoutineCleanUp();

    /**
     * @return fallback value for maximum directory size in bytes for the `cdc_raw` directory when can't be read from `system_views.settings` table.
     */
    long fallbackCdcRawDirectoryMaxSizeBytes();

    /**
     * @return max percent usage of the cdc_raw directory before CdcRawDirectorySpaceCleaner starts removing the oldest segments.
     */
    float cdcRawDirectoryMaxPercentUsage();

    /**
     * @return the critical time period in seconds that indicates the `cdc_raw` directory is not large enough to buffer this time-window of mutations.
     */
    SecondBoundConfiguration cdcRawDirectoryCriticalBufferWindow();

    /**
     * @return the low time period in seconds that indicates the `cdc_raw` directory is not large enough to buffer this time-window of mutations.
     */
    SecondBoundConfiguration cdcRawDirectoryLowBufferWindow();

    /**
     * @return the time period which the CdcRawDirectorySpaceCleaner should cache the cdc_total_space before refreshing.
     */
    SecondBoundConfiguration cacheMaxUsage();
}
