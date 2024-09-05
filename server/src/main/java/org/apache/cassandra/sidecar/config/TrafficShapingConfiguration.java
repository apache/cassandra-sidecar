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
 * Configuration for the global traffic shaping options. These TCP server options enable configuration of
 * bandwidth limiting. Both inbound and outbound bandwidth can be limited through these options.
 */
public interface TrafficShapingConfiguration
{
    /**
     * @return the bandwidth limit in bytes per second for inbound connections
     */
    long inboundGlobalBandwidthBytesPerSecond();

    /**
     * @return the bandwidth limit in bytes per second for outbound connections
     */
    long outboundGlobalBandwidthBytesPerSecond();

    /**
     * @return the maximum global write size in bytes per second allowed in the buffer globally for all channels
     * before write suspended is set
     */
    long peakOutboundGlobalBandwidthBytesPerSecond();

    /**
     * @return the maximum delay to wait in case of traffic excess in milliseconds
     */
    long maxDelayToWaitMillis();

    /**
     * @return the delay in milliseconds between two computations of performances for channels or {@code 0} if no
     * stats are to be computed
     */
    long checkIntervalForStatsMillis();

    /**
     * @return the bandwidth limit in bytes per second for incoming files (i.e. SSTable components upload), this
     * setting is upper-bounded by the {@link #inboundGlobalBandwidthBytesPerSecond()} configuration if throttled
     */
    long inboundGlobalFileBandwidthBytesPerSecond();
}
