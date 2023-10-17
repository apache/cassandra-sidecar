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
import org.apache.cassandra.sidecar.config.TrafficShapingConfiguration;

/**
 * Reads the configuration for the global traffic shaping options from a YAML file. These TCP server options enable
 * configuration of bandwidth limiting. Both inbound and outbound bandwidth can be limited through these options.
 */
public class TrafficShapingConfigurationImpl implements TrafficShapingConfiguration
{
    @JsonProperty(value = "inbound_global_bandwidth_bps", defaultValue = "0")
    protected final long inboundGlobalBandwidthBytesPerSecond;

    @JsonProperty(value = "outbound_global_bandwidth_bps", defaultValue = "0")
    protected final long outboundGlobalBandwidthBytesPerSecond;

    @JsonProperty(value = "peak_outbound_global_bandwidth_bps",
    defaultValue = DEFAULT_PEAK_OUTBOUND_GLOBAL_BANDWIDTH_LIMIT + "")
    protected final long peakOutboundGlobalBandwidthBytesPerSecond;

    @JsonProperty(value = "max_delay_to_wait_millis", defaultValue = DEFAULT_MAX_DELAY_TIME + "")
    protected final long maxDelayToWaitMillis;

    @JsonProperty(value = "check_interval_for_stats_millis", defaultValue = DEFAULT_CHECK_INTERVAL + "")
    protected final long checkIntervalForStatsMillis;

    @JsonProperty(value = "inbound_global_file_bandwidth_bps", defaultValue = "0")
    protected final long inboundGlobalFileBandwidthBytesPerSecond;

    public TrafficShapingConfigurationImpl()
    {
        this(DEFAULT_INBOUND_GLOBAL_BANDWIDTH_LIMIT,
             DEFAULT_OUTBOUND_GLOBAL_BANDWIDTH_LIMIT,
             DEFAULT_PEAK_OUTBOUND_GLOBAL_BANDWIDTH_LIMIT,
             DEFAULT_MAX_DELAY_TIME,
             DEFAULT_CHECK_INTERVAL,
             DEFAULT_INBOUND_FILE_GLOBAL_BANDWIDTH_LIMIT
        );
    }

    public TrafficShapingConfigurationImpl(long inboundGlobalBandwidthBytesPerSecond,
                                           long outboundGlobalBandwidthBytesPerSecond,
                                           long peakOutboundGlobalBandwidthBytesPerSecond,
                                           long maxDelayToWaitMillis,
                                           long checkIntervalForStatsMillis,
                                           long inboundGlobalFileBandwidthBytesPerSecond)
    {
        this.inboundGlobalBandwidthBytesPerSecond = inboundGlobalBandwidthBytesPerSecond;
        this.outboundGlobalBandwidthBytesPerSecond = outboundGlobalBandwidthBytesPerSecond;
        this.peakOutboundGlobalBandwidthBytesPerSecond = peakOutboundGlobalBandwidthBytesPerSecond;
        this.maxDelayToWaitMillis = maxDelayToWaitMillis;
        this.checkIntervalForStatsMillis = checkIntervalForStatsMillis;
        this.inboundGlobalFileBandwidthBytesPerSecond = inboundGlobalFileBandwidthBytesPerSecond;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "inbound_global_bandwidth_bps", defaultValue = "0")
    public long inboundGlobalBandwidthBytesPerSecond()
    {
        return inboundGlobalBandwidthBytesPerSecond;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "outbound_global_bandwidth_bps", defaultValue = "0")
    public long outboundGlobalBandwidthBytesPerSecond()
    {
        return outboundGlobalBandwidthBytesPerSecond;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "peak_outbound_global_bandwidth_bps",
    defaultValue = DEFAULT_PEAK_OUTBOUND_GLOBAL_BANDWIDTH_LIMIT + "")
    public long peakOutboundGlobalBandwidthBytesPerSecond()
    {
        return peakOutboundGlobalBandwidthBytesPerSecond;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "max_delay_to_wait_millis", defaultValue = DEFAULT_MAX_DELAY_TIME + "")
    public long maxDelayToWaitMillis()
    {
        return maxDelayToWaitMillis;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "check_interval_for_stats_millis", defaultValue = DEFAULT_CHECK_INTERVAL + "")
    public long checkIntervalForStatsMillis()
    {
        return checkIntervalForStatsMillis;
    }

    @Override
    @JsonProperty(value = "inbound_global_file_bandwidth_bps", defaultValue = "0")
    public long inboundGlobalFileBandwidthBytesPerSecond()
    {
        return inboundGlobalFileBandwidthBytesPerSecond;
    }
}
