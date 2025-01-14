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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.netty.handler.traffic.AbstractTrafficShapingHandler;
import io.netty.handler.traffic.GlobalTrafficShapingHandler;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.TrafficShapingConfiguration;

/**
 * Reads the configuration for the global traffic shaping options from a YAML file. These TCP server options enable
 * configuration of bandwidth limiting. Both inbound and outbound bandwidth can be limited through these options.
 */
public class TrafficShapingConfigurationImpl implements TrafficShapingConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TrafficShapingConfigurationImpl.class);

    /**
     * Default inbound bandwidth limit in bytes/sec = 0 (0 implies unthrottled)
     */
    public static final long DEFAULT_INBOUND_GLOBAL_BANDWIDTH_LIMIT = 0;

    /**
     * Default outbound bandwidth limit in bytes/sec = 0 (0 implies unthrottled)
     */
    public static final long DEFAULT_OUTBOUND_GLOBAL_BANDWIDTH_LIMIT = 0;

    /**
     * Default peak outbound bandwidth limit. Defaults to 400 megabytes/sec
     * See {@link GlobalTrafficShapingHandler#maxGlobalWriteSize}
     */
    public static final long DEFAULT_PEAK_OUTBOUND_GLOBAL_BANDWIDTH_LIMIT = 400L * 1024L * 1024L;

    /**
     * Default max delay in case of traffic shaping
     * (during which no communication will occur).
     * Shall be less than TIMEOUT. Here half of "standard" 30s.
     * See {@link AbstractTrafficShapingHandler#DEFAULT_MAX_TIME}
     */
    public static final MillisecondBoundConfiguration DEFAULT_MAX_DELAY_TIME = MillisecondBoundConfiguration.parse("15s");

    /**
     * Default delay between two checks: 1s (1000ms)
     * See {@link AbstractTrafficShapingHandler#DEFAULT_CHECK_INTERVAL}
     */
    public static final MillisecondBoundConfiguration DEFAULT_CHECK_INTERVAL = MillisecondBoundConfiguration.parse("1s");

    /**
     * Default inbound bandwidth limit in bytes/sec for ingress files = 0 (0 implies unthrottled)
     */
    public static final long DEFAULT_INBOUND_FILE_GLOBAL_BANDWIDTH_LIMIT = 0;

    @JsonProperty(value = "inbound_global_bandwidth_bps")
    protected final long inboundGlobalBandwidthBytesPerSecond;

    @JsonProperty(value = "outbound_global_bandwidth_bps")
    protected final long outboundGlobalBandwidthBytesPerSecond;

    @JsonProperty(value = "peak_outbound_global_bandwidth_bps")
    protected final long peakOutboundGlobalBandwidthBytesPerSecond;

    protected MillisecondBoundConfiguration maxDelayToWait;

    protected MillisecondBoundConfiguration checkIntervalForStats;

    @JsonProperty(value = "inbound_global_file_bandwidth_bps")
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
                                           MillisecondBoundConfiguration maxDelayToWait,
                                           MillisecondBoundConfiguration checkIntervalForStats,
                                           long inboundGlobalFileBandwidthBytesPerSecond)
    {
        this.inboundGlobalBandwidthBytesPerSecond = inboundGlobalBandwidthBytesPerSecond;
        this.outboundGlobalBandwidthBytesPerSecond = outboundGlobalBandwidthBytesPerSecond;
        this.peakOutboundGlobalBandwidthBytesPerSecond = peakOutboundGlobalBandwidthBytesPerSecond;
        this.maxDelayToWait = maxDelayToWait;
        this.checkIntervalForStats = checkIntervalForStats;
        this.inboundGlobalFileBandwidthBytesPerSecond = inboundGlobalFileBandwidthBytesPerSecond;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "inbound_global_bandwidth_bps")
    public long inboundGlobalBandwidthBytesPerSecond()
    {
        return inboundGlobalBandwidthBytesPerSecond;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "outbound_global_bandwidth_bps")
    public long outboundGlobalBandwidthBytesPerSecond()
    {
        return outboundGlobalBandwidthBytesPerSecond;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "peak_outbound_global_bandwidth_bps")
    public long peakOutboundGlobalBandwidthBytesPerSecond()
    {
        return peakOutboundGlobalBandwidthBytesPerSecond;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "max_delay_to_wait")
    public MillisecondBoundConfiguration maxDelayToWait()
    {
        return maxDelayToWait;
    }

    @JsonProperty(value = "max_delay_to_wait")
    public void setMaxDelayToWait(MillisecondBoundConfiguration maxDelayToWait)
    {
        this.maxDelayToWait = maxDelayToWait;
    }

    /**
     * Legacy property {@code max_delay_to_wait_millis}
     *
     * @param maxDelayToWaitMillis max delay to wait in milliseconds
     * @deprecated in favor of {@code max_delay_to_wait}
     */
    @JsonProperty(value = "max_delay_to_wait_millis")
    @Deprecated
    public void setMaxDelayToWaitMillis(long maxDelayToWaitMillis)
    {
        LOGGER.warn("'max_delay_to_wait_millis' is deprecated, use 'max_delay_to_wait' instead");
        setMaxDelayToWait(new MillisecondBoundConfiguration(maxDelayToWaitMillis, TimeUnit.MILLISECONDS));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = "check_interval_for_stats")
    public MillisecondBoundConfiguration checkIntervalForStats()
    {
        return checkIntervalForStats;
    }

    @JsonProperty(value = "check_interval_for_stats")
    public void setCheckIntervalForStats(MillisecondBoundConfiguration checkIntervalForStats)
    {
        this.checkIntervalForStats = checkIntervalForStats;
    }

    /**
     * Legacy property {@code check_interval_for_stats_millis}
     *
     * @param checkIntervalForStatsMillis check interval for stats in milliseconds
     * @deprecated in favor of {@code check_interval_for_stats}
     */
    @JsonProperty(value = "check_interval_for_stats_millis")
    @Deprecated
    public void setCheckIntervalForStatsMillis(long checkIntervalForStatsMillis)
    {
        LOGGER.warn("'check_interval_for_stats_millis' is deprecated, use 'check_interval_for_stats' instead");
        setCheckIntervalForStats(new MillisecondBoundConfiguration(checkIntervalForStatsMillis, TimeUnit.MILLISECONDS));
    }

    @Override
    @JsonProperty(value = "inbound_global_file_bandwidth_bps")
    public long inboundGlobalFileBandwidthBytesPerSecond()
    {
        return inboundGlobalFileBandwidthBytesPerSecond;
    }
}
