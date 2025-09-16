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
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.CdcConfiguration;

/**
 * Encapsulate configuration values for CDC
 */
public class CdcConfigurationImpl implements CdcConfiguration
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcConfigurationImpl.class);
    public static final String IS_ENABLED_PROPERTY = "enabled";
    public static final String CONFIGURATION_REFRESH_TIME_PROPERTY = "config_refresh_time";
    public static final String TABLE_SCHEMA_REFRESH_TIME_PROPERTY = "table_schema_refresh_time";
    public static final String SEGMENT_HARD_LINK_CACHE_EXPIRY_PROPERTY = "segment_hardlink_cache_expiry";
    public static final boolean DEFAULT_IS_ENABLED = false;
    public static final MillisecondBoundConfiguration DEFAULT_CDC_CONFIG_REFRESH_TIME =
            MillisecondBoundConfiguration.parse("30s");
    public static final SecondBoundConfiguration DEFAULT_TABLE_SCHEMA_REFRESH_TIME =
            SecondBoundConfiguration.parse("60s");
    public static final SecondBoundConfiguration DEFAULT_SEGMENT_HARD_LINK_CACHE_EXPIRY =
            SecondBoundConfiguration.parse("5m");
    public static final String CDC_RAW_CLEANER_FREQUENCY_PROPERTY = "cdc_raw_cleaner_frequency";
    public static final SecondBoundConfiguration DEFAULT_CDC_RAW_CLEANER_FREQUENCY =
    SecondBoundConfiguration.parse("1m");

    public static final String ENABLE_CDC_RAW_CLEANER_PROPERTY = "enable_cdc_raw_cleaner";
    public static final boolean DEFAULT_ENABLE_CDC_RAW_CLEANER_PROPERTY = true;

    public static final String FALLBACK_CDC_RAW_MAX_DIRECTORY_SIZE_BYTES = "fallback_cdc_raw_max_directory_size_bytes";
    public static final long DEFAULT_FALLBACK_CDC_RAW_MAX_DIRECTORY_SIZE_BYTES = 1L << 31; // 2 GiB

    public static final String CDC_RAW_MAX_DIRECTORY_MAX_PERCENT = "cdc_raw_max_directory_max_percent";
    public static final float DEFAULT_CDC_RAW_MAX_DIRECTORY_MAX_PERCENT = 1.0f;

    public static final String CDC_RAW_MAX_CRITICAL_BUFFER_WINDOW = "cdc_raw_critical_buffer_window";
    public static final SecondBoundConfiguration DEFAULT_CDC_RAW_MAX_CRITICAL_BUFFER_WINDOW = SecondBoundConfiguration.parse("15m");

    public static final String CDC_RAW_MAX_LOW_BUFFER_WINDOW = "cdc_raw_low_buffer_window";
    public static final SecondBoundConfiguration DEFAULT_CDC_RAW_MAX_LOW_BUFFER_WINDOW = SecondBoundConfiguration.parse("60m");

    public static final String CDC_CACHE_MAX_USAGE_DURATION = "cdc_raw_cache_max_usage_duration";
    public static final SecondBoundConfiguration DEFAULT_CDC_CACHE_MAX_USAGE_DURATION = SecondBoundConfiguration.parse("15m");

    @JsonProperty(value = IS_ENABLED_PROPERTY)
    private final boolean isEnabled;
    @JsonProperty(value = CONFIGURATION_REFRESH_TIME_PROPERTY)
    private final MillisecondBoundConfiguration cdcConfigRefreshTime;
    @JsonProperty(value = TABLE_SCHEMA_REFRESH_TIME_PROPERTY)
    private final SecondBoundConfiguration tableSchemaRefreshTime;
    @JsonProperty(value = SEGMENT_HARD_LINK_CACHE_EXPIRY_PROPERTY)
    private SecondBoundConfiguration segmentHardLinkCacheExpiry;
    @JsonProperty(value = CDC_RAW_CLEANER_FREQUENCY_PROPERTY)
    private SecondBoundConfiguration cdcRawCleanerFrequency;
    @JsonProperty(value = ENABLE_CDC_RAW_CLEANER_PROPERTY)
    private boolean enableCdcRawCleaner;
    @JsonProperty(value = FALLBACK_CDC_RAW_MAX_DIRECTORY_SIZE_BYTES)
    private long fallbackCdcRawMaxDirectorySize;
    @JsonProperty(value = CDC_RAW_MAX_DIRECTORY_MAX_PERCENT)
    private float cdcRawMaxPercent;
    @JsonProperty(value = CDC_RAW_MAX_CRITICAL_BUFFER_WINDOW)
    private SecondBoundConfiguration cdcRawCriticalBufferWindow;
    @JsonProperty(value = CDC_RAW_MAX_LOW_BUFFER_WINDOW)
    private SecondBoundConfiguration cdcRawLowBufferWindow;
    @JsonProperty(value = CDC_CACHE_MAX_USAGE_DURATION)
    private SecondBoundConfiguration cacheMaxUsage;

    public CdcConfigurationImpl()
    {
        this.segmentHardLinkCacheExpiry = DEFAULT_SEGMENT_HARD_LINK_CACHE_EXPIRY;
        this.cdcConfigRefreshTime = DEFAULT_CDC_CONFIG_REFRESH_TIME;
        this.tableSchemaRefreshTime = DEFAULT_TABLE_SCHEMA_REFRESH_TIME;
        this.isEnabled = DEFAULT_IS_ENABLED;
        this.cdcRawCleanerFrequency = DEFAULT_CDC_RAW_CLEANER_FREQUENCY;
        this.enableCdcRawCleaner = DEFAULT_ENABLE_CDC_RAW_CLEANER_PROPERTY;
        this.fallbackCdcRawMaxDirectorySize = DEFAULT_FALLBACK_CDC_RAW_MAX_DIRECTORY_SIZE_BYTES;
        this.cdcRawMaxPercent = DEFAULT_CDC_RAW_MAX_DIRECTORY_MAX_PERCENT;
        this.cdcRawCriticalBufferWindow = DEFAULT_CDC_RAW_MAX_CRITICAL_BUFFER_WINDOW;
        this.cdcRawLowBufferWindow = DEFAULT_CDC_RAW_MAX_LOW_BUFFER_WINDOW;
        this.cacheMaxUsage = DEFAULT_CDC_CACHE_MAX_USAGE_DURATION;
    }

    public CdcConfigurationImpl(boolean isEnabled,
                                MillisecondBoundConfiguration cdcConfigRefreshTime,
                                SecondBoundConfiguration segmentHardLinkCacheExpiry)
    {
        this(
        isEnabled,
        cdcConfigRefreshTime,
        segmentHardLinkCacheExpiry,
        DEFAULT_CDC_RAW_CLEANER_FREQUENCY,
        DEFAULT_TABLE_SCHEMA_REFRESH_TIME,
        DEFAULT_ENABLE_CDC_RAW_CLEANER_PROPERTY,
        DEFAULT_FALLBACK_CDC_RAW_MAX_DIRECTORY_SIZE_BYTES,
        DEFAULT_CDC_RAW_MAX_DIRECTORY_MAX_PERCENT,
        DEFAULT_CDC_RAW_MAX_CRITICAL_BUFFER_WINDOW,
        DEFAULT_CDC_RAW_MAX_LOW_BUFFER_WINDOW,
        DEFAULT_CDC_CACHE_MAX_USAGE_DURATION
        );
    }

    public CdcConfigurationImpl(boolean isEnabled,
                                MillisecondBoundConfiguration cdcConfigRefreshTime,
                                SecondBoundConfiguration tableSchemaRefreshTime,
                                SecondBoundConfiguration segmentHardLinkCacheExpiry,
                                SecondBoundConfiguration cdcRawCleanerFrequency,
                                boolean enableCdcRawCleaner,
                                long cdcRawMaxDirectorySize,
                                float cdcRawMaxPercent,
                                SecondBoundConfiguration cdcRawCriticalBufferWindow,
                                SecondBoundConfiguration cdcRawLowBufferWindow,
                                SecondBoundConfiguration cacheMaxUsage)
    {
        this.isEnabled = isEnabled;
        this.cdcConfigRefreshTime = cdcConfigRefreshTime;
        this.tableSchemaRefreshTime = tableSchemaRefreshTime;
        this.segmentHardLinkCacheExpiry = segmentHardLinkCacheExpiry;
        this.cdcRawCleanerFrequency = cdcRawCleanerFrequency;
        this.enableCdcRawCleaner = enableCdcRawCleaner;
        this.fallbackCdcRawMaxDirectorySize = cdcRawMaxDirectorySize;
        this.cdcRawMaxPercent = cdcRawMaxPercent;
        this.cdcRawCriticalBufferWindow = cdcRawCriticalBufferWindow;
        this.cdcRawLowBufferWindow = cdcRawLowBufferWindow;
        this.cacheMaxUsage = cacheMaxUsage;
    }

    @Override
    @JsonProperty(value = IS_ENABLED_PROPERTY)
    public boolean isEnabled()
    {
        return isEnabled;
    }

    @Override
    @JsonProperty(value = CONFIGURATION_REFRESH_TIME_PROPERTY)
    public MillisecondBoundConfiguration cdcConfigRefreshTime()
    {
        return cdcConfigRefreshTime;
    }

    @Override
    @JsonProperty(value = TABLE_SCHEMA_REFRESH_TIME_PROPERTY)
    public SecondBoundConfiguration tableSchemaRefreshTime()
    {
        return tableSchemaRefreshTime;
    }

    @Override
    @JsonProperty(value = SEGMENT_HARD_LINK_CACHE_EXPIRY_PROPERTY)
    public SecondBoundConfiguration segmentHardLinkCacheExpiry()
    {
        return segmentHardLinkCacheExpiry;
    }

    /**
     * Legacy property {@code segment_hardlink_cache_expiry_in_secs}
     *
     * @param segmentHardlinkCacheExpiryInSecs expiry in seconds
     * @deprecated in favor of {@code segment_hardlink_cache_expiry}
     */
    @JsonProperty(value = "segment_hardlink_cache_expiry_in_secs")
    @Deprecated
    public void setSegmentHardLinkCacheExpiryInSecs(long segmentHardlinkCacheExpiryInSecs)
    {
        LOGGER.warn("'segment_hardlink_cache_expiry_in_secs' is deprecated, use 'segment_hardlink_cache_expiry' instead");
        this.segmentHardLinkCacheExpiry = new SecondBoundConfiguration(segmentHardlinkCacheExpiryInSecs, TimeUnit.SECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = CDC_RAW_CLEANER_FREQUENCY_PROPERTY)
    public SecondBoundConfiguration cdcRawDirectorySpaceCleanerFrequency()
    {
        return cdcRawCleanerFrequency;
    }

    @JsonProperty(value = CDC_RAW_CLEANER_FREQUENCY_PROPERTY)
    public void setCdcRawDirectorySpaceCleanerFrequency(SecondBoundConfiguration cdcRawCleanerFrequency)
    {
        this.cdcRawCleanerFrequency = cdcRawCleanerFrequency;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = ENABLE_CDC_RAW_CLEANER_PROPERTY)
    public boolean enableCdcRawDirectoryRoutineCleanUp()
    {
        return enableCdcRawCleaner;
    }

    @JsonProperty(value = ENABLE_CDC_RAW_CLEANER_PROPERTY)
    public void setEnableCdcRawCleaner(boolean enableCdcRawCleaner)
    {
        this.enableCdcRawCleaner = enableCdcRawCleaner;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = FALLBACK_CDC_RAW_MAX_DIRECTORY_SIZE_BYTES)
    public long fallbackCdcRawDirectoryMaxSizeBytes()
    {
        return fallbackCdcRawMaxDirectorySize;
    }

    @JsonProperty(value = FALLBACK_CDC_RAW_MAX_DIRECTORY_SIZE_BYTES)
    public void setFallbackCdcRawDirectoryMaxSizeBytes(long fallbackCdcRawMaxDirectorySize)
    {
        this.fallbackCdcRawMaxDirectorySize = fallbackCdcRawMaxDirectorySize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = CDC_RAW_MAX_DIRECTORY_MAX_PERCENT)
    public float cdcRawDirectoryMaxPercentUsage()
    {
        return cdcRawMaxPercent;
    }

    @JsonProperty(value = CDC_RAW_MAX_DIRECTORY_MAX_PERCENT)
    public void setCdcRawDirectoryMaxPercentUsage(long cdcRawMaxPercent)
    {
        this.cdcRawMaxPercent = cdcRawMaxPercent;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = CDC_RAW_MAX_CRITICAL_BUFFER_WINDOW)
    public SecondBoundConfiguration cdcRawDirectoryCriticalBufferWindow()
    {
        return cdcRawCriticalBufferWindow;
    }

    @JsonProperty(value = CDC_RAW_MAX_CRITICAL_BUFFER_WINDOW)
    public void setCdcRawDirectoryCriticalBufferWindow(SecondBoundConfiguration cdcRawCriticalBufferWindow)
    {
        this.cdcRawCriticalBufferWindow = cdcRawCriticalBufferWindow;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = CDC_RAW_MAX_LOW_BUFFER_WINDOW)
    public SecondBoundConfiguration cdcRawDirectoryLowBufferWindow()
    {
        return cdcRawLowBufferWindow;
    }

    @JsonProperty(value = CDC_RAW_MAX_LOW_BUFFER_WINDOW)
    public void setCdcRawDirectoryLowBufferWindow(SecondBoundConfiguration cdcRawLowBufferWindow)
    {
        this.cdcRawLowBufferWindow = cdcRawLowBufferWindow;
    }

    /**
     * {@inheritDoc}
     */
    @JsonProperty(value = CDC_CACHE_MAX_USAGE_DURATION)
    @Override
    public SecondBoundConfiguration cacheMaxUsage()
    {
        return cacheMaxUsage;
    }

    @JsonProperty(value = CDC_CACHE_MAX_USAGE_DURATION)
    public void setCacheMaxUsage(SecondBoundConfiguration cacheMaxUsage)
    {
        this.cacheMaxUsage = cacheMaxUsage;
    }
}
