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
package org.apache.cassandra.sidecar.cdc;

import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * In memory representation of CDC and Kafka configurations from "configs" table in sidecar keyspace.
 */
public interface CdcConfig
{
    Logger LOGGER = LoggerFactory.getLogger(CdcConfig.class);

    String DEFAULT_JOB_ID = "test-job";
    int DEFAULT_MAX_WATERMARKER_SIZE = 400000;

    CdcConfig STUB = new CdcConfig()
    {
    };

    /**
     * Topic format
     */
    enum TopicFormatType
    {
        STATIC, KEYSPACE, KEYSPACETABLE, TABLE, MAP
    }

    default String env()
    {
        return "";
    }

    @Nullable
    default String kafkaTopic()
    {
        return null;
    }

    @NotNull
    default TopicFormatType topicFormat()
    {
        return TopicFormatType.STATIC;
    }

    default boolean cdcEnabled()
    {
        return true;
    }

    default String jobId()
    {
        return DEFAULT_JOB_ID;
    }

    default Map<String, Object> kafkaConfigs()
    {
        return Map.of();
    }

    default Map<String, Object> cdcConfigs()
    {
        return Map.of();
    }

    default boolean logOnly()
    {
        return false;
    }

    default String dc()
    {
        return "DATACENTER1";
    }

    default Duration watermarkWindow()
    {
        return Duration.ofHours(4);
    }

    /**
     * @return max Kafka record size in bytes. If value is non-negative then the KafkaPublisher will chunk larger records into multiple messages.
     */
    default int maxRecordSizeBytes()
    {
        return -1;
    }

    /**
     * @return "zstd" to enable compression on large blobs, or null or empty string if disabled.
     */
    @Nullable
    default String compression()
    {
        return null;
    }

    /**
     * @return true if Kafka publisher should fail if Kafka client returns "record too large" error
     */
    default boolean failOnRecordTooLargeError()
    {
        return false;
    }

    /**
     * @return true if Kafka publisher should fail if Kafka client returns any other error.
     */
    default boolean failOnKafkaError()
    {
        return true;
    }

    /**
     * Initialization of tables and loading config takes some time, returns if the config
     * is ready to be loaded or not.
     *
     * @return true if config is ready to be read.
     */
    default boolean isConfigReady()
    {
        return true;
    }

    default Duration minDelayBetweenMicroBatches()
    {
        return Duration.ofMillis(1000);
    }

    default int maxCommitLogsPerInstance()
    {
        return 4;
    }

    /**
     * @return the maximum number of entries to hold in the watermarker state for mutations that
     * are have not achieved the consistency level. Each entry is an MD5 with a byte integer,
     * approximately 30-60 bytes per entry before compression.
     */
    default int maxWatermarkerSize()
    {
        return DEFAULT_MAX_WATERMARKER_SIZE;
    }

    /**
     * @return true if CDC state should be persisted to Cassandra
     */
    default boolean persistEnabled()
    {
        return true;
    }

    /**
     * @return the delay in millis between persist calls.
     */
    default Duration persistDelay()
    {
        return Duration.ofMillis(1000);
    }
}

