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

import java.util.Map;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * In memory representation of CDC and Kafka configurations from "configs" table in sidecar keyspace.
 */
public interface CdcConfig
{
    /**
     * @return returns environment of the cassandra cluster. This config could be optional
     */
    String env();

    /**
     * @return returns kafka topic for the mutations to be published
     */
    @Nullable
    String kafkaTopic();

    /**
     * @return returns topic formats
     */
    @NotNull
    TopicFormatType topicFormat();

    /**
     * @return returns if CDC is enabled or not
     */
    boolean cdcEnabled();

    /**
     * @return returns unique global identifier for CDC job, CDC state is associated with job-id
     */
    String jobId();

    /**
     * @return returns configurations of the kafka for the mutations to be published.
     */
    Map<String, Object> kafkaConfigs();

    /**
     * @return returns CDC configurations as a map
     */
    Map<String, Object> cdcConfigs();

    /**
     * @return if logOnly config is set, mutations will not be published to kafka, instead they
     * would be logged, this would be useful for debugging and running sidecar application locally
     */
    boolean logOnly();

    /**
     * @return returns the data center, this config could be optional
     */
    String datacenter();

    /**
     * @return watermark window
     */
    SecondBoundConfiguration watermarkWindow();

    /**
     * @return max Kafka record size in bytes. If value is non-negative then the KafkaPublisher will chunk larger records into multiple messages.
     */
    int maxRecordSizeBytes();

    /**
     * @return "zstd" to enable compression on large blobs, or null or empty string if disabled.
     */
    @Nullable
    String compression();

    /**
     * @return true if Kafka publisher should fail if Kafka client returns "record too large" error
     */
    boolean failOnRecordTooLargeError();

    /**
     * @return true if Kafka publisher should fail if Kafka client returns any other error.
     */
    boolean failOnKafkaError();

    /**
     * Initialization of tables and loading config takes some time, returns if the config
     * is ready to be loaded or not.
     *
     * @return true if config is ready to be read.
     */
    boolean isConfigReady();

    /**
     * @return Delay between the micro batches in milli seconds
     */
    MillisecondBoundConfiguration minDelayBetweenMicroBatches();

    /**
     *
     * @return maxmum commit logs per instance
     */
    int maxCommitLogsPerInstance();

    /**
     * @return the maximum number of entries to hold in the watermarker state for mutations that
     * are have not achieved the consistency level. Each entry is an MD5 with a byte integer,
     * approximately 30-60 bytes per entry before compression.
     */
    int maxWatermarkerSize();

    /**
     * @return true if CDC state should be persisted to Cassandra
     */
    boolean persistEnabled();

    /**
     * @return the delay in millis between persist calls.
     */
    MillisecondBoundConfiguration persistDelay();

    /**
     * Topic format
     */
    enum TopicFormatType
    {
        STATIC,
        KEYSPACE,
        KEYSPACETABLE,
        TABLE,
        MAP
    }
}

