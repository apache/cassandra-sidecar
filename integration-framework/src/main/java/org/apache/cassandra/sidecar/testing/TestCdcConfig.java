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
package org.apache.cassandra.sidecar.testing;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.sidecar.cdc.CdcConfig;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;

/**
 * Test implementation of {@link CdcConfig} for integration tests.
 * Provides hardcoded configuration values suitable for testing CDC functionality
 * without requiring external Kafka infrastructure.
 */
public class TestCdcConfig implements CdcConfig
{
    @Override
    public String env()
    {
        return "test";
    }

    @Override
    public String kafkaTopic()
    {
        return "test-topic";
    }

    @Override
    public TopicFormatType topicFormat()
    {
        return TopicFormatType.KEYSPACETABLE;
    }

    @Override
    public boolean cdcEnabled()
    {
        return true;
    }

    @Override
    public String jobId()
    {
        return "test-job-id";
    }

    @Override
    public Map<String, Object> kafkaConfigs()
    {
        return new HashMap<>();
    }

    @Override
    public Map<String, Object> cdcConfigs()
    {
        return new HashMap<>();
    }

    @Override
    public boolean logOnly()
    {
        return true;
    }

    @Override
    public String datacenter()
    {
        return "datacenter1";
    }

    @Override
    public SecondBoundConfiguration watermarkWindow()
    {
        return new SecondBoundConfiguration(3, TimeUnit.DAYS);
    }

    @Override
    public int maxRecordSizeBytes()
    {
        return -1;
    }

    @Override
    public String compression()
    {
        return null;
    }

    @Override
    public MillisecondBoundConfiguration minDelayBetweenMicroBatches()
    {
        return new MillisecondBoundConfiguration(1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public int maxCommitLogsPerInstance()
    {
        return 4;
    }

    @Override
    public int maxWatermarkerSize()
    {
        return 400000;
    }

    @Override
    public boolean persistEnabled()
    {
        return true;
    }

    @Override
    public boolean failOnRecordTooLargeError()
    {
        return false;
    }

    @Override
    public boolean failOnKafkaError()
    {
        return true;
    }

    @Override
    public MillisecondBoundConfiguration persistDelay()
    {
        return new MillisecondBoundConfiguration(1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isConfigReady()
    {
        return true;
    }
}
