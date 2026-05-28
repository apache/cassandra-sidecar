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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.vertx.core.Handler;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.db.CdcConfigAccessor;
import org.apache.cassandra.sidecar.tasks.CdcConfigRefresherNotifierTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CDC_CONFIGURATION_CHANGED;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CDC_CONFIG_MAPPINGS_CHANGED;

/**
 * Implementation of the interface {@link CdcConfig}, an in-memory representation holding
 * CDC and Kafka configurations from "configs" table inside sidecar internal keyspace.
 */
public class CdcConfigImpl implements CdcConfig
{
    private static final int DEFAULT_MAX_WATERMARKER_SIZE = 400000;
    private static final String DEFAULT_JOB_ID = "test-job-id";
    private static final int DEFAULT_MAX_COMMITLOGS_PER_INSTANCE = 4;
    private static final int DEFAULT_MAX_RECORD_BYTE_SIZE = -1;
    public static final int DEFAULT_WATERMARK_WINDOW = 259200;
    private final Vertx vertx;
    private final CdcConfigAccessor cdcConfigAccessor;

    private volatile Map<String, String> kafkaConfigMappings = Map.of();
    private volatile Map<String, String> cdcConfigMappings = Map.of();

    @Inject
    public CdcConfigImpl(Vertx vertx,
                         CdcConfigAccessor cdcConfigAccessor)
    {
        this.vertx = vertx;
        this.cdcConfigAccessor = cdcConfigAccessor;

        vertx.eventBus().localConsumer(ON_CDC_CONFIG_MAPPINGS_CHANGED.address(), new ConfigMappingsChanged());
    }

    private class ConfigMappingsChanged implements Handler<Message<Object>>
    {
        public void handle(Message<Object> event)
        {
            CdcConfigRefresherNotifierTask.ConfigMappings configMappings = (CdcConfigRefresherNotifierTask.ConfigMappings) event.body();
            kafkaConfigMappings = configMappings.getKafkaConfigMappings();
            cdcConfigMappings = configMappings.getCdcConfigMappings();
            vertx.eventBus().publish(ON_CDC_CONFIGURATION_CHANGED.address(), "Cdc Configuration Changed");
        }
    }

    @Override
    public Map<String, Object> kafkaConfigs()
    {
        Map<String, Object> kafkaConfigs = new HashMap<>();
        kafkaConfigs.putAll(getAuthConfigs());
        kafkaConfigs.putAll(kafkaConfigMappings);
        return ImmutableMap.copyOf(kafkaConfigs);
    }

    @Override
    public Map<String, Object> cdcConfigs()
    {
        return ImmutableMap.copyOf(cdcConfigMappings);
    }

    @Override
    public boolean isConfigReady()
    {
        kafkaConfigs();
        cdcConfigs();
        return cdcConfigAccessor.isAvailable()
                && !kafkaConfigMappings.isEmpty()
                && !cdcConfigMappings.isEmpty();
    }

    @Override
    public String kafkaTopic()
    {
        return cdcConfigMappings.get(ConfigKeys.TOPIC.lowcaseName);
    }

    @NotNull
    public TopicFormatType topicFormat()
    {
        return TopicFormatType.valueOf(cdcConfigMappings.getOrDefault(ConfigKeys.TOPIC_FORMAT_TYPE.lowcaseName, TopicFormatType.STATIC.name()));
    }

    public boolean cdcEnabled()
    {
        return Boolean.parseBoolean(cdcConfigMappings.getOrDefault(ConfigKeys.CDC_ENABLED.lowcaseName, "true"));
    }

    @Override
    public String jobId()
    {
        return cdcConfigMappings.getOrDefault(ConfigKeys.JOBID.lowcaseName, DEFAULT_JOB_ID);
    }

    @Override
    public boolean logOnly()
    {
        return getBool(ConfigKeys.LOG_ONLY.lowcaseName, false);
    }

    @Override
    public boolean persistEnabled()
    {
        return getBool(ConfigKeys.PERSIST_STATE.lowcaseName, true);
    }

    @Override
    public boolean failOnRecordTooLargeError()
    {
        return getBool(ConfigKeys.FAIL_KAFKA_TOO_LARGE_ERRORS.lowcaseName, false);
    }

    @Override
    public boolean failOnKafkaError()
    {
        return getBool(ConfigKeys.FAIL_KAFKA_ERRORS.lowcaseName, true);
    }

    @Override
    public MillisecondBoundConfiguration persistDelay()
    {
        return new MillisecondBoundConfiguration(getInt(ConfigKeys.PERSIST_DELAY_MILLIS.lowcaseName, 1000), TimeUnit.MILLISECONDS);
    }

    @Override
    public String datacenter()
    {
        return cdcConfigMappings.get(ConfigKeys.DATACENTER.lowcaseName);
    }

    @Override
    public SecondBoundConfiguration watermarkWindow()
    {
        // this prop sets the maximum duration age accepted by CDC, any mutations with write timestamps older than
        // the watermark window will be dropped with log message "Exclude the update due to out of the allowed time window."
        return new SecondBoundConfiguration(getInt(ConfigKeys.WATERMARK_SECONDS.lowcaseName, DEFAULT_WATERMARK_WINDOW), TimeUnit.SECONDS);
    }

    @Override
    public int maxRecordSizeBytes()
    {
        return DEFAULT_MAX_RECORD_BYTE_SIZE;
    }

    @Override
    public @Nullable String compression()
    {
        return null;
    }

    @Override
    public MillisecondBoundConfiguration minDelayBetweenMicroBatches()
    {
        // this prop allows us to add a minimum delay between CDC micro batches
        // usually if we need to slow down CDC
        // e.g. if CDC is started with a large backlog of commit log segments and is working hard to process.
        // e.g. or if there is a large data dump or burst of writes that causes high CDC activity.
        final long millis = Long.parseLong(cdcConfigMappings.getOrDefault(ConfigKeys.MICRO_BATCH_DELAY_MILLIS.lowcaseName, "1000"));
        return new MillisecondBoundConfiguration(millis, TimeUnit.MILLISECONDS);
    }

    @Override
    public String env()
    {
        return cdcConfigMappings.getOrDefault(ConfigKeys.ENV.lowcaseName, "");
    }

    @Override
    public int maxCommitLogsPerInstance()
    {
        return getInt(ConfigKeys.MAX_COMMIT_LOGS.lowcaseName, DEFAULT_MAX_COMMITLOGS_PER_INSTANCE);
    }

    @Override
    public int maxWatermarkerSize()
    {
        return getInt(ConfigKeys.MAX_WATERMARKER_SIZE.lowcaseName, DEFAULT_MAX_WATERMARKER_SIZE);
    }

    protected boolean getBool(String key, boolean orDefault)
    {
        String bool = cdcConfigMappings.get(key);
        return bool != null ? Boolean.parseBoolean(bool) : orDefault;
    }

    protected int getInt(String key, int orDefault)
    {
        return getInt(key, () -> orDefault);
    }

    protected int getInt(String key, Supplier<Integer> orDefault)
    {
        String aInt = cdcConfigMappings.get(key);
        return aInt != null ? Integer.valueOf(aInt) : orDefault.get();
    }

    private Map<String, Object> getAuthConfigs()
    {
        return new HashMap<>();
    }

    enum ConfigKeys
    {
        DATACENTER,
        LOG_ONLY,
        PERSIST_STATE,
        ENV,
        TOPIC,
        TOPIC_FORMAT_TYPE,
        CDC_ENABLED,
        JOBID,
        WATERMARK_SECONDS,
        MICRO_BATCH_DELAY_MILLIS,
        MAX_COMMIT_LOGS,
        MAX_WATERMARKER_SIZE,
        FAIL_KAFKA_ERRORS,
        FAIL_KAFKA_TOO_LARGE_ERRORS,
        PERSIST_DELAY_MILLIS;
        private final String lowcaseName;

        ConfigKeys()
        {
            this.lowcaseName = this.name().toLowerCase();
        }
    }
}
