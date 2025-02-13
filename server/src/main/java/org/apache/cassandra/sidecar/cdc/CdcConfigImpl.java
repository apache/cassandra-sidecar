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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Promise;

import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.config.CdcConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.db.CdcConfigAccessor;
import org.apache.cassandra.sidecar.db.KafkaConfigAccessor;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of the interface {@link CdcConfig}, an in-memory representation holding
 * CDC and Kafka configurations from "configs" table inside sidecar internal keyspace.
 */
@Singleton
public class CdcConfigImpl implements CdcConfig
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcConfigImpl.class);
    private static final String CDC_CONFIG_DC_KEY = "dc";
    private static final String CDC_CONFIG_LOG_ONLY_KEY = "log_only";
    private static final String CDC_CONFIG_PERSIST_STATE_KEY = "persist_state";
    private static final String CDC_CONFIG_ENV_KEY = "env";
    private static final String KAFKA_CONFIG_TOPIC_KEY = "topic";
    private static final String KAFKA_FORMAT_TYPE_CONFIG_TOPIC_KEY = "topic_format_type";
    private static final String CDC_ENABLED_KEY = "cdc_enabled";
    private static final String KAFKA_CONFIG_JOB_ID_KEY = "jobId";
    private static final String WATERMARK_WINDOW_KEY = "watermark_seconds";
    private static final String MICROBATCH_DELAY_KEY = "microbatch_delay_millis";
    private static final String CDC_CONFIG_MAX_COMMIT_LOGS_KEY = "max_commit_logs";
    private static final String CDC_MAX_WATERMARKER_SIZE_KEY = "max_watermarker_size";
    private static final String CDC_FAIL_KAFKA_ERRORS = "fail_kafka_errors";
    private static final String CDC_FAIL_KAFKA_TOO_LARGE_ERRORS = "fail_kafka_too_large_errors";
    private static final String CDC_PERSIST_DELAY_MILLIS = "persist_delay_millis";

    private final SchemaKeyspaceConfiguration schemaKeyspaceConfiguration;
    private final CdcConfiguration cdcConfiguration;
    private final CdcConfigAccessor cdcConfigAccessor;
    private final KafkaConfigAccessor kafkaConfigAccessor;
    private final List<Callable<?>> configChangeListeners = Collections.synchronizedList(new ArrayList<>());
    private final ConfigRefreshNotifier configRefreshNotifier;
    private volatile Map<String, String> kafkaConfigMappings = Map.of();
    private volatile Map<String, String> cdcConfigMappings = Map.of();

    @Inject
    public CdcConfigImpl(CdcConfiguration cdcConfiguration,
                         SchemaKeyspaceConfiguration schemaKeyspaceConfiguration,
                         CdcConfigAccessor cdcConfigAccessor,
                         KafkaConfigAccessor kafkaConfigAccessor,
                         PeriodicTaskExecutor periodicTaskExecutor)
    {
        this.schemaKeyspaceConfiguration = schemaKeyspaceConfiguration;
        this.cdcConfiguration = cdcConfiguration;
        this.cdcConfigAccessor = cdcConfigAccessor;
        this.kafkaConfigAccessor = kafkaConfigAccessor;

        if (this.schemaKeyspaceConfiguration.isEnabled())
        {
            this.configRefreshNotifier = new ConfigRefreshNotifier();
            periodicTaskExecutor.schedule(configRefreshNotifier);
        }
        else
        {
            this.configRefreshNotifier = null;
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
        return cdcConfigAccessor.isSchemaInitialized()
                && !kafkaConfigMappings.isEmpty()
                && !cdcConfigMappings.isEmpty();
    }

    @Override
    public String kafkaTopic()
    {
        return cdcConfigMappings.getOrDefault(KAFKA_CONFIG_TOPIC_KEY, null);
    }

    @NotNull
    public TopicFormatType topicFormat()
    {
        return TopicFormatType.valueOf(cdcConfigMappings.getOrDefault(KAFKA_FORMAT_TYPE_CONFIG_TOPIC_KEY, TopicFormatType.STATIC.name()));
    }

    public boolean cdcEnabled()
    {
        return Boolean.parseBoolean(cdcConfigMappings.getOrDefault(CDC_ENABLED_KEY, "true"));
    }

    @Override
    public String jobId()
    {
        return cdcConfigMappings.getOrDefault(KAFKA_CONFIG_JOB_ID_KEY, DEFAULT_JOB_ID);
    }

    @Override
    public boolean logOnly()
    {
        return getBool(CDC_CONFIG_LOG_ONLY_KEY, false);
    }

    @Override
    public boolean persistEnabled()
    {
        return getBool(CDC_CONFIG_PERSIST_STATE_KEY, true);
    }

    @Override
    public boolean failOnRecordTooLargeError()
    {
        return getBool(CDC_FAIL_KAFKA_TOO_LARGE_ERRORS, false);
    }

    @Override
    public boolean failOnKafkaError()
    {
        return getBool(CDC_FAIL_KAFKA_ERRORS, true);
    }

    @Override
    public Duration persistDelay()
    {
        return Duration.ofMillis(getInt(CDC_PERSIST_DELAY_MILLIS, 1000));
    }

    @Override
    public String dc()
    {
        return cdcConfigMappings.get(CDC_CONFIG_DC_KEY);
    }

    @Override
    public Duration watermarkWindow()
    {
        // this prop sets the maximum duration age accepted by CDC, any mutations with write timestamps older than
        // the watermark window will be dropped with log message "Exclude the update due to out of the allowed time window."
        final int seconds = getInt(WATERMARK_WINDOW_KEY, 259200);
        return Duration.ofSeconds(seconds);
    }

    @Override
    public Duration minDelayBetweenMicroBatches()
    {
        // this prop allows us to add a minimum delay between CDC micro batches
        // usually if we need to slow down CDC
        // e.g. if CDC is started with a large backlog of commit log segments and is working hard to process.
        // e.g. or if there is a large data dump or burst of writes that causes high CDC activity.
        final long millis = Long.parseLong(cdcConfigMappings.getOrDefault(MICROBATCH_DELAY_KEY, "1000"));
        return Duration.ofMillis(millis);
    }

    @Override
    public String env()
    {
        return cdcConfigMappings.getOrDefault(CDC_CONFIG_ENV_KEY, "");
    }

    @Override
    public int maxCommitLogsPerInstance()
    {
        return getInt(CDC_CONFIG_MAX_COMMIT_LOGS_KEY, CdcConfig.super::maxCommitLogsPerInstance);
    }

    @Override
    public int maxWatermarkerSize()
    {
        return getInt(CDC_MAX_WATERMARKER_SIZE_KEY, CdcConfig.super::maxWatermarkerSize);
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

    /**
     * Adds a listener to service config changes
     *
     * @param listener The listener to call
     */
    public void registerConfigChangeListener(Callable<?> listener)
    {
        this.configChangeListeners.add(listener);
    }

    private Map<String, Object> getAuthConfigs()
    {
        Map<String, Object> authConfigs = new HashMap<>();
        authConfigs.put("pie.queue.kaffe.client.private.key.location", cdcConfiguration.kafkaClientPrivateKeyPath());
        return authConfigs;
    }

    @VisibleForTesting
    void forceExecuteNotifier()
    {
        if (configRefreshNotifier != null &&
                configRefreshNotifier.scheduleDecision() == ScheduleDecision.EXECUTE)
        {
            configRefreshNotifier.execute(Promise.promise());
        }
    }

    @VisibleForTesting
    ConfigRefreshNotifier configRefreshNotifier()
    {
        return configRefreshNotifier;
    }

    class ConfigRefreshNotifier implements PeriodicTask
    {
        @Override
        public DurationSpec delay()
        {
            return cdcConfiguration.cdcConfigRefreshTime();
        }

        @Override
        public void execute(Promise<Void> promise)
        {
            for (Callable<?> listener : configChangeListeners)
            {
                try
                {
                    listener.call();
                }
                catch (Throwable e)
                {
                    LOGGER.error(String.format("There was an error with callback %s", listener), e);
                }
            }
            promise.tryComplete();
        }

        // skip if any of the following condition is true
        // - sidecar schema not enabled or cdc not enabled
        // - both configs have not changed
        @Override
        public ScheduleDecision scheduleDecision()
        {
            if (!schemaKeyspaceConfiguration.isEnabled() || !cdcConfiguration.isEnabled())
            {
                LOGGER.trace("Skipping config refreshing");
                return ScheduleDecision.SKIP;
            }

            Map<String, String> newKafkaConfigMappings;
            Map<String, String> newCdcConfigMappings;
            try
            {
                newKafkaConfigMappings = kafkaConfigAccessor.getConfig().getConfigs();
                newCdcConfigMappings = cdcConfigAccessor.getConfig().getConfigs();
            }
            catch (Throwable e)
            {
                LOGGER.error("Failed to access cdc/kafka configs", e);
                return ScheduleDecision.SKIP;
            }

            boolean shouldSkip = true;
            if (!newKafkaConfigMappings.equals(kafkaConfigMappings))
            {
                shouldSkip = false;
                kafkaConfigMappings = newKafkaConfigMappings;
            }
            if (!newCdcConfigMappings.equals(cdcConfigMappings))
            {
                shouldSkip = false;
                cdcConfigMappings = newCdcConfigMappings;
            }
            return shouldSkip ? ScheduleDecision.SKIP : ScheduleDecision.EXECUTE;
        }
    }
}
