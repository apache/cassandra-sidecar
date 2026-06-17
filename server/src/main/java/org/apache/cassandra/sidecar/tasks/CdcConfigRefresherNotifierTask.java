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

package org.apache.cassandra.sidecar.tasks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.codecs.CdcConfigMappingsCodec;
import org.apache.cassandra.sidecar.common.server.ThrowingRunnable;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.CdcConfigAccessor;
import org.apache.cassandra.sidecar.db.KafkaConfigAccessor;
import org.apache.cassandra.sidecar.utils.EventBusUtils;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CDC_CONFIG_MAPPINGS_CHANGED;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_SCHEMA_INITIALIZED;

/**
 * Periodic task notifying if the CDC config changed
 */
public class CdcConfigRefresherNotifierTask implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcConfigRefresherNotifierTask.class);
    private final SidecarConfiguration sidecarConfiguration;
    private final CdcConfigAccessor cdcConfigAccessor;
    private final KafkaConfigAccessor kafkaConfigAccessor;
    private final Vertx vertx;
    private final List<ThrowingRunnable> configChangeListeners = Collections.synchronizedList(new ArrayList<>());
    private final ConfigMappings configMappings = new ConfigMappings();

    @Inject
    public CdcConfigRefresherNotifierTask(Vertx vertx,
                                          SidecarConfiguration sidecarConfiguration,
                                          KafkaConfigAccessor kafkaConfigAccessor,
                                          CdcConfigAccessor cdcConfigAccessor)
    {
        this.sidecarConfiguration = sidecarConfiguration;
        this.kafkaConfigAccessor = kafkaConfigAccessor;
        this.cdcConfigAccessor = cdcConfigAccessor;
        this.vertx = vertx;

        vertx.eventBus().registerDefaultCodec(ConfigMappings.class, CdcConfigMappingsCodec.INSTANCE);
    }

    public void registerConfigChangeListener(ThrowingRunnable listener)
    {
        this.configChangeListeners.add(listener);
    }

    @Override
    public DurationSpec delay()
    {
        return sidecarConfiguration.serviceConfiguration().cdcConfiguration().cdcConfigRefreshTime();
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        vertx.eventBus().publish(ON_CDC_CONFIG_MAPPINGS_CHANGED.address(), configMappings);
        promise.tryComplete();
    }

    // skip if any of the following condition is true
    // - sidecar schema not enabled or cdc not enabled
    // - both configs have not changed
    @Override
    public ScheduleDecision scheduleDecision()
    {
        if (!sidecarConfiguration.serviceConfiguration().schemaKeyspaceConfiguration().isEnabled()
            || !sidecarConfiguration.serviceConfiguration().cdcConfiguration().isEnabled())
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
        if (!newKafkaConfigMappings.equals(configMappings.getKafkaConfigMappings()))
        {
            shouldSkip = false;
            configMappings.setKafkaConfigMappings(newKafkaConfigMappings);
        }
        if (!newCdcConfigMappings.equals(configMappings.getCdcConfigMappings()))
        {
            shouldSkip = false;
            configMappings.setCdcConfigMappings(newCdcConfigMappings);
        }
        return shouldSkip ? ScheduleDecision.SKIP : ScheduleDecision.EXECUTE;
    }

    @Override
    public void deploy(Vertx vertx, PeriodicTaskExecutor executor)
    {
        EventBusUtils.onceLocalConsumer(vertx.eventBus(), ON_SIDECAR_SCHEMA_INITIALIZED.address(), ignored -> executor.schedule(this));
    }

    /**
     * Wrapper class containing kafka and cdc configs
     */
    public static class ConfigMappings
    {
        private Map<String, String> kafkaConfigMappings = Map.of();
        private Map<String, String> cdcConfigMappings = Map.of();

        public Map<String, String> getKafkaConfigMappings()
        {
            return kafkaConfigMappings;
        }

        public void setKafkaConfigMappings(Map<String, String> kafkaConfigMappings)
        {
            this.kafkaConfigMappings = kafkaConfigMappings;
        }

        public Map<String, String> getCdcConfigMappings()
        {
            return cdcConfigMappings;
        }

        public void setCdcConfigMappings(Map<String, String> cdcConfigMappings)
        {
            this.cdcConfigMappings = cdcConfigMappings;
        }
    }
}
