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

package org.apache.cassandra.sidecar.db.schema;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.SidecarSchemaModificationException;
import org.apache.cassandra.sidecar.metrics.SchemaMetrics;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_READY;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_DRIVER_CLOSED;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SERVER_STOP;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_SCHEMA_INITIALIZED;

/**
 * Encapsulates all related operations for features provided by Sidecar
 */
public class SidecarSchema
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarSchema.class);
    protected static final long INITIALIZATION_LOOP_DELAY_MILLIS = 1000;

    private final Vertx vertx;
    private final ExecutorPools executorPools;
    private final SchemaKeyspaceConfiguration schemaKeyspaceConfiguration;
    private final SidecarInternalKeyspace sidecarInternalKeyspace;
    private final AtomicLong initializationTimerId = new AtomicLong(Long.MIN_VALUE);
    private final CQLSessionProvider cqlSessionProvider;
    private final SchemaMetrics metrics;

    private boolean isInitialized = false;

    public SidecarSchema(Vertx vertx,
                         ExecutorPools executorPools,
                         SidecarConfiguration config,
                         SidecarInternalKeyspace sidecarInternalKeyspace,
                         CQLSessionProvider cqlSessionProvider,
                         SchemaMetrics metrics)
    {
        this.vertx = vertx;
        this.executorPools = executorPools;
        this.schemaKeyspaceConfiguration = config.serviceConfiguration().schemaKeyspaceConfiguration();
        this.sidecarInternalKeyspace = sidecarInternalKeyspace;
        this.cqlSessionProvider = cqlSessionProvider;
        this.metrics = metrics;
        configureSidecarServerEventListenersMaybe();
    }

    private void configureSidecarServerEventListenersMaybe()
    {
        if (!this.schemaKeyspaceConfiguration.isEnabled())
        {
            LOGGER.info("Sidecar schema is disabled!");
            return;
        }

        EventBus eventBus = vertx.eventBus();
        eventBus.localConsumer(ON_CASSANDRA_CQL_READY.address(), message -> startSidecarSchemaInitializer());
        eventBus.localConsumer(ON_SERVER_STOP.address(), message -> reset());
        eventBus.localConsumer(ON_CASSANDRA_DRIVER_CLOSED.address(), message -> reset());
    }

    @VisibleForTesting
    public void startSidecarSchemaInitializer()
    {
        if (!schemaKeyspaceConfiguration.isEnabled() || sidecarInternalKeyspace == null)
            return;

        // schedule one initializer exactly
        if (!initializationTimerId.compareAndSet(Long.MIN_VALUE, -1))
        {
            LOGGER.debug("Skipping starting the sidecar schema initializer because there is an initialization " +
                         "in progress with timerId={}", initializationTimerId);
            return;
        }

        // loop initialization until initialized
        long timerId = executorPools.internal()
                                    .setPeriodic(INITIALIZATION_LOOP_DELAY_MILLIS, this::initialize);
        initializationTimerId.set(timerId);
    }

    public boolean isInitialized()
    {
        return schemaKeyspaceConfiguration.isEnabled() && isInitialized;
    }

    public void ensureInitialized()
    {
        if (!isInitialized())
        {
            throw new IllegalStateException("Sidecar schema is not initialized!");
        }
    }

    private synchronized void initialize(long timerId)
    {
        // it should not happen since the callback is only scheduled when isEnabled == true
        if (!schemaKeyspaceConfiguration.isEnabled())
        {
            return;
        }

        if (isInitialized())
        {
            LOGGER.debug("Sidecar schema is already initialized!");
            cancelTimer(timerId);
            return;
        }

        Session session = cqlSessionProvider.get();
        if (session == null)
        {
            LOGGER.debug("Cql session is not yet available. Skip initializing...");
            return;
        }

        try
        {
            isInitialized = sidecarInternalKeyspace.initialize(session);

            if (isInitialized())
            {
                LOGGER.info("Sidecar schema is initialized");
                cancelTimer(timerId);
                reportSidecarSchemaInitialized();
            }
        }
        catch (Exception ex)
        {
            LOGGER.warn("Failed to initialize schema", ex);
            if (ex instanceof SidecarSchemaModificationException)
            {
                LOGGER.warn("Failed to modify schema", ex);
                metrics.failedModifications.metric.update(1);
            }
            metrics.failedInitializations.metric.update(1);
        }
    }

    private synchronized void reset()
    {
        if (!schemaKeyspaceConfiguration.isEnabled() || !isInitialized)
        {
            return;
        }

        LOGGER.info("Resetting schema and nullify prepared statements");

        cancelTimer(initializationTimerId.get());
        initializationTimerId.set(Long.MIN_VALUE);
        sidecarInternalKeyspace.reset();
        isInitialized = false;
    }

    private synchronized void cancelTimer(long timerId)
    {
        // invalid timerId; nothing to cancel
        if (timerId < 0)
        {
            return;
        }

        if (initializationTimerId.compareAndSet(timerId, -1L))
        {
            executorPools.internal().cancelTimer(timerId);
        }
    }

    private void reportSidecarSchemaInitialized()
    {
        vertx.eventBus().publish(ON_SIDECAR_SCHEMA_INITIALIZED.address(), "SidecarSchema initialized");
    }
}
