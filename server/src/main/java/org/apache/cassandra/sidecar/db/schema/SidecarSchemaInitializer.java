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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.coordination.ExecuteOnClusterLeaseholderOnly;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.exceptions.SidecarSchemaModificationException;
import org.apache.cassandra.sidecar.metrics.server.SchemaMetrics;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.apache.cassandra.sidecar.utils.EventBusUtils;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_READY;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_SCHEMA_INITIALIZED;

/**
 * Initializer that retries until sidecar schema is initialized. Once initialized, it un-schedules itself.
 */
public class SidecarSchemaInitializer implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarSchemaInitializer.class);
    private static final DurationSpec INITIALIZATION_LOOP_DELAY = MillisecondBoundConfiguration.parse("1s");

    private final CQLSessionProvider cqlSessionProvider;
    private final SidecarInternalKeyspace sidecarInternalKeyspace;
    private final SchemaMetrics schemaMetrics;
    private final ClusterLease clusterLease;
    private final SidecarSchema sidecarSchema;
    private final boolean isSidecarSchemaEnabled;
    private Vertx vertx;
    private PeriodicTaskExecutor periodicTaskExecutor;

    public SidecarSchemaInitializer(SidecarConfiguration sidecarConfiguration,
                                    CQLSessionProvider cqlSessionProvider,
                                    SidecarInternalKeyspace sidecarInternalKeyspace,
                                    SchemaMetrics schemaMetrics,
                                    SidecarSchema sidecarSchema,
                                    ClusterLease clusterLease)
    {
        this.isSidecarSchemaEnabled = sidecarConfiguration.serviceConfiguration().schemaKeyspaceConfiguration().isEnabled();
        this.cqlSessionProvider = cqlSessionProvider;
        this.sidecarInternalKeyspace = sidecarInternalKeyspace;
        this.schemaMetrics = schemaMetrics;
        this.sidecarSchema = sidecarSchema;
        this.clusterLease = clusterLease;
    }

    @Override
    public void deploy(Vertx vertx, PeriodicTaskExecutor executor)
    {
        if (!isSidecarSchemaEnabled)
        {
            LOGGER.info("SidecarSchema is disabled. Skip deployment of SidecarSchemaInitializer");
            return;
        }
        this.vertx = vertx;
        this.periodicTaskExecutor = executor;
        // deploy when CQL connection is established
        EventBusUtils.onceLocalConsumer(vertx.eventBus(), ON_CASSANDRA_CQL_READY.address(), ignored -> executor.schedule(this));
    }

    @Override
    public ScheduleDecision scheduleDecision()
    {
        if (cqlSessionProvider.getIfConnected() == null)
        {
            LOGGER.debug("CQL connection is not yet established. Skip this run of initialization.");
            return ScheduleDecision.SKIP;
        }
        return ScheduleDecision.EXECUTE;
    }

    @Override
    public DurationSpec delay()
    {
        return INITIALIZATION_LOOP_DELAY;
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        try
        {
            Session session = cqlSessionProvider.get();
            boolean isInitialized = sidecarInternalKeyspace.initialize(session, this::shouldCreateSchema);

            if (isInitialized)
            {
                LOGGER.info("Sidecar schema is initialized. Stopping SchemaSidecarInitializer");
                periodicTaskExecutor.unschedule(this);
                reportSidecarSchemaInitialized();
            }
        }
        catch (Exception ex)
        {
            LOGGER.warn("Failed to initialize schema. Retry in {}", delay(), ex);
            if (ex instanceof CassandraUnavailableException) // not quite expected here according to the schedule decision, but still check for it
            {
                return; // do not count Cassandra unavailable as failure
            }
            else if (ex instanceof SidecarSchemaModificationException)
            {
                LOGGER.warn("Failed to modify schema", ex);
                schemaMetrics.failedModifications.metric.update(1);
            }
            schemaMetrics.failedInitializations.metric.update(1);
        }
        promise.tryComplete();
    }

    protected void reportSidecarSchemaInitialized()
    {
        sidecarSchema.markInitialized();
        vertx.eventBus().publish(ON_SIDECAR_SCHEMA_INITIALIZED.address(), "SidecarSchema initialized");
    }

    /**
     * Returns {@code true} when the schema should be created by this Sidecar instance. For schemas
     * of type {@link ExecuteOnClusterLeaseholderOnly}, the schema creation is conditioned to whether
     * the local Sidecar instance has claimed the cluster-wide lease. For all other types of schemas,
     * the schemas will be created.
     *
     * @param schema the schema to test
     * @return {@code true} if the schema should be created by this Sidecar instance, {@code false} otherwise
     */
    protected boolean shouldCreateSchema(AbstractSchema schema)
    {
        if (schema instanceof ExecuteOnClusterLeaseholderOnly)
        {
            return clusterLease.isClaimedByLocalSidecar();
        }
        return true;
    }
}
