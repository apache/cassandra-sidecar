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

package org.apache.cassandra.sidecar.coordination;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.CASWriteUnknownException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.db.SidecarLeaseDatabaseAccessor;
import org.apache.cassandra.sidecar.metrics.CoordinationMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.tasks.ExecutionDetermination;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.jetbrains.annotations.VisibleForTesting;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_GLOBAL_LEASE_CLAIMED;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_GLOBAL_LEASE_LOST;

/**
 * A best-effort process where 0 or more instance executors can be chosen from the electorate.
 * The electorate is expected to be a small subset of the entirety of Sidecar instances.
 *
 * <p>There will be situations where multiple members of the electorate may
 * be acting as executors, for example in cases where we have:
 * <ul>
 *     <li>Network partitions
 *     <li>Binary protocol is disabled for a member of the electorate
 * </ul>
 *
 * <p>The chosen instance executor(s) must keep in mind that there might be other executors in the
 * cluster, so operations that they perform must be safe to be performed by one or more
 * Sidecar instances.
 */
public class BestEffortSingleConditionalExecutor implements ConditionalExecutor, PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BestEffortSingleConditionalExecutor.class);
    private final ElectorateMembership electorateMembership;
    private final SidecarLeaseDatabaseAccessor accessor;
    private final CoordinationMetrics metrics;
    private final Parameters parameters;
    private final ServiceConfiguration config;
    private final Vertx vertx;
    private final TaskExecutorPool internalPool;
    private volatile Instant leaseTime;
    private volatile ExecutionDetermination determination = ExecutionDetermination.INDETERMINATE;

    public BestEffortSingleConditionalExecutor(Vertx vertx,
                                               ExecutorPools executorPools,
                                               ServiceConfiguration serviceConfiguration,
                                               ElectorateMembership electorateMembership,
                                               SidecarLeaseDatabaseAccessor accessor,
                                               SidecarMetrics metrics)
    {
        this.vertx = vertx;
        this.internalPool = executorPools.internal();
        this.parameters = Parameters.from(serviceConfiguration.coordinationConfiguration().conditionalExecutorParameters());
        this.config = serviceConfiguration;
        this.electorateMembership = electorateMembership;
        this.accessor = accessor;
        this.metrics = metrics.server().coordination();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExecutionDetermination executionDetermination()
    {
        return determination;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean shouldSkip()
    {
        if (!config.schemaKeyspaceConfiguration().isEnabled())
        {
            // The Sidecar schema feature is required for this implementation
            // so skip when the feature is not enabled
            return true;
        }
        return !parameters.isEnabled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long initialDelay()
    {
        return parameters.initialDelayMillis();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long delay()
    {
        return parameters.delayMillis();
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        // Complete early so we can be scheduled for the next iteration
        promise.complete();

        // runs the election process based on the electorate membership
        internalPool.runBlocking(() -> determineSingleConditionalExecutor(electorateMembership));
    }

    protected void determineSingleConditionalExecutor(ElectorateMembership electorateMembership)
    {
        boolean shouldParticipate = electorateMembership.isMember();
        LOGGER.debug("Sidecar instance shouldParticipate={} in the selection", shouldParticipate);
        if (!shouldParticipate)
        {
            determination = ExecutionDetermination.SKIP_EXECUTION;
            return;
        }

        boolean wasCurrentExecutor = determination.shouldExecuteOnLocalInstance();
        Boolean isCurrentExecutor = null;

        String sidecarHostId = sidecarHostId();
        LOGGER.debug("Starting selection for sidecarHostId={}", sidecarHostId);
        if (wasCurrentExecutor)
        {
            isCurrentExecutor = executeLeaseAction("extend", sidecarHostId, accessor::extendLease, isCurrentExecutor);
        }

        if (isCurrentExecutor == null || !isCurrentExecutor)
        {
            isCurrentExecutor = executeLeaseAction("claim", sidecarHostId, accessor::claimLease, isCurrentExecutor);
        }

        maybeNotifyResults(isCurrentExecutor, sidecarHostId, wasCurrentExecutor);
        updateMetrics();
    }

    Boolean executeLeaseAction(String actionName,
                               String sidecarHostId,
                               Function<String, SidecarLeaseDatabaseAccessor.LeaseClaimResult> actionFn,
                               Boolean isCurrentExecutor)
    {
        SidecarLeaseDatabaseAccessor.LeaseClaimResult result = null;
        try
        {
            LOGGER.debug("Attempting to {} lease for sidecarHostId={}", actionName, sidecarHostId);
            result = actionFn.apply(sidecarHostId);
        }
        catch (CASWriteUnknownException | NoHostAvailableException e)
        {
            LOGGER.debug("Unable to {} lease for sidecarHostId={}", actionName, sidecarHostId, e);
        }
        catch (Exception e)
        {
            LOGGER.error("Unable to {} lease for sidecarHostId={}", actionName, sidecarHostId, e);
        }

        isCurrentExecutor = determineIfIsCurrentLeaseHolder(isCurrentExecutor, result, sidecarHostId);
        LOGGER.debug("{} lease for sidecarHostId={} result={}", actionName, sidecarHostId, isCurrentExecutor);
        return isCurrentExecutor;
    }

    void maybeNotifyResults(Boolean isCurrentExecutor, String sidecarHostId, boolean wasCurrentExecutor)
    {
        if (isCurrentExecutor == null)
        {
            LOGGER.debug("Unable to perform lease election for sidecarHostId={}", sidecarHostId);

            if (!wasCurrentExecutor || leaseExpired())
            {
                if (leaseExpired())
                {
                    LOGGER.info("Giving up lease for sidecarHostId={} leaseAcquired={} leaseExpired={}",
                                sidecarHostId, leaseTime, leaseExpirationTime());
                }

                determination = ExecutionDetermination.INDETERMINATE;
                leaseTime = null;
            }
        }
        else
        {
            if (isCurrentExecutor)
            {
                leaseTime = Instant.now();
                determination = ExecutionDetermination.EXECUTE;
            }
            else
            {
                leaseTime = null;
                determination = ExecutionDetermination.SKIP_EXECUTION;
            }

            if (wasCurrentExecutor && !isCurrentExecutor)
            {
                LOGGER.info("Cluster-wide lease has been lost by sidecarHostId={}", sidecarHostId);
                // notify lease has been lost
                vertx.eventBus().publish(ON_SIDECAR_GLOBAL_LEASE_LOST.address(), sidecarHostId);
            }

            if (!wasCurrentExecutor && isCurrentExecutor)
            {
                LOGGER.info("Cluster-wide lease has been claimed by sidecarHostId={}", sidecarHostId);
                // notify lease has been gained
                vertx.eventBus().publish(ON_SIDECAR_GLOBAL_LEASE_CLAIMED.address(), sidecarHostId);
            }

            if (LOGGER.isDebugEnabled() && wasCurrentExecutor && isCurrentExecutor)
            {
                LOGGER.debug("Cluster-wide lease has been extended by sidecarHostId={}", sidecarHostId);
            }
        }
    }

    private boolean leaseExpired()
    {
        return leaseTime != null && leaseExpirationTime().isBefore(Instant.now());
    }

    private Instant leaseExpirationTime()
    {
        return leaseTime.plus(config.schemaKeyspaceConfiguration().leaseSchemaTTLSeconds(), ChronoUnit.SECONDS);
    }

    void updateMetrics()
    {
        if (determination.shouldExecuteOnLocalInstance())
        {
            metrics.leaseHolders.metric.update(1);
        }
        metrics.participants.metric.update(1);
    }

    protected Boolean determineIfIsCurrentLeaseHolder(Boolean isCurrentLeaseHolder,
                                                      SidecarLeaseDatabaseAccessor.LeaseClaimResult result,
                                                      String sidecarHostId)
    {
        if (result == null)
        {
            return isCurrentLeaseHolder;
        }

        if (result.leaseAcquired)
        {
            return true;
        }

        // For the case where the current Sidecar was a lease-holder but the information was lost from
        // the in-memory process (i.e. Sidecar restarted) but the information is still persisted
        // in the database, so we recover the state from the database. Currently, the implementation
        // relies on the sidecarHostId, which is a unique UUID generated during cluster start-up.
        // For this feature to survive Sidecar restarts we need a more deterministic way to specify
        // the sidecar host ID.
        return sidecarHostId.equals(result.currentOwner);
    }

    /**
     * Returns a unique identifier for the Sidecar instance.
     *
     * @return a unique identifier for the Sidecar instance
     */
    protected String sidecarHostId()
    {
        return config.hostId();
    }

    /**
     * Make this Sidecar instance a non-executor for testing purposes
     */
    @VisibleForTesting
    void resetExecutor()
    {
        determination = ExecutionDetermination.INDETERMINATE;
    }

    /**
     * Configuration parameters for the {@link BestEffortSingleConditionalExecutor}
     */
    public static class Parameters
    {
        public static final boolean DEFAULT_ENABLED = true;
        public static final long DEFAULT_FREQUENCY_MILLIS = TimeUnit.MINUTES.toMillis(1);
        public static final long MINIMUM_FREQUENCY_MILLIS = TimeUnit.SECONDS.toMillis(30);
        public static final long DEFAULT_INITIAL_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(1);

        private final Map<String, String> conditionalExecutorParameters;

        public Parameters(Map<String, String> conditionalExecutorParameters)
        {
            this.conditionalExecutorParameters = conditionalExecutorParameters;
        }

        public static Parameters from(Map<String, String> conditionalExecutorParameters)
        {
            return new Parameters(conditionalExecutorParameters);
        }

        public boolean isEnabled()
        {
            String value = conditionalExecutorParameters.get("is_enabled");
            if ("true".equalsIgnoreCase(value))
                return true;
            if ("false".equalsIgnoreCase(value))
                return false;
            return DEFAULT_ENABLED;
        }

        public long initialDelayMillis()
        {
            return getConfigurationOrDefault("initial_delay_millis", 0, DEFAULT_INITIAL_DELAY_MILLIS);
        }

        public long delayMillis()
        {
            return getConfigurationOrDefault("frequency_millis", MINIMUM_FREQUENCY_MILLIS, DEFAULT_FREQUENCY_MILLIS);
        }

        private long getConfigurationOrDefault(String configurationName, long minimumValue, long defaultValue)
        {
            String stringValue = conditionalExecutorParameters.get(configurationName);
            long value = defaultValue;
            try
            {
                value = Long.parseLong(stringValue);
            }
            catch (NumberFormatException exception)
            {
                LOGGER.warn("Ignoring invalid {} configuration '{}' for BestEffortSingleConditionalExecutor",
                            configurationName, stringValue);
                return value;
            }

            if (value >= minimumValue)
            {
                return value;
            }
            else
            {
                LOGGER.warn("Ignoring {} configuration value '{}' for BestEffortSingleConditionalExecutor " +
                            "which is less than the required minimum of '{}'",
                            configurationName, value, minimumValue);
                return minimumValue;
            }
        }
    }
}
