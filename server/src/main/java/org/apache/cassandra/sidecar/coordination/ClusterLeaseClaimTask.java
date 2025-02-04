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
import java.util.Objects;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryConsistencyException;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.PeriodicTaskConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.db.SidecarLeaseDatabaseAccessor;
import org.apache.cassandra.sidecar.metrics.CoordinationMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.jetbrains.annotations.VisibleForTesting;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_GLOBAL_LEASE_CLAIMED;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_GLOBAL_LEASE_LOST;

/**
 * A best-effort process to determine a single Sidecar instance that holds
 * a cluster-wide lease. The Sidecar instances attempting to claim the lease
 * is determined by the {@link ElectorateMembership}.
 * The electorate is expected to be a small subset of the entirety of Sidecar
 * instances.
 *
 * <p>There will be situations where multiple members of the electorate may
 * claim the cluster lease, for example in cases where we have:
 * <ul>
 *     <li>Network partitions
 *     <li>Binary protocol is disabled for a member of the electorate
 * </ul>
 *
 * <p>The leaseholder instance(s) must keep in mind that there might be other leaseholder
 * instances in the cluster, so operations that they perform must be safe to be performed
 * by one or more Sidecar instances.
 */
public class ClusterLeaseClaimTask implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterLeaseClaimTask.class);

    static final SecondBoundConfiguration MINIMUM_DELAY = SecondBoundConfiguration.parse("30s");
    private final ElectorateMembership electorateMembership;
    private final SidecarLeaseDatabaseAccessor accessor;
    private final ClusterLease clusterLease;
    private final CoordinationMetrics metrics;
    private final PeriodicTaskConfiguration periodicTaskConfiguration;
    private final ServiceConfiguration config;
    private final Vertx vertx;
    private String currentLeaseholder;
    private Instant leaseTime;

    public ClusterLeaseClaimTask(Vertx vertx,
                                 ServiceConfiguration serviceConfiguration,
                                 ElectorateMembership electorateMembership,
                                 SidecarLeaseDatabaseAccessor accessor,
                                 ClusterLease clusterLease,
                                 SidecarMetrics metrics)
    {
        this.vertx = vertx;
        this.periodicTaskConfiguration = serviceConfiguration.coordinationConfiguration().clusterLeaseClaimConfiguration();
        this.config = serviceConfiguration;
        this.electorateMembership = electorateMembership;
        this.accessor = accessor;
        this.clusterLease = clusterLease;
        this.metrics = metrics.server().coordination();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ScheduleDecision scheduleDecision()
    {
        // The Sidecar schema feature is required for this implementation
        // so skip when the feature is not enabled
        boolean isEnabled = config.schemaKeyspaceConfiguration().isEnabled() && periodicTaskConfiguration.enabled();
        boolean isMember = false;
        if (isEnabled)
        {
            // Do expensive call when the feature is enabled to determine if this Sidecar is member
            // of the electorate
            try
            {
                isMember = electorateMembership.isMember();
            }
            catch (Throwable t)
            {
                LOGGER.debug("Membership determination fails due to unexpected exception", t);
                // isMember remains false
            }
            LOGGER.debug("Sidecar instance part of electorate isMember={}", isMember);
        }
        if (!isEnabled || !isMember)
        {
            clusterLease.setOwnership(ClusterLease.Ownership.LOST);
            return ScheduleDecision.SKIP;
        }

        // When accessor is available, it permits execution (where accessor is used)
        // Otherwise, it reschedules to skip the run and retry sooner, assuming initial delay is less than delay
        return accessor.isAvailable() ? ScheduleDecision.EXECUTE : ScheduleDecision.RESCHEDULE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DurationSpec initialDelay()
    {
        return periodicTaskConfiguration.initialDelay();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DurationSpec delay()
    {
        DurationSpec delay = periodicTaskConfiguration.executeInterval();

        if (delay.compareTo(MINIMUM_DELAY) < 0)
        {
            LOGGER.warn("Ignoring delay value {} which is less than the required minimum of {}", delay, MINIMUM_DELAY);
            return MINIMUM_DELAY;
        }

        return delay;
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        runClaimProcess();
        promise.complete();
    }

    @VisibleForTesting
    protected void runClaimProcess()
    {
        String sidecarHostId = sidecarHostId();
        boolean wasLeaseholder = isCurrentLeaseholder(sidecarHostId);
        tryToClaimClusterLease(sidecarHostId);
        updateClusterLease(sidecarHostId, wasLeaseholder);
        maybeNotify(sidecarHostId, wasLeaseholder);
        updateMetrics();
    }

    protected void tryToClaimClusterLease(String sidecarHostId)
    {
        LOGGER.debug("Starting selection for sidecarHostId={}", sidecarHostId);

        if (isCurrentLeaseholder(sidecarHostId))
        {
            currentLeaseholder = executeLeaseAction("extend", sidecarHostId, accessor::extendLease);
        }
        else
        {
            // always try to claim the lease, because we don't know when the leaseholder will
            // give up the lease
            currentLeaseholder = executeLeaseAction("claim", sidecarHostId, accessor::claimLease);
        }
    }

    protected String executeLeaseAction(String actionName,
                                        String sidecarHostId,
                                        Function<String, SidecarLeaseDatabaseAccessor.LeaseClaimResult> actionFn)
    {
        try
        {
            LOGGER.debug("Attempting to {} lease for sidecarHostId={}", actionName, sidecarHostId);
            return actionFn.apply(sidecarHostId).currentOwner;
        }
        catch (QueryConsistencyException | NoHostAvailableException e)
        {
            LOGGER.debug("Unable to {} lease for sidecarHostId={}", actionName, sidecarHostId, e);
        }
        catch (Exception e)
        {
            LOGGER.error("Unable to {} lease for sidecarHostId={}", actionName, sidecarHostId, e);
        }
        return null; // owner is unknown
    }

    protected void updateClusterLease(String sidecarHostId, boolean wasLeaseholder)
    {
        // When the lease operation failed, the currentLeaseholder field will be null.
        // This means that we are not able to determine who the leaseholder is.
        // However, if we are the leaseholder, we want to preserve that information
        // unless the lease expires, in which case we set the cluster lease to
        // the indeterminate state.
        if (currentLeaseholder == null)
        {
            boolean leaseExpired = leaseExpired();
            if (wasLeaseholder && !leaseExpired)
            {
                // do not extend the lease time here because leaseholder is not resolved in this run
                currentLeaseholder = sidecarHostId;
                LOGGER.debug("Lease will expire on {}. Assume the current leaseholder, even though no leaseholder is resolved from this run",
                             leaseExpirationTime());
            }
            else
            {
                if (leaseExpired)
                {
                    LOGGER.info("Giving up lease for sidecarHostId={} leaseAcquired={} leaseExpired={}",
                                sidecarHostId, leaseTime, leaseExpirationTime());
                }

                leaseTime = null;
                clusterLease.setOwnership(ClusterLease.Ownership.INDETERMINATE);
            }
            return;
        }

        if (isCurrentLeaseholder(sidecarHostId))
        {
            leaseTime = Instant.now();
            clusterLease.setOwnership(ClusterLease.Ownership.CLAIMED);
        }
        else
        {
            leaseTime = null;
            clusterLease.setOwnership(ClusterLease.Ownership.LOST);
        }
    }

    protected void maybeNotify(String sidecarHostId, boolean wasLeaseholder)
    {
        boolean isCurrentLeaseholder = isCurrentLeaseholder(sidecarHostId);
        // lease has been lost
        if (wasLeaseholder && !isCurrentLeaseholder)
        {
            LOGGER.info("Cluster-wide lease has been lost by sidecarHostId={}", sidecarHostId);
            vertx.eventBus().publish(ON_SIDECAR_GLOBAL_LEASE_LOST.address(), sidecarHostId);
        }

        // lease has been claimed
        if (!wasLeaseholder && isCurrentLeaseholder)
        {
            LOGGER.info("Cluster-wide lease has been claimed by sidecarHostId={}", sidecarHostId);
            vertx.eventBus().publish(ON_SIDECAR_GLOBAL_LEASE_CLAIMED.address(), sidecarHostId);
        }

        // lease has been extended
        if (LOGGER.isDebugEnabled() && wasLeaseholder && isCurrentLeaseholder)
        {
            LOGGER.debug("Cluster-wide lease has been extended by sidecarHostId={}", sidecarHostId);
        }
    }

    void updateMetrics()
    {
        if (clusterLease.isClaimedByLocalSidecar())
        {
            metrics.leaseholders.metric.update(1);
        }
        metrics.participants.metric.update(1);
    }

    private boolean leaseExpired()
    {
        return leaseTime != null && leaseExpirationTime().isBefore(Instant.now());
    }

    private Instant leaseExpirationTime()
    {
        return leaseTime.plus(config.schemaKeyspaceConfiguration().leaseSchemaTTL().toSeconds(), ChronoUnit.SECONDS);
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

    private boolean isCurrentLeaseholder(String sidecarHostId)
    {
        // For the case where the current Sidecar was a lease-holder but the information was lost from
        // the in-memory process (i.e. Sidecar restarted) but the information is still persisted
        // in the database, so we recover the state from the database. Currently, the implementation
        // relies on the sidecarHostId, which is a unique UUID generated during cluster start-up.
        // For this feature to survive Sidecar restarts we need a more deterministic way to specify
        // the sidecar host ID.
        return Objects.equals(sidecarHostId, currentLeaseholder);
    }

    /**
     * Resets the leaseholder information for testing purposes
     */
    @VisibleForTesting
    void resetLeaseholder()
    {
        currentLeaseholder = null;
        leaseTime = null;
        clusterLease.setOwnership(ClusterLease.Ownership.INDETERMINATE);
    }
}
