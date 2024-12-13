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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.CASWriteUnknownException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.CoordinationConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.db.SidecarLeaseDatabaseAccessor;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.jetbrains.annotations.VisibleForTesting;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_GLOBAL_LEASE_CLAIMED;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_GLOBAL_LEASE_LOST;

/**
 * A best-effort process where 0 or more instance executors can be chosen from the electorate.
 * The electorate is expected to be a small subset of the entirety of Sidecar instances.
 *
 * <p>There will be situations where multiple members of the electorate will
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
public class BestEffortSingleInstanceExecutor implements SingleInstanceExecutor, PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BestEffortSingleInstanceExecutor.class);
    private final ElectorateMembership electorateMembership;
    private final SidecarLeaseDatabaseAccessor accessor;
    private final ServiceConfiguration config;
    private final CoordinationConfiguration coordinationConfiguration;
    private final Vertx vertx;
    private final TaskExecutorPool internalPool;
    private volatile boolean isLocalSidecarSingleInstanceExecutor = false;

    public BestEffortSingleInstanceExecutor(Vertx vertx,
                                            ExecutorPools executorPools,
                                            ServiceConfiguration serviceConfiguration,
                                            ElectorateMembership electorateMembership,
                                            SidecarLeaseDatabaseAccessor accessor)
    {
        this.vertx = vertx;
        this.internalPool = executorPools.internal();
        this.config = serviceConfiguration;
        this.coordinationConfiguration = serviceConfiguration.coordinationConfiguration();
        this.electorateMembership = electorateMembership;
        this.accessor = accessor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isLocalSidecarSingleInstanceExecutor()
    {
        return isLocalSidecarSingleInstanceExecutor;
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
        return !coordinationConfiguration.singleInstanceExecutorProcessEnabled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long initialDelay()
    {
        return coordinationConfiguration.singleInstanceExecutorInitialDelayMillis();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long delay()
    {
        return coordinationConfiguration.singleInstanceExecutorFrequencyMillis();
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        // Complete early so we can be scheduled for the next iteration
        promise.complete();

        // runs the election process based on the electorate membership
        internalPool.runBlocking(() -> determineSingleInstanceExecutor(electorateMembership));
    }

    @Override
    public void determineSingleInstanceExecutor(ElectorateMembership electorateMembership)
    {
        boolean shouldParticipate = electorateMembership.shouldParticipate();
        LOGGER.debug("Sidecar instance shouldParticipate={} in the selection", shouldParticipate);
        if (!shouldParticipate)
        {
            return;
        }

        boolean wasCurrentExecutor = isLocalSidecarSingleInstanceExecutor;
        Boolean isCurrentExecutor = null;

        String sidecarHostId = sidecarHostId();
        LOGGER.debug("Starting selection for sidecarHostId={}", sidecarHostId);
        if (wasCurrentExecutor)
        {
            isCurrentExecutor = executeLeaseAction(sidecarHostId, isCurrentExecutor);
        }

        if (isCurrentExecutor == null || !isCurrentExecutor)
        {
            SidecarLeaseDatabaseAccessor.LeaseClaimResult result = null;
            try
            {
                LOGGER.debug("Attempting to claim lease for sidecarHostId={}", sidecarHostId);
                result = accessor.claimLease(sidecarHostId);
            }
            catch (CASWriteUnknownException | NoHostAvailableException e)
            {
                LOGGER.debug("Unable to claim lease for sidecarHostId={}", sidecarHostId, e);
            }
            catch (Exception e)
            {
                LOGGER.error("Unable to claim lease for sidecarHostId={}", sidecarHostId, e);
            }

            isCurrentExecutor = determineIfIsCurrentLeaseHolder(isCurrentExecutor, result, sidecarHostId);
            LOGGER.debug("Claim lease for sidecarHostId={} result={}", sidecarHostId, isCurrentExecutor);
        }

        maybeNotifyResults(isCurrentExecutor, sidecarHostId, wasCurrentExecutor);
    }

    private Boolean executeLeaseAction(String sidecarHostId, Boolean isCurrentExecutor)
    {
        SidecarLeaseDatabaseAccessor.LeaseClaimResult result = null;
        try
        {
            LOGGER.debug("Attempting to extend lease for sidecarHostId={}", sidecarHostId);
            result = accessor.extendLease(sidecarHostId);
        }
        catch (CASWriteUnknownException | NoHostAvailableException e)
        {
            LOGGER.debug("Unable to claim lease for sidecarHostId={}", sidecarHostId, e);
        }
        catch (Exception e)
        {
            LOGGER.error("Unable to extend lease for sidecarHostId={}", sidecarHostId, e);
        }

        isCurrentExecutor = determineIfIsCurrentLeaseHolder(isCurrentExecutor, result, sidecarHostId);
        LOGGER.debug("Extend lease for sidecarHostId={} result={}", sidecarHostId, isCurrentExecutor);
        return isCurrentExecutor;
    }

    private void maybeNotifyResults(Boolean isCurrentExecutor, String owner, boolean wasCurrentExecutor)
    {
        if (isCurrentExecutor == null)
        {
            LOGGER.debug("Unable to perform lease election for owner={}", owner);

//            if (wasCurrentExecutor)
//            {
            // TODO : do we give up lease after some period of time? ie TTL which is 10 minutes?
//            }
        }
        else
        {
            if (wasCurrentExecutor && !isCurrentExecutor)
            {
                LOGGER.info("Cluster-wide lease has been lost by owner={}", owner);
                isLocalSidecarSingleInstanceExecutor = isCurrentExecutor;
                // notify lease has been lost
                vertx.eventBus().publish(ON_SIDECAR_GLOBAL_LEASE_LOST.address(), owner);
            }

            if (!wasCurrentExecutor && isCurrentExecutor)
            {
                LOGGER.info("Cluster-wide lease has been claimed by owner={}", owner);
                isLocalSidecarSingleInstanceExecutor = isCurrentExecutor;
                // notify lease has been gained
                vertx.eventBus().publish(ON_SIDECAR_GLOBAL_LEASE_CLAIMED.address(), owner);
            }

            if (LOGGER.isDebugEnabled() && wasCurrentExecutor && isCurrentExecutor)
            {
                LOGGER.debug("Cluster-wide lease has been extended by owner={}", owner);
            }
        }
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
        // in the database, so we recover the state from the database
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
        isLocalSidecarSingleInstanceExecutor = false;
    }
}
