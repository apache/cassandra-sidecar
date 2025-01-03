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

import java.util.Objects;

import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Holds information about whether this Sidecar instance has claimed the lease, it has not, or it's unable
 * to determinate if the lease has been claimed.
 */
@Singleton
public class ClusterLease
{
    private volatile Ownership leaseOwnership;

    public ClusterLease()
    {
        this(Ownership.INDETERMINATE);
    }

    public ClusterLease(Ownership leaseOwnership)
    {
        this.leaseOwnership = leaseOwnership;
    }

    /**
     * @return the current execution determination
     */
    public ScheduleDecision toScheduleDecision()
    {
        return leaseOwnership.toScheduleDecision();
    }

    /**
     * @return {@code true} if the local Sidecar instance has claimed the cluster lease, {@code false} otherwisel
     */
    public boolean isClaimedByLocalSidecar()
    {
        return leaseOwnership == Ownership.CLAIMED;
    }

    /**
     * Updates the ownership of the lease
     *
     * @param ownership ownership of the lease
     */
    void setOwnership(Ownership ownership)
    {
        this.leaseOwnership = Objects.requireNonNull(ownership, "ownership must be provided");
    }

    @VisibleForTesting // only use it for testing
    public void setOwnershipTesting(Ownership ownership)
    {
        setOwnership(ownership);
    }

    /**
     * Ownership of the cluster-wide lease
     */
    public enum Ownership
    {
        /**
         * The cluster lease is claimed by the local Sidecar
         */
        CLAIMED,
        /**
         * The cluster lease is lost by the local Sidecar. It is claimed by another remote Sidecar.
         */
        LOST,
        /**
         * The cluster lease is neither claimed by the local Sidecar nor any other remote Sidecar
         */
        INDETERMINATE;

        private ScheduleDecision toScheduleDecision()
        {
            switch (this)
            {
                case CLAIMED:
                    return ScheduleDecision.EXECUTE;
                case LOST:
                    return ScheduleDecision.SKIP;
                case INDETERMINATE:
                default:
                    // When the process is unable to determine whether it is the leaseholder, we want
                    // the task to be rescheduled for a shorter period of time. Assume you have a PeriodicTask that
                    // runs every day. If it happens to run during a period of time when there's no
                    // determination whether task should run or not, we do not want to wait another day for the
                    // task to run, so instead we reschedule it and only wait the initial delay of the task for
                    // the task to try again.
                    return ScheduleDecision.RESCHEDULE;
            }
        }
    }
}
