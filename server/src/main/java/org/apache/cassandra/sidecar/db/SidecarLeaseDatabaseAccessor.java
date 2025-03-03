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

package org.apache.cassandra.sidecar.db;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.SidecarLeaseSchema;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Encapsulates database access operations for the Sidecar Lease election process
 */
@Singleton
public class SidecarLeaseDatabaseAccessor extends DatabaseAccessor<SidecarLeaseSchema>
{
    @Inject
    public SidecarLeaseDatabaseAccessor(SidecarSchema sidecarSchema, CQLSessionProvider sessionProvider)
    {
        this(sidecarSchema.tableSchema(SidecarLeaseSchema.class), sessionProvider);
    }

    @VisibleForTesting
    public SidecarLeaseDatabaseAccessor(SidecarLeaseSchema leaseSchema, CQLSessionProvider sessionProvider)
    {
        super(leaseSchema, sessionProvider);
    }

    /**
     * Attempts to obtain the lease, returning the result of the claim
     *
     * @param leaseClaimer the identifier of the instances attempting to claim the lease
     * @return the results of performing the lease claim
     * @throws IllegalArgumentException when the owner information is not available from the lease claim operation
     */
    public LeaseClaimResult claimLease(String leaseClaimer)
    {
        BoundStatement statement = tableSchema.claimLeaseStatement().bind(leaseClaimer);
        ResultSet resultSet = execute(statement);
        return LeaseClaimResult.from(resultSet, leaseClaimer);
    }

    /**
     * Attempts to extend the existing lease, returning the result of the attempt
     *
     * @param currentOwner the current owner extending the lease
     * @return the results of performing the lease extension
     * @throws IllegalArgumentException when the owner information is not available from the lease extension operation
     */
    public LeaseClaimResult extendLease(String currentOwner)
    {
        BoundStatement statement = tableSchema.extendLeaseStatement().bind(currentOwner, currentOwner);
        ResultSet resultSet = execute(statement);
        return LeaseClaimResult.from(resultSet, currentOwner);
    }

    /**
     * Captures the results of claiming lease for a given Sidecar owner
     */
    public static class LeaseClaimResult
    {
        public final String currentOwner;

        LeaseClaimResult(String currentOwner)
        {
            this.currentOwner = currentOwner;
        }

        static LeaseClaimResult from(ResultSet resultSet, String newOwner)
        {
            return resultSet.wasApplied()
                   ? new LeaseClaimResult(newOwner)
                   // In some rare cases, the resultSet will not contain the owner information
                   // even though the resultSet was not applied. This will translate into an
                   // IllegalArgumentException being thrown when trying to retrieve the non-existing
                   // owner string. This exception is left to be handled by the caller method
                   : new LeaseClaimResult(resultSet.one().getString("owner"));
        }
    }
}
