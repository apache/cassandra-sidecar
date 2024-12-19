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
import org.apache.cassandra.sidecar.tasks.ExecutionDetermination;

/**
 * Holds information about whether this Sidecar instance has claimed the lease, it has not, or it's unable
 * to determinate if the lease has been claimed.
 */
@Singleton
public class ClusterLease
{
    private volatile ExecutionDetermination executionDetermination = ExecutionDetermination.INDETERMINATE;

    public ClusterLease()
    {
    }

    public ClusterLease(ExecutionDetermination executionDetermination)
    {
        this.executionDetermination = executionDetermination;
    }

    /**
     * @return the current execution determination
     */
    public ExecutionDetermination executionDetermination()
    {
        return executionDetermination;
    }

    /**
     * @return {@code true} if the local Sidecar instance has claimed the cluster lease, {@code false} otherwisel
     */
    public boolean isClaimedByLocalSidecar()
    {
        return executionDetermination.shouldExecuteOnLocalSidecar();
    }

    /**
     * Updates the determination
     *
     * @param executionDetermination the new value
     */
    void setExecutionDetermination(ExecutionDetermination executionDetermination)
    {
        this.executionDetermination = Objects.requireNonNull(executionDetermination, "executionDetermination must be provided");
    }
}
