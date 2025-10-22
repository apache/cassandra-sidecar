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

package org.apache.cassandra.sidecar.common.response;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.Nullable;

/**
 * Response object representing the status of a live migration operation.
 * This class encapsulates the current state of a live migration process.
 */
public class LiveMigrationStatus
{
    public static final LiveMigrationStatus NOT_COMPLETED_STATUS = new LiveMigrationStatus(MigrationState.NOT_COMPLETED, null);

    private final MigrationState state;
    private final Long endTime;

    @JsonCreator
    public LiveMigrationStatus(@JsonProperty("state") MigrationState state,
                               @JsonProperty("endTime") Long endTime)
    {
        if (state == MigrationState.COMPLETED && endTime == null)
        {
            throw new IllegalArgumentException("endTime cannot be null if state is COMPLETED");
        }
        this.state = state;
        this.endTime = endTime;
    }

    /**
     * Returns the live migration state.
     *
     * @return COMPLETED - if live migration has been completed
     * NOT_COMPLETED - if live migration has not been completed
     */
    @JsonProperty("state")
    public MigrationState state()
    {
        return state;
    }


    @Nullable
    @JsonProperty("endTime")
    public Long endTime()
    {
        return endTime;
    }

    /**
     * Represents possible states of a live migration operation.
     */
    public enum MigrationState
    {
        /** Indicates that the live migration is still in progress and has not completed yet. */
        NOT_COMPLETED,
        /** Indicates that the live migration has finished successfully. */
        COMPLETED
    }

    @Override
    public String toString()
    {
        return "LiveMigrationStatus{" +
               "state=" + state +
               ", endTime=" + endTime +
               '}';
    }

    @Override
    public boolean equals(Object object)
    {
        if (object == null || getClass() != object.getClass()) return false;
        LiveMigrationStatus status = (LiveMigrationStatus) object;
        return state == status.state && Objects.equals(endTime, status.endTime);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(state, endTime);
    }
}
