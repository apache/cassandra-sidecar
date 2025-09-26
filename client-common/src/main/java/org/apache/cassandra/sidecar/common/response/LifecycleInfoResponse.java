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

import org.apache.cassandra.sidecar.common.data.Lifecycle.CassandraState;
import org.apache.cassandra.sidecar.common.data.Lifecycle.OperationStatus;

/**
 * A class representing a response for the {@code LifecycleInfoRequest}.
 */
public class LifecycleInfoResponse
{

    private final CassandraState currentState;
    private final CassandraState desiredState;
    private final OperationStatus status;
    private final String lastUpdate;

    /**
     * Constructs a {@link LifecycleInfoResponse} object with the {@code currentState}, {@code intendedState}, {@code result},
     * and {@code message}
     *
     * @param currentState the current state of the Cassandra node
     * @param desiredState the intended state of the Cassandra node
     * @param status the result of the last lifecycle operation
     * @param lastUpdate a message providing additional context about the last lifecycle operation
     */
    @JsonCreator
    public LifecycleInfoResponse(@JsonProperty("current_state") CassandraState currentState,
                                 @JsonProperty("desired_state") CassandraState desiredState,
                                 @JsonProperty("status") OperationStatus status,
                                 @JsonProperty("last_update") String lastUpdate)
    {
        this.currentState = Objects.requireNonNull(currentState, "State must be non-null");
        this.desiredState = desiredState;
        this.status = status;
        this.lastUpdate = lastUpdate;
    }

    /**
     * @return the current state of the Cassandra node
     */
    @JsonProperty("current_state")
    public CassandraState currentState()
    {
        return currentState;
    }

    /**
     * @return the intended state of the Cassandra node
     */
    @JsonProperty("desired_state")
    public CassandraState desiredState()
    {
        return desiredState;
    }

    /**
     * @return the status of the last lifecycle operation
     */
    @JsonProperty("status")
    public OperationStatus status()
    {
        return status;
    }

    /**
     * @return message providing additional context about the last lifecycle operation
     */
    @JsonProperty("last_update")
    public String lastUpdate()
    {
        return lastUpdate;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof LifecycleInfoResponse)) return false;
        LifecycleInfoResponse that = (LifecycleInfoResponse) o;
        return currentState == that.currentState
               && desiredState == that.desiredState
               && status == that.status
               && Objects.equals(lastUpdate, that.lastUpdate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(currentState, desiredState, status, lastUpdate);
    }

    @Override
    public String toString()
    {
        return "LifecycleInfoResponse{" +
               "state=" + currentState +
               ", intent=" + desiredState +
               ", result=" + status +
               ", message='" + lastUpdate + '\'' +
               '}';
    }
}

