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

package org.apache.cassandra.sidecar.common.data;

import static org.apache.cassandra.sidecar.common.request.data.NodeCommandRequestPayload.State;

/**
 * Utilities for representing the lifecycle state of a Cassandra instance and the status of
 * lifecycle operations executed by the LifecycleManager.
 */
public final class Lifecycle
{
    /**
     * Represents the lifecycle state of a Cassandra instance.
     */
    public enum CassandraState
    {
        /**
         * The state when a desired lifecycle state has not been submitted yet
         */
        UNKNOWN,
        /**
         * The state when a Cassandra process is running
         */
        RUNNING,
        /**
         * The state when a Cassandra process is not running
         */
        STOPPED;

        public boolean isRunning()
        {
            return this == RUNNING;
        }

        public static CassandraState fromNodeCommandState(State state)
        {
            return state == State.START ? RUNNING : STOPPED;
        }
    }

    /**
     * Represents the status of a LifecycleManager operation to converge the current lifecycle state
     * of a Cassandra instance to the desired state.
     */
    public enum OperationStatus
    {
        /**
         * The status when a desired lifecycle state has not been submitted yet
         */
        UNDEFINED,
        /**
         * The status when the current lifecycle state of an instance matches the desired lifecycle state
         */
        CONVERGED,
        /**
         * The status when the current lifecycle state of an instance does not match the desired lifecycle state,
         * and there is no operation in progress to match the states
         */
        DIVERGED,
        /**
         * The status when the current lifecycle state of an instance does not match the desired lifecycle state,
         * and there is an operation in progress to start or stop the instance to match the states
         */
        CONVERGING;

        public boolean isCompleted()
        {
            return this == CONVERGED || this == DIVERGED;
        }

        public boolean isConverged()
        {
            return this == CONVERGED;
        }
    }
}
