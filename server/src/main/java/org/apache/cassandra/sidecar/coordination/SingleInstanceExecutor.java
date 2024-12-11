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

/**
 * Defines an interface to choose a single Sidecar instance that will run certain types of operations that need to
 * run on a limited subset of Sidecar instances. In most cases there will be a single Sidecar instance chosen as
 * the executor.
 */
public interface SingleInstanceExecutor
{
    SingleInstanceExecutor ALWAYS_SCHEDULE_EXECUTOR = new SingleInstanceExecutor()
    {
        @Override
        public void determineSingleInstanceExecutor(ElectorateMembership electorateMembership)
        {
        }

        @Override
        public boolean isLocalSidecarSingleInstanceExecutor()
        {
            return true;
        }
    };
    /**
     * A process that determines the single instance executor.
     *
     * @param electorateMembership determines eligibility to participate in the determination of the single instance
     *                             executor
     */
    void determineSingleInstanceExecutor(ElectorateMembership electorateMembership);

    /**
     * @return {@code true} if the local Sidecar instance is a single instance executor, {@code false} otherwise
     */
    boolean isLocalSidecarSingleInstanceExecutor();
}
