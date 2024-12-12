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

/**
 * Encapsulates the states of the job lifecycle.
 * Operational jobs are the ones running on Cassandra, e.g. decommission, etc.
 */
public enum OperationalJobStatus
{
    /**
     * The operational job is created
     */
    CREATED,
    /**
     * The operational job is running on Cassandra
     */
    RUNNING,
    /**
     * The operational job succeeds
     */
    SUCCEEDED,
    /**
     * The operational job fails
     */
    FAILED;

    public boolean isCompleted()
    {
        return this == SUCCEEDED || this == FAILED;
    }
}
