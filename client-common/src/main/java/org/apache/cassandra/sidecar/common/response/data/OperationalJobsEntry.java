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

package org.apache.cassandra.sidecar.common.response.data;

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Structure of the operational job instance within the list operational jobs API response
 */
public class OperationalJobsEntry
{
    public final UUID jobId;
    public final String status;
    public final String failureReason;
    public final String operation;

    /**
     * Constructs a {@link OperationalJobsEntry} object.
     */
    public OperationalJobsEntry(@JsonProperty("jobId") UUID jobId,
                                @JsonProperty("status") String status,
                                @JsonProperty("failureReason") String failureReason,
                                @JsonProperty("operation") String operation)
    {
        this.jobId = jobId;
        this.status = status;
        this.failureReason = failureReason;
        this.operation = operation;
    }
}
