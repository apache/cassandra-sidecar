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

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;

/**
 * Response structure of the jobs status API
 */
public class NodeDecommissionResponse
{
    private final UUID jobId;
    private final OperationalJobStatus status;

    private final String instance;

    private final String reason;

    @JsonCreator
    public NodeDecommissionResponse(@JsonProperty("jobId") UUID jobId,
                                    @JsonProperty("jobStatus") OperationalJobStatus status,
                                    @JsonProperty("instance") String instance,
                                    @JsonProperty("reason") String reason)
    {
        this.jobId = jobId;
        this.status = status;
        this.instance = instance;
        this.reason = reason;
    }

    /**
     * @return identifier of the job
     */
    @JsonProperty("jobId")
    public UUID jobId()
    {
        return jobId;
    }

    /**
     * @return status of the job
     */
    @JsonProperty("jobStatus")
    public OperationalJobStatus status()
    {
        return status;
    }

    /**
     * @return job id of restore job
     */
    @JsonProperty("instance")
    public String instance()
    {
        return instance;
    }

    /**
     * @return reason for job failure
     */
    @JsonProperty("reason")
    public String reason()
    {
        return reason;
    }

}
