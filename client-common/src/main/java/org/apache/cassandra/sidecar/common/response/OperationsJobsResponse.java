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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.utils.OperationsJobResult;

/**
 * Response structure of the operations jobs API
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OperationsJobsResponse
{
    private final UUID jobId;
    private final OperationsJobResult.OperationsJobStatus status;
    private final String operation;
    private final String reason;


    @JsonCreator
    public OperationsJobsResponse(@JsonProperty("jobId") UUID jobId,
                                  @JsonProperty("jobStatus") OperationsJobResult.OperationsJobStatus status,
                                  @JsonProperty("operation") String operation,
                                  @JsonProperty("reason") String reason)
    {
        this.jobId = jobId;
        this.status = status;
        this.operation = operation;
        this.reason = reason;
    }

    /**
     * @return job id of operations job
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
    public OperationsJobResult.OperationsJobStatus status()
    {
        return status;
    }

    /**
     * @return operation of the job
     */
    @JsonProperty("operation")
    public String operation()
    {
        return operation;
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
