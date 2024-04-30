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

package org.apache.cassandra.sidecar.common.request.data;

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_ID;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_STATUS;

/**
 * A class representing a response for the {@link CreateRestoreJobRequestPayload}.
 */
public class CreateRestoreJobResponsePayload
{
    private final UUID jobId;
    private final String status;

    @JsonCreator
    public CreateRestoreJobResponsePayload(@JsonProperty(JOB_ID) UUID jobId,
                                           @JsonProperty(JOB_STATUS) String status)
    {
        this.jobId = jobId;
        this.status = status;
    }

    /**
     * @return job id of restore job
     */
    @JsonProperty(JOB_ID)
    public UUID jobId()
    {
        return jobId;
    }

    /**
     * @return status of the job
     */
    @JsonProperty(JOB_STATUS)
    public String status()
    {
        return status;
    }
}
