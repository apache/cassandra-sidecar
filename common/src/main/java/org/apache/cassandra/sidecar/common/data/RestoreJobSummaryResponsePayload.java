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

import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_AGENT;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_CREATED_AT;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_ID;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_KEYSPACE;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_SECRETS;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_STATUS;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_TABLE;

/**
 * Restore job summary view for client
 */
public class RestoreJobSummaryResponsePayload
{
    private final String createdAt;
    private final UUID jobId;
    private final String jobAgent;
    private final String keyspace;
    private final String table;
    private final RestoreJobSecrets secrets;
    private final String status;

    @JsonCreator
    public RestoreJobSummaryResponsePayload(@JsonProperty(JOB_CREATED_AT) String createdAt,
                                            @JsonProperty(JOB_ID) UUID jobId,
                                            @JsonProperty(JOB_AGENT) String jobAgent,
                                            @JsonProperty(JOB_KEYSPACE) String keyspace,
                                            @JsonProperty(JOB_TABLE) String table,
                                            @JsonProperty(JOB_SECRETS) RestoreJobSecrets secrets,
                                            @JsonProperty(JOB_STATUS) String status)
    {
        this.createdAt = createdAt;
        this.jobId = jobId;
        this.jobAgent = jobAgent;
        this.keyspace = keyspace;
        this.table = table;
        this.secrets = secrets;
        this.status = status;
    }

    @JsonProperty(JOB_CREATED_AT)
    public String createdAt()
    {
        return createdAt;
    }

    @JsonProperty(JOB_ID)
    public UUID jobId()
    {
        return jobId;
    }

    @JsonProperty(JOB_AGENT)
    public String jobAgent()
    {
        return jobAgent;
    }

    @JsonProperty(JOB_KEYSPACE)
    public String keyspace()
    {
        return keyspace;
    }

    @JsonProperty(JOB_TABLE)
    public String table()
    {
        return table;
    }

    @JsonProperty(JOB_SECRETS)
    public RestoreJobSecrets secrets()
    {
        return secrets;
    }

    @JsonProperty(JOB_STATUS)
    public String status()
    {
        return status;
    }
}
