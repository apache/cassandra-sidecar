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

/**
 * Response object returned when a live migration task is created.
 * Contains the task identifier and a URL to query the task status.
 */
public class LiveMigrationTaskCreationResponse
{
    private final String taskId;
    private final String statusUrl;

    @JsonCreator
    public LiveMigrationTaskCreationResponse(@JsonProperty("taskId") String taskId,
                                             @JsonProperty("statusUrl") String statusUrl)
    {
        this.taskId = Objects.requireNonNull(taskId, "taskId cannot be null");
        this.statusUrl = Objects.requireNonNull(statusUrl, "statusUrl cannot be null");
    }

    @JsonProperty("taskId")
    public String taskId()
    {
        return taskId;
    }

    @JsonProperty("statusUrl")
    public String statusUrl()
    {
        return statusUrl;
    }

    @Override
    public String toString()
    {
        return "LiveMigrationTaskCreationResponse{" +
               "taskId='" + taskId + '\'' +
               ", statusUrl='" + statusUrl + '\'' +
               '}';
    }
}
