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

package org.apache.cassandra.sidecar.common.utils;

/**
 * Holder class for the results of a job execution. Captures the job status and reason
 * as a result of performing the downstream operation.
 */
public class JobResult
{
    /**
     * Encapsulates the states of the job lifecycle. All new jobs are in Pending state.
     */
    public enum JobStatus
    { Pending, Running, Completed, Failed }

    private JobStatus status;
    private String reason;

    public JobResult(JobStatus status)
    {
        this.status = status;
        this.reason = "";
    }

    public JobResult(JobStatus status, String reason)
    {
        this.status = status;
        this.reason = reason;
    }

    /**
     * Fetch the {@code JobStatus} of the job execution result.
     *
     * @return status of the job execution
     */
    public JobStatus status()
    {
        return status;
    }

    /**
     * Fetch the failure reason of the job execution
     *
     * @return failure reason of the job execution
     */
    public String reason()
    {
        return reason;
    }
}

