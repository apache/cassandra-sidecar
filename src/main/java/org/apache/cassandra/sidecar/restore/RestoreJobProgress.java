/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.restore;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.cluster.ConsistencyVerifier;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.response.data.RestoreJobProgressResponsePayload;
import org.apache.cassandra.sidecar.common.response.data.RestoreJobRange;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.jetbrains.annotations.Nullable;

/**
 * Represents the progress of a {@link RestoreJob}
 */
public class RestoreJobProgress
{
    private final RestoreJob restoreJob;
    private final ConsistencyVerifier.Result overallStatus;
    private final List<RestoreRange> failedRanges;
    private final List<RestoreRange> pendingRanges;
    private final List<RestoreRange> succeededRanges;

    private RestoreJobProgress(Builder builder)
    {
        this.restoreJob = builder.restoreJob;
        this.overallStatus = builder.overallStatus;
        this.failedRanges = builder.failedRanges;
        this.pendingRanges = builder.pendingRanges;
        this.succeededRanges = builder.succeededRanges;
    }

    public RestoreJobProgressResponsePayload toResponsePayload()
    {
        return RestoreJobProgressResponsePayload.builder()
                                                .withSucceededRanges(toRestoreJobRange(succeededRanges))
                                                .withFailedRanges(toRestoreJobRange(failedRanges))
                                                .withPendingRanges(toRestoreJobRange(pendingRanges))
                                                .withMessage(buildMessage(restoreJob.status))
                                                .withJobSummary(restoreJob.createdAt.toString(),
                                                                restoreJob.jobId,
                                                                restoreJob.jobAgent,
                                                                restoreJob.keyspaceName,
                                                                restoreJob.tableName,
                                                                restoreJob.status.name())
                                                .build();
    }

    @Nullable
    private List<RestoreJobRange> toRestoreJobRange(List<RestoreRange> ranges)
    {
        // return null to exclude the field from response payload
        if (ranges == null)
        {
            return null;
        }

        return ranges.stream()
                     .map(RestoreRange::toRestoreJobRange)
                     .collect(Collectors.toList());
    }

    private String buildMessage(RestoreJobStatus jobStatus)
    {
        String message;
        switch (overallStatus)
        {
            case SATISFIED:
                message = "All ranges have succeeded.";
                break;
            case FAILED:
                message = "One or more ranges have failed.";
                break;
            default:
                message = "One or more ranges are in progress. None of the ranges fail.";
                break;
        }

        return message + " Current job status: " + jobStatus;
    }

    static class Builder implements DataObjectBuilder<Builder, RestoreJobProgress>
    {
        private final RestoreJob restoreJob;
        private ConsistencyVerifier.Result overallStatus;
        private List<RestoreRange> failedRanges;
        private List<RestoreRange> pendingRanges;
        private List<RestoreRange> succeededRanges;

        public Builder(RestoreJob restoreJob)
        {
            this.restoreJob = restoreJob;
        }

        public Builder withOverallStatus(ConsistencyVerifier.Result overallStatus)
        {
            return update(b -> b.overallStatus = overallStatus);
        }

        public Builder addFailedRange(RestoreRange range)
        {
            failedRanges = createIfNull(failedRanges);
            return update(b -> b.failedRanges.add(range));
        }

        public Builder addPendingRange(RestoreRange range)
        {
            pendingRanges = createIfNull(pendingRanges);
            return update(b -> b.pendingRanges.add(range));
        }

        public Builder addSucceededRange(RestoreRange range)
        {
            succeededRanges = createIfNull(succeededRanges);
            return update(b -> b.succeededRanges.add(range));
        }

        private List<RestoreRange> createIfNull(List<RestoreRange> list)
        {
            return list == null ? new ArrayList<>() : list;
        }

        @Override
        public Builder self()
        {
            return this;
        }

        @Override
        public RestoreJobProgress build()
        {
            return new RestoreJobProgress(self());
        }
    }
}
