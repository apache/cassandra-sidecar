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

package org.apache.cassandra.sidecar.restore;

import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.ConsistencyVerificationResult;
import org.apache.cassandra.sidecar.common.response.data.RestoreJobProgressResponsePayload;
import org.apache.cassandra.sidecar.common.response.data.RestoreJobSummaryResponsePayload;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobTest;

import static org.assertj.core.api.Assertions.assertThat;

abstract class BaseRestoreJobProgressCollectorTest
{
    private final RestoreJob restoreJob = RestoreJobTest.createNewTestingJob(UUIDs.timeBased());
    protected final RestoreJobProgressCollector collector = createCollector(restoreJob);

    protected abstract RestoreJobProgressCollector createCollector(RestoreJob restoreJob);

    @Test
    void testCollectNothing()
    {
        RestoreJobProgressResponsePayload payload = collector.toRestoreJobProgress().toResponsePayload();
        assertThat(payload.message()).isEqualTo("All ranges have succeeded. Current job status: CREATED");
        assertJobSummary(payload.summary());
        assertThat(payload.failedRanges()).isNull();
        assertThat(payload.abortedRanges()).isNull();
        assertThat(payload.pendingRanges()).isNull();
        assertThat(payload.succeededRanges()).isNull();
    }

    protected void createRangesAndCollect(int count, ConsistencyVerificationResult result)
    {
        for (int i = 0; i < count; i++)
        {
            if (collector.canCollectMore())
            {
                collector.collect(RestoreRangeTest.createTestRange(restoreJob, Paths.get("."), true), result);
            }
        }
    }

    protected void assertJobSummary(RestoreJobSummaryResponsePayload summary)
    {
        assertThat(summary.jobId()).isEqualTo(restoreJob.jobId);
        assertThat(summary.keyspace()).isEqualTo(restoreJob.keyspaceName);
        assertThat(summary.table()).isEqualTo(restoreJob.tableName);
        assertThat(summary.jobAgent()).isEqualTo(restoreJob.jobAgent);
        assertThat(summary.status()).isEqualTo(restoreJob.status.toString());
        assertThat(summary.secrets()).isNull();
    }
}
