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

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.cluster.ConsistencyVerifier;
import org.apache.cassandra.sidecar.common.data.RestoreJobProgressFetchPolicy;
import org.apache.cassandra.sidecar.common.response.data.RestoreJobProgressResponsePayload;
import org.apache.cassandra.sidecar.db.RestoreJob;

import static org.assertj.core.api.Assertions.assertThat;

class RestoreJobProgressCollectAllTest extends BaseRestoreJobProgressCollectorTest
{
    @Test
    void testAlwaysCollectMore()
    {
        assertThat(collector.canCollectMore()).isTrue();
        for (ConsistencyVerifier.Result result : ConsistencyVerifier.Result.values())
        {
            createRangesAndCollect(1, result);
            assertThat(collector.canCollectMore()).isTrue();
        }
    }

    @Test
    void testCollectAllSatisfiedRanges()
    {
        int succeededRanges = 10;
        createRangesAndCollect(succeededRanges, ConsistencyVerifier.Result.SATISFIED);
        RestoreJobProgressResponsePayload payload = collector.toRestoreJobProgress().toResponsePayload();
        assertThat(payload.message()).isEqualTo("All ranges have succeeded. Current job status: CREATED");
        assertJobSummary(payload.summary());
        assertThat(payload.failedRanges()).isNull();
        assertThat(payload.abortedRanges()).isNull();
        assertThat(payload.pendingRanges()).isNull();
        assertThat(payload.succeededRanges()).hasSize(succeededRanges);
    }

    @Test
    void testCollectAllFailed()
    {
        int failedRanges = 10;
        createRangesAndCollect(failedRanges, ConsistencyVerifier.Result.FAILED);
        RestoreJobProgressResponsePayload payload = collector.toRestoreJobProgress().toResponsePayload();
        assertThat(payload.message()).isEqualTo("One or more ranges have failed. Current job status: CREATED");
        assertJobSummary(payload.summary());
        assertThat(payload.failedRanges()).hasSize(failedRanges);
        assertThat(payload.abortedRanges()).isNull();
        assertThat(payload.pendingRanges()).isNull();
        assertThat(payload.succeededRanges()).isNull();
    }

    @Test
    void testCollectAllPending()
    {
        int pendingRanges = 10;
        createRangesAndCollect(pendingRanges, ConsistencyVerifier.Result.PENDING);
        RestoreJobProgressResponsePayload payload = collector.toRestoreJobProgress().toResponsePayload();
        assertThat(payload.message()).isEqualTo("One or more ranges are in progress. None of the ranges fail. Current job status: CREATED");
        assertJobSummary(payload.summary());
        assertThat(payload.failedRanges()).isNull();
        assertThat(payload.abortedRanges()).isNull();
        assertThat(payload.pendingRanges()).hasSize(pendingRanges);
        assertThat(payload.succeededRanges()).isNull();
    }

    @Test
    void testCollectAllMixed()
    {
        int pendingRanges = 3;
        int failedRanges = 1;
        int satisfiedRanges = 2;
        createRangesAndCollect(pendingRanges, ConsistencyVerifier.Result.PENDING);
        createRangesAndCollect(failedRanges, ConsistencyVerifier.Result.FAILED);
        createRangesAndCollect(satisfiedRanges, ConsistencyVerifier.Result.SATISFIED);
        RestoreJobProgressResponsePayload payload = collector.toRestoreJobProgress().toResponsePayload();
        assertThat(payload.message()).isEqualTo("One or more ranges have failed. Current job status: CREATED");
        assertJobSummary(payload.summary());
        assertThat(payload.failedRanges()).hasSize(failedRanges);
        assertThat(payload.pendingRanges()).hasSize(pendingRanges);
        assertThat(payload.abortedRanges()).isNull();
        assertThat(payload.succeededRanges()).hasSize(satisfiedRanges);
    }

    @Test
    void testCollectAllMixedNoFailed()
    {
        int pendingRanges = 3;
        int satisfiedRanges = 2;
        createRangesAndCollect(pendingRanges, ConsistencyVerifier.Result.PENDING);
        createRangesAndCollect(satisfiedRanges, ConsistencyVerifier.Result.SATISFIED);
        RestoreJobProgressResponsePayload payload = collector.toRestoreJobProgress().toResponsePayload();
        assertThat(payload.message()).isEqualTo("One or more ranges are in progress. None of the ranges fail. Current job status: CREATED");
        assertJobSummary(payload.summary());
        assertThat(payload.failedRanges()).isNull();
        assertThat(payload.pendingRanges()).hasSize(pendingRanges);
        assertThat(payload.abortedRanges()).isNull();
        assertThat(payload.succeededRanges()).hasSize(satisfiedRanges);
    }

    @Override
    protected RestoreJobProgressCollector createCollector(RestoreJob restoreJob)
    {
        return RestoreJobProgressCollectors.create(restoreJob, RestoreJobProgressFetchPolicy.ALL);
    }
}
