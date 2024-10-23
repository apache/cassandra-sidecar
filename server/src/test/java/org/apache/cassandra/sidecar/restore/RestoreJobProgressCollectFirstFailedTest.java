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

import org.apache.cassandra.sidecar.common.data.ConsistencyVerificationResult;
import org.apache.cassandra.sidecar.common.data.RestoreJobProgressFetchPolicy;
import org.apache.cassandra.sidecar.common.response.data.RestoreJobProgressResponsePayload;
import org.apache.cassandra.sidecar.db.RestoreJob;

import static org.assertj.core.api.Assertions.assertThat;

class RestoreJobProgressCollectFirstFailedTest extends BaseRestoreJobProgressCollectorTest
{
    @Test
    void testContinueCollectOnSatisfied()
    {
        assertThat(collector.canCollectMore()).isTrue();
        createRangesAndCollect(1, ConsistencyVerificationResult.SATISFIED);
        assertThat(collector.canCollectMore()).isTrue();
    }

    @Test
    void testStopCollectionOnFirstFailed()
    {
        assertThat(collector.canCollectMore()).isTrue();
        createRangesAndCollect(1, ConsistencyVerificationResult.PENDING);
        assertThat(collector.canCollectMore()).isTrue();
        createRangesAndCollect(1, ConsistencyVerificationResult.FAILED);
        assertThat(collector.canCollectMore()).isFalse();
        createRangesAndCollect(1, ConsistencyVerificationResult.PENDING);
        assertThat(collector.canCollectMore()).isFalse();
    }

    @Test
    void testCollectMixed()
    {
        // The collector with FIRST_FAILED policy skips SATISFIED and PENDING ranges and stops after seeing the first FAILED
        createRangesAndCollect(3, ConsistencyVerificationResult.SATISFIED);
        createRangesAndCollect(1, ConsistencyVerificationResult.PENDING);
        createRangesAndCollect(1, ConsistencyVerificationResult.FAILED);
        // collector should stop from collecting
        createRangesAndCollect(5, ConsistencyVerificationResult.PENDING); // not being collected
        RestoreJobProgressResponsePayload payload = collector.toRestoreJobProgress().toResponsePayload();
        assertThat(payload.message()).isEqualTo("One or more ranges have failed. Current job status: CREATED");
        assertJobSummary(payload.summary());
        assertThat(payload.failedRanges()).hasSize(1);
        assertThat(payload.abortedRanges()).isNull();
        assertThat(payload.pendingRanges()).isNull();
        assertThat(payload.succeededRanges()).isNull();
    }

    @Override
    protected RestoreJobProgressCollector createCollector(RestoreJob restoreJob)
    {
        return RestoreJobProgressCollectors.create(restoreJob,
                                                   RestoreJobProgressFetchPolicy.FIRST_FAILED);
    }
}
