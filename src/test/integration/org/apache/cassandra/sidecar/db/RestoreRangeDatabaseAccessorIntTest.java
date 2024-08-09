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

package org.apache.cassandra.sidecar.db;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;
import org.apache.cassandra.testing.SimpleCassandraVersion;

import static org.apache.cassandra.sidecar.restore.RestoreRangeTest.createTestRange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

class RestoreRangeDatabaseAccessorIntTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void testCrudOperations(CassandraTestContext cassandraTestContext)
    {
        assumeThat(cassandraTestContext.version).as("TTL is only supported in Cassandra 5.1")
                                                .isGreaterThanOrEqualTo(SimpleCassandraVersion.create(5, 1, 0));
        waitForSchemaReady(10, TimeUnit.SECONDS);

        RestoreRangeDatabaseAccessor accessor = injector.getInstance(RestoreRangeDatabaseAccessor.class);
        UUID jobId = UUIDs.timeBased();
        assertThat(accessor.findAll(jobId, (short) 0)).isEmpty();

        // create range
        RestoreRange range = createTestRange(0, 10)
                             .unbuild()
                             .jobId(jobId)
                             .bucketId((short) 0)
                             .build();
        accessor.create(range);

        // find the only ranges
        RestoreRange fetchedRange = findAllAndReturnFirstRange(accessor, jobId, 1);
        assertThat(fetchedRange).isEqualTo(range);
        Map<String, RestoreRangeStatus> statusByReplica = new HashMap<>(fetchedRange.statusByReplica());
        assertThat(statusByReplica)
        .hasSize(1)
        .containsEntry("replica1", RestoreRangeStatus.CREATED);

        // find no range from non-existing bucket
        assertThat(accessor.findAll(jobId, (short) 1))
        .describedAs("No ranges in the other bucket")
        .isEmpty();

        // update status
        statusByReplica.put("replica1", RestoreRangeStatus.STAGED);
        accessor.updateStatus(range.unbuild().replicaStatus(statusByReplica).build());

        // read the updated range back
        fetchedRange = findAllAndReturnFirstRange(accessor, jobId, 1);
        assertThat(fetchedRange)
        .describedAs("The updated statusByReplica should not affect equality check")
        .isEqualTo(range);
        statusByReplica = new HashMap<>(fetchedRange.statusByReplica());
        assertThat(statusByReplica)
        .hasSize(1)
        .containsEntry("replica1", RestoreRangeStatus.STAGED);

        // update status with new replica status
        statusByReplica.put("replica2", RestoreRangeStatus.CREATED);
        accessor.updateStatus(range.unbuild().replicaStatus(statusByReplica).build());

        // create another range
        RestoreRange newRange = createTestRange(10, 20)
                                .unbuild()
                                .jobId(jobId)
                                .bucketId((short) 0)
                                .build();
        accessor.create(newRange);

        // read the updated range back; there are 2 ranges now
        fetchedRange = findAllAndReturnFirstRange(accessor, jobId, 2);
        assertThat(fetchedRange)
        .describedAs("The updated statusByReplica should not affect equality check")
        .isEqualTo(range);
        assertThat(fetchedRange.statusByReplica())
        .hasSize(2)
        .containsEntry("replica1", RestoreRangeStatus.STAGED)
        .containsEntry("replica2", RestoreRangeStatus.CREATED);
    }

    private RestoreRange findAllAndReturnFirstRange(RestoreRangeDatabaseAccessor accessor, UUID jobId, int size)
    {
        List<RestoreRange> allRanges = accessor.findAll(jobId, (short) 0);
        assertThat(allRanges).hasSize(size);
        RestoreRange fetchedRange = allRanges.get(0);
        assertThat(fetchedRange.canProduceTask())
        .describedAs("The materialized range cannot produce task")
        .isFalse();
        return fetchedRange;
    }
}
