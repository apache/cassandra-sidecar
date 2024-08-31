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

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

class RestoreSliceDatabaseAccessorIntTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void testCrudOperations()
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        RestoreSliceDatabaseAccessor accessor = injector.getInstance(RestoreSliceDatabaseAccessor.class);
        RestoreJob testJob = RestoreJobTest.createNewTestingJob(UUIDs.timeBased());
        List<RestoreSlice> fetchedSlices = accessor.selectByJobByBucketByTokenRange(testJob, (short) 0, new TokenRange(0, 10));
        assertThat(fetchedSlices).isEmpty();;

        RestoreSlice slice0to10 = RestoreSliceTest.createTestingSlice(testJob, "sliceId-1", 0, 10);
        accessor.create(slice0to10);
        fetchedSlices = accessor.selectByJobByBucketByTokenRange(testJob, (short) 0, new TokenRange(5, 15));
        assertThat(fetchedSlices).hasSize(1)
                                 .contains(slice0to10);

        RestoreSlice slice10to20 = RestoreSliceTest.createTestingSlice(testJob, "sliceId-2", 10, 20);
        accessor.create(slice10to20);
        RestoreSlice slice20to30 = RestoreSliceTest.createTestingSlice(testJob, "sliceId-3", 20, 30);
        accessor.create(slice20to30);
        fetchedSlices = accessor.selectByJobByBucketByTokenRange(testJob, (short) 0, new TokenRange(5, 15));
        assertThat(fetchedSlices).hasSize(2)
                                 .contains(slice0to10, slice10to20)
                                 .doesNotContain(slice20to30);
    }
}
