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

import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThatNoException;

class ReprepareStatementsOnReconnectionIntTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void testStatementsAreRepreparedOnReconnection()
    {
        RestoreSliceDatabaseAccessor accessor = injector.getInstance(RestoreSliceDatabaseAccessor.class);
        RestoreJob testJob = RestoreJobTest.createNewTestingJob(UUIDs.timeBased());
        TokenRange range = new TokenRange(0, 10);

        waitForSchemaReady(10, TimeUnit.SECONDS);
        assertThatNoException().isThrownBy(() -> accessor.selectByJobByBucketByTokenRange(testJob, (short) 0, range));

        closeNativeThenReconnect();

        waitForSchemaReady(10, TimeUnit.SECONDS);
        assertThatNoException().isThrownBy(() -> accessor.selectByJobByBucketByTokenRange(testJob, (short) 0, range));
    }
}
