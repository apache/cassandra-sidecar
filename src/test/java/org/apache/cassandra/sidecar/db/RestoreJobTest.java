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

import java.util.Date;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.data.SSTableImportOptions;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test restore job setup options
 */
public class RestoreJobTest
{
    public static RestoreJob createNewTestingJob(UUID jobId) throws DataObjectMappingException
    {
        return createTestingJob(jobId, RestoreJobStatus.CREATED);
    }

    public static RestoreJob createTestingJob(UUID jobId, RestoreJobStatus status) throws DataObjectMappingException
    {
        RestoreJob.Builder builder = RestoreJob.builder();
        builder.createdAt(RestoreJob.toLocalDate(jobId))
               .keyspace("ks")
               .table("table")
               .jobId(jobId)
               .jobStatus(status)
               .expireAt(new Date(System.currentTimeMillis() + 10000L));
        return builder.build();
    }

    @Test
    void testDefaultImportOptionsWhenNotSetInDb()
    {
        RestoreJob job = createNewTestingJob(UUIDs.timeBased());
        assertThat(job.importOptions).isEqualTo(SSTableImportOptions.defaults());
    }
}
