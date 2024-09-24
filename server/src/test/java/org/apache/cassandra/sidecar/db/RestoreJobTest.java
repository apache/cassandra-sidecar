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

import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.ConsistencyLevel;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.data.SSTableImportOptions;
import org.apache.cassandra.sidecar.common.server.data.DataObjectMappingException;
import org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        return createTestingJob(jobId, status, null);
    }

    public static RestoreJob createTestingJob(UUID jobId,
                                              RestoreJobStatus status,
                                              ConsistencyLevel consistencyLevel) throws DataObjectMappingException
    {
        return createTestingJob(jobId, "ks", status, consistencyLevel, null);
    }

    public static RestoreJob createTestingJob(UUID jobId,
                                              String keyspace,
                                              RestoreJobStatus status,
                                              ConsistencyLevel consistencyLevel) throws DataObjectMappingException
    {
        return createTestingJob(jobId, keyspace, status, consistencyLevel, null);
    }

    public static RestoreJob createTestingJob(UUID jobId,
                                              String keyspace,
                                              RestoreJobStatus status,
                                              ConsistencyLevel consistencyLevel,
                                              String dcName) throws DataObjectMappingException
    {
        RestoreJob.Builder builder = RestoreJob.builder();
        builder.createdAt(RestoreJob.toLocalDate(jobId))
               .keyspace(keyspace)
               .table("table")
               .jobId(jobId)
               .jobStatus(status)
               .consistencyLevel(consistencyLevel)
               .localDatacenter(dcName)
               .expireAt(new Date(System.currentTimeMillis() + 10000L));
        return builder.build();
    }

    public static RestoreJob createUpdatedJob(UUID jobId, String jobAgent,
                                              RestoreJobStatus status,
                                              RestoreJobSecrets secrets,
                                              Date expireAt)
    throws DataObjectMappingException
    {
        RestoreJob.Builder builder = RestoreJob.builder();
        builder.createdAt(RestoreJob.toLocalDate(jobId))
               .jobId(jobId).jobAgent(jobAgent)
               .jobStatus(status)
               .jobSecrets(secrets)
               .expireAt(expireAt);
        return builder.build();
    }

    @Test
    void testDefaultImportOptionsWhenNotSetInDb()
    {
        RestoreJob job = createNewTestingJob(UUIDs.timeBased());
        assertThat(job.importOptions).isEqualTo(SSTableImportOptions.defaults());
    }

    @Test
    void testExpectedNextRangeStatus()
    {
        UUID jobId = UUIDs.timeBased();
        for (RestoreJobStatus status : RestoreJobStatus.values())
        {
            RestoreJob job = createTestingJob(jobId, status);
            if (status == RestoreJobStatus.CREATED)
            {
                assertThatThrownBy(job::expectedNextRangeStatus)
                .hasMessage("Cannot check progress for restore job in CREATED status. jobId: " + jobId);
            }
            else if (status == RestoreJobStatus.STAGE_READY)
            {
                assertThat(job.expectedNextRangeStatus())
                .describedAs("Expecting the ranges in STAGE_READY job to enter STAGED")
                .isEqualTo(RestoreRangeStatus.STAGED);
            }
            else if (status == RestoreJobStatus.STAGED)
            {
                assertThat(job.expectedNextRangeStatus())
                .describedAs("Expecting the ranges in STAGED job to remain STAGED")
                .isEqualTo(RestoreRangeStatus.STAGED);
            }
            else
            {
                assertThat(job.expectedNextRangeStatus())
                .describedAs("Expecting the ranges in IMPORT_READY or SUCCEEDED or FAILED or ABORTED job to enter SUCCEEDED")
                .isEqualTo(RestoreRangeStatus.SUCCEEDED);
            }
        }
    }

    @Test
    void testCreateLocalConsistencyLevelJobWithoutLocalDcFails()
    {
        UUID jobId = UUIDs.timeBased();
        for (ConsistencyLevel localCL : Arrays.asList(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE))
        {
            assertThatThrownBy(() -> createTestingJob(jobId, RestoreJobStatus.CREATED, localCL))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("When local consistency level is used, localDatacenter must also present");
        }
    }

    @Test
    void testCreateSidecarManagedJobs()
    {
        UUID jobId = UUIDs.timeBased();
        String dcName = "dc1";
        for (ConsistencyLevel cl : ConsistencyLevel.values())
        {
            RestoreJob job = createTestingJob(jobId, "ks", RestoreJobStatus.CREATED, cl, dcName);
            assertThat(job.isManagedBySidecar()).isTrue();
        }
    }
}
