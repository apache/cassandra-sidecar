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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;

import static org.apache.cassandra.sidecar.db.RestoreJobTest.createNewTestingJob;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RestoreJobStatusCheckerTest
{
    private final SidecarConfiguration sidecarConfig = mock(SidecarConfiguration.class);
    private final RestoreJobConfiguration restoreJobConfig = mock(RestoreJobConfiguration.class);
    private final SidecarSchema sidecarSchema = mock(SidecarSchema.class);
    private final RestoreJobDatabaseAccessor jobAccessor = mock(RestoreJobDatabaseAccessor.class);
    private final RestoreJobDiscoverer discoverer = mock(RestoreJobDiscoverer.class);
    private RestoreJobStatusChecker checker;

    @BeforeEach
    void setup()
    {
        when(sidecarConfig.restoreJobConfiguration()).thenReturn(restoreJobConfig);
        when(restoreJobConfig.jobDiscoveryStatusCheckInterval())
        .thenReturn(MillisecondBoundConfiguration.parse("1s"));
        checker = new RestoreJobStatusChecker(sidecarConfig, sidecarSchema, jobAccessor, discoverer);
    }

    @Test
    void testDelayReadsFromConfig()
    {
        assertThat(checker.delay()).isEqualTo(MillisecondBoundConfiguration.parse("1s"));
    }

    @Test
    void testSkipWhenSchemaNotInitialized()
    {
        when(sidecarSchema.isInitialized()).thenReturn(false);
        assertThat(checker.scheduleDecision()).isEqualTo(ScheduleDecision.SKIP);
    }

    @Test
    void testSkipWhenNoInflightJobs()
    {
        when(sidecarSchema.isInitialized()).thenReturn(true);
        when(discoverer.inflightJobIds()).thenReturn(Collections.emptySet());
        assertThat(checker.scheduleDecision()).isEqualTo(ScheduleDecision.SKIP);
    }

    @Test
    void testExecuteWhenInflightJobsPresent()
    {
        when(sidecarSchema.isInitialized()).thenReturn(true);
        when(discoverer.inflightJobIds()).thenReturn(Collections.singleton(UUIDs.timeBased()));
        assertThat(checker.scheduleDecision()).isEqualTo(ScheduleDecision.EXECUTE);
    }

    @Test
    void testExecuteReadsEachInflightJobAndDelegatesTransition()
    {
        UUID jobId1 = UUIDs.timeBased();
        UUID jobId2 = UUIDs.timeBased();
        Set<UUID> ids = new HashSet<>();
        ids.add(jobId1);
        ids.add(jobId2);
        when(discoverer.inflightJobIds()).thenReturn(ids);
        RestoreJob job1 = createNewTestingJob(jobId1);
        RestoreJob job2 = createNewTestingJob(jobId2);
        when(jobAccessor.find(jobId1)).thenReturn(job1);
        when(jobAccessor.find(jobId2)).thenReturn(job2);

        Promise<Void> promise = Promise.promise();
        checker.execute(promise);

        verify(jobAccessor).find(jobId1);
        verify(jobAccessor).find(jobId2);
        verify(discoverer).handleStatusTransition(job1);
        verify(discoverer).handleStatusTransition(job2);
        assertThat(promise.future().succeeded()).isTrue();
    }

    @Test
    void testExecuteSkipsMissingJobs()
    {
        UUID jobId = UUIDs.timeBased();
        when(discoverer.inflightJobIds()).thenReturn(Collections.singleton(jobId));
        when(jobAccessor.find(jobId)).thenReturn(null);

        Promise<Void> promise = Promise.promise();
        checker.execute(promise);

        verify(jobAccessor).find(jobId);
        verify(discoverer, never()).handleStatusTransition(any());
        assertThat(promise.future().succeeded()).isTrue();
    }

    @Test
    void testExecuteContinuesAfterPerJobException()
    {
        UUID jobId1 = UUIDs.timeBased();
        UUID jobId2 = UUIDs.timeBased();
        Set<UUID> ids = new HashSet<>();
        ids.add(jobId1);
        ids.add(jobId2);
        when(discoverer.inflightJobIds()).thenReturn(ids);
        when(jobAccessor.find(jobId1)).thenThrow(new RuntimeException("db error"));
        RestoreJob job2 = createNewTestingJob(jobId2);
        when(jobAccessor.find(jobId2)).thenReturn(job2);

        Promise<Void> promise = Promise.promise();
        checker.execute(promise);

        verify(discoverer).handleStatusTransition(job2);
        assertThat(promise.future().succeeded()).isTrue();
    }
}
