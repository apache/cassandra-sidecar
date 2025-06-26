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

package org.apache.cassandra.sidecar.job;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.request.data.RepairPayload;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.RepairConfiguration;
import org.apache.cassandra.sidecar.config.yaml.RepairConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.handlers.data.RepairRequestParam;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Tests to validate job to run repairs
 */
public class RepairJobTest
{
    protected Vertx vertx;
    protected ExecutorPools executorPool;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        executorPool = new ExecutorPools(vertx, new ServiceConfigurationImpl());
    }

    @AfterEach
    void cleanup()
    {
        TestResourceReaper.create().with(vertx).with(executorPool).close();
    }

    @Test
    void testRepairJob()
    {
        OperationalJobTracker tracker = new OperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker, executorPool);

        StorageOperations storageOperations = mock(StorageOperations.class);
        when(storageOperations.getParentRepairStatus(anyInt())).thenReturn(Arrays.asList(RepairJob.ParentRepairStatus.COMPLETED.name()));
        when(storageOperations.repair(any(), any())).thenReturn(1);

        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of("testtable"))
                                             .build();
        RepairRequestParam repairParams = new RepairRequestParam(new Name("testkeyspace"), payload);

        RepairConfiguration config = new RepairConfigurationImpl(2_000, 1_000);
        RepairJob testJob = new RepairJob(vertx, config, UUIDs.timeBased(), storageOperations, repairParams);
        manager.trySubmitJob(testJob);
        testJob.execute(Promise.promise());
        assertThat(testJob.asyncResult().isComplete()).isTrue();
        assertThat(testJob.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(tracker.get(testJob.jobId())).isNotNull();
    }

    @Test
    void testLongRunningRepairJob()
    {
        OperationalJobTracker tracker = new OperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker, executorPool);

        StorageOperations storageOperations = mock(StorageOperations.class);
        when(storageOperations.getParentRepairStatus(anyInt()))
        .thenReturn(Collections.singletonList(RepairJob.ParentRepairStatus.IN_PROGRESS.name()))
        .thenReturn(Collections.singletonList(RepairJob.ParentRepairStatus.COMPLETED.name()));
        when(storageOperations.repair(any(), any())).thenReturn(1);
        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of("testtable"))
                                             .build();
        RepairRequestParam repairParams = new RepairRequestParam(new Name("testkeyspace"), payload);

        RepairConfiguration config = new RepairConfigurationImpl(20_000, 5_000);
        RepairJob testJob = new RepairJob(vertx, config, UUIDs.timeBased(), storageOperations, repairParams);
        manager.trySubmitJob(testJob);
        testJob.execute(Promise.promise());
        assertThat(testJob.asyncResult().isComplete()).isTrue();
        assertThat(testJob.status()).isEqualTo(OperationalJobStatus.SUCCEEDED);
        assertThat(tracker.get(testJob.jobId())).isNotNull();
        Mockito.verify(storageOperations, times(2)).getParentRepairStatus(anyInt());
    }

    @Test
    void testLongRunningRepairJobTimeout()
    {
        OperationalJobTracker tracker = new OperationalJobTracker(4);
        OperationalJobManager manager = new OperationalJobManager(tracker, executorPool);

        StorageOperations storageOperations = mock(StorageOperations.class);
        when(storageOperations.getParentRepairStatus(anyInt())).thenReturn(Arrays.asList(RepairJob.ParentRepairStatus.IN_PROGRESS.name()));
        doAnswer(AdditionalAnswers.answersWithDelay(6000, invocation -> 1))
        .when(storageOperations).repair(any(), any());

        RepairPayload payload = RepairPayload.builder()
                                             .isPrimaryRange(true)
                                             .tables(List.of("testtable"))
                                             .build();
        RepairRequestParam repairParams = new RepairRequestParam(new Name("testkeyspace"), payload);

        RepairConfiguration config = new RepairConfigurationImpl(2_000, 1_000);
        RepairJob testJob = new RepairJob(vertx, config, UUIDs.timeBased(), storageOperations, repairParams);
        manager.trySubmitJob(testJob);
        testJob.execute(Promise.promise());
        assertThat(testJob.asyncResult().isComplete()).isTrue();
        assertThat(testJob.status()).isEqualTo(OperationalJobStatus.FAILED);
        assertThat(tracker.get(testJob.jobId())).isNotNull();
    }
}
