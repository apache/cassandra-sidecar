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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.utils.UUIDs;

import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.common.data.OperationalJobStatus;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.job.storage.OperationalJobRecord;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.TimeProvider;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test for {@link StorageBackedLocalJobCoordinator}
 */
class StorageBackedLocalJobCoordinatorIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void testCoordinatorPollCycleWithStorageProvider() throws InterruptedException
    {
        waitForSchemaReady(10, TimeUnit.SECONDS);

        StorageProvider storageProvider = injector.getInstance(StorageProvider.class);
        storageProvider.initialize();
        SidecarConfiguration sidecarConfig = injector.getInstance(SidecarConfiguration.class);

        UUID operationId = UUIDs.timeBased();
        UUID node1 = UUID.randomUUID();
        UUID node2 = UUID.randomUUID();

        OperationalJobRecord jobRecord = OperationalJobRecord.builder()
                                                             .jobId(operationId)
                                                             .operationType(OperationType.DRAIN)
                                                             .status(OperationalJobStatus.RUNNING)
                                                             .nodeExecutionOrder(Arrays.asList(
                                                             Arrays.asList(node1),
                                                             Arrays.asList(node2)
                                                             ))
                                                             .build();
        storageProvider.persistJob(jobRecord);
        storageProvider.trySetActiveOperation(OperationType.DRAIN, operationId);
        storageProvider.updateNodeStatus(operationId, node1, OperationalJobStatus.CREATED);
        storageProvider.updateNodeStatus(operationId, node2, OperationalJobStatus.CREATED);

        InstanceMetadataFetcher mockFetcher = mockFetcherForNode(node2, "127.0.0.2");

        LocalJobFactory testFactory = (opId, nodeId, host, opType) -> new LocalJob(opId, nodeId, host, opType)
        {
            @Override
            protected void executeInternal()
            {
            }
        };

        OperationalJobCoordinator opCoordinator = new StorageBackedOperationalJobCoordinator(storageProvider);
        ExecutorPools executorPools = injector.getInstance(ExecutorPools.class);
        LocalJobManager localJobManager = new LocalJobManager(executorPools);

        StorageBackedLocalJobCoordinator coordinator = new StorageBackedLocalJobCoordinator(executorPools,
                                                                                            opCoordinator,
                                                                                            storageProvider,
                                                                                            mockFetcher,
                                                                                            localJobManager,
                                                                                            sidecarConfig,
                                                                                            testFactory,
                                                                                            TimeProvider.DEFAULT_TIME_PROVIDER);

        executeAndWait(coordinator);

        OperationalJobStatus node2Status = storageProvider.getNodeStatus(operationId, node2);
        assertThat(node2Status)
        .describedAs("Node2 should remain CREATED while predecessor is not complete")
        .isEqualTo(OperationalJobStatus.CREATED);

        storageProvider.updateNodeStatus(operationId, node1, OperationalJobStatus.SUCCEEDED);
        executeAndWait(coordinator);

        OperationalJobStatus node2SubmittedStatus = storageProvider.getNodeStatus(operationId, node2);
        assertThat(node2SubmittedStatus)
        .describedAs("Node2 should be RUNNING or SUCCEEDED after predecessor completed")
        .isIn(OperationalJobStatus.RUNNING, OperationalJobStatus.SUCCEEDED);

        loopAssert(5, () -> {
            OperationalJobStatus status = storageProvider.getNodeStatus(operationId, node2);
            assertThat(status)
            .describedAs("Node2 should eventually reach SUCCEEDED")
            .isEqualTo(OperationalJobStatus.SUCCEEDED);
        });

        executeAndWait(coordinator);

        loopAssert(5, () -> {
            OperationalJobRecord finalRecord = storageProvider.findJob(operationId);
            assertThat(finalRecord).isNotNull();
            assertThat(finalRecord.status())
            .describedAs("Job should be finalized as SUCCEEDED when all nodes complete")
            .isEqualTo(OperationalJobStatus.SUCCEEDED);
        });

        assertThat(opCoordinator.getActiveOperation(OperationType.DRAIN))
        .describedAs("Active operation should be cleared after job completion")
        .isNull();
    }

    private InstanceMetadataFetcher mockFetcherForNode(UUID nodeId, String host)
    {
        InstanceMetadataFetcher fetcher = mock(InstanceMetadataFetcher.class);
        InstanceMetadata instance = mock(InstanceMetadata.class);
        CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);
        NodeSettings nodeSettings = NodeSettings.builder().hostId(nodeId).build();

        when(instance.host()).thenReturn(host);
        when(instance.delegate()).thenReturn(delegate);
        when(delegate.nodeSettings()).thenReturn(nodeSettings);
        when(fetcher.allLocalInstances()).thenReturn(Collections.singletonList(instance));
        return fetcher;
    }

    private void executeAndWait(StorageBackedLocalJobCoordinator coordinator)
    {
        CountDownLatch latch = new CountDownLatch(1);
        Promise<Void> promise = Promise.promise();
        promise.future().onComplete(ar -> latch.countDown());
        coordinator.execute(promise);
        try
        {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
