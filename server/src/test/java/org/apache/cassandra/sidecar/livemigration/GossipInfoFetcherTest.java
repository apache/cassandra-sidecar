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

package org.apache.cassandra.sidecar.livemigration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.vertx.core.Future;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.GossipInfoResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("SameParameterValue")
class GossipInfoFetcherTest
{
    @Test
    void testSuccessOnFirstBatch() throws InterruptedException
    {
        SidecarClient mockClient = mock(SidecarClient.class);
        InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
        List<InstanceMetadata> instances = createMockInstances(2);
        when(mockInstancesMetadata.instances()).thenReturn(instances);

        GossipInfoResponse mockResponse = new GossipInfoResponse();
        when(mockClient.gossipInfo(any(SidecarInstance.class)))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

        GossipInfoFetcher fetcher = new GossipInfoFetcher(mockClient, mockInstancesMetadata, 9043, 5, 5);
        Future<GossipInfoResponse> result = fetcher.fetchGossipInfo();

        awaitForFuture(result);

        assertThat(result.succeeded()).isTrue();
        assertThat(result.result()).isEqualTo(mockResponse);
    }

    @Test
    void testSuccessOnSecondBatchAfterFirstFails() throws InterruptedException
    {
        SidecarClient mockClient = mock(SidecarClient.class);
        InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);

        // Create 10 instances - first 5 will fail, 6th will succeed
        List<InstanceMetadata> instances = createMockInstances(6);
        when(mockInstancesMetadata.instances()).thenReturn(instances);

        GossipInfoResponse mockResponse = new GossipInfoResponse();

        // First 5 instances fail, 6th succeeds
        when(mockClient.gossipInfo(any(SidecarInstance.class)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

        GossipInfoFetcher fetcher = spy(new GossipInfoFetcher(mockClient, mockInstancesMetadata, 9043, 5, 5));
        Future<GossipInfoResponse> result = fetcher.fetchGossipInfo();

        awaitForFuture(result);

        assertThat(result.succeeded()).isTrue();
        assertThat(result.result()).isEqualTo(mockResponse);
        verify(fetcher, times(2)).fetchGossipFromBatch(anyList(), anyInt(), anyInt(), anyInt(), anyInt());
    }

    @Test
    void testFailureWhenAllInstancesFail() throws InterruptedException
    {
        SidecarClient mockClient = mock(SidecarClient.class);
        InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
        List<InstanceMetadata> instances = createMockInstances(2);
        when(mockInstancesMetadata.instances()).thenReturn(instances);
        when(mockClient.gossipInfo(any(SidecarInstance.class)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")));

        GossipInfoFetcher fetcher = new GossipInfoFetcher(mockClient, mockInstancesMetadata, 9043, 5, 5);
        Future<GossipInfoResponse> result = fetcher.fetchGossipInfo();

        awaitForFuture(result);

        assertThat(result.failed()).isTrue();
        assertThat(result.cause().getMessage()).contains("Failed to fetch gossip after trying all 2 instances");
    }

    @Test
    void testFailureWhenNoInstancesAvailable() throws InterruptedException
    {
        SidecarClient mockClient = mock(SidecarClient.class);
        InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
        when(mockInstancesMetadata.instances()).thenReturn(List.of());

        GossipInfoFetcher fetcher = new GossipInfoFetcher(mockClient, mockInstancesMetadata, 9043, 5, 5);
        Future<GossipInfoResponse> result = fetcher.fetchGossipInfo();

        awaitForFuture(result);

        assertThat(result.failed()).isTrue();
        assertThat(result.cause().getMessage()).isEqualTo("No instances available for gossip fetch");
    }

    @Test
    void testPartialBatchSuccess() throws InterruptedException
    {
        SidecarClient mockClient = mock(SidecarClient.class);
        InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);

        List<InstanceMetadata> instances = createMockInstances(3);
        when(mockInstancesMetadata.instances()).thenReturn(instances);

        GossipInfoResponse mockResponse = new GossipInfoResponse();

        // First instance fails, second succeeds
        when(mockClient.gossipInfo(any(SidecarInstance.class)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

        GossipInfoFetcher fetcher = spy(new GossipInfoFetcher(mockClient, mockInstancesMetadata, 9043, 5, 5));
        Future<GossipInfoResponse> result = fetcher.fetchGossipInfo();

        awaitForFuture(result);

        assertThat(result.succeeded()).isTrue();
        assertThat(result.result()).isEqualTo(mockResponse);
        verify(fetcher, times(1))
        .fetchGossipFromBatch(anyList(), anyInt(), anyInt(), anyInt(), anyInt());
    }

    @Test
    void testReturnsFirstSuccessfulResponse() throws InterruptedException
    {
        SidecarClient mockClient = mock(SidecarClient.class);
        InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
        List<InstanceMetadata> instances = createMockInstances(2);
        when(mockInstancesMetadata.instances()).thenReturn(instances);

        GossipInfoResponse response1 = new GossipInfoResponse();
        GossipInfoResponse response2 = new GossipInfoResponse();

        when(mockClient.gossipInfo(any(SidecarInstance.class)))
        .thenReturn(CompletableFuture.completedFuture(response1))
        .thenReturn(CompletableFuture.completedFuture(response2));

        GossipInfoFetcher fetcher = new GossipInfoFetcher(mockClient, mockInstancesMetadata, 9043, 5, 5);
        Future<GossipInfoResponse> result = fetcher.fetchGossipInfo();

        awaitForFuture(result);

        assertThat(result.succeeded()).isTrue();
        // Should return one of the responses (Future.any returns first successful)
        assertThat(result.result()).isIn(response1, response2);
    }

    @Test
    void testMaxRetriesExhausted() throws InterruptedException
    {
        SidecarClient mockClient = mock(SidecarClient.class);
        InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);

        // Create enough instances to exceed max retries (5 batches of 5 = 25 instances)
        List<InstanceMetadata> instances = createMockInstances(25);
        when(mockInstancesMetadata.instances()).thenReturn(instances);
        when(mockClient.gossipInfo(any(SidecarInstance.class)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")));

        GossipInfoFetcher fetcher = new GossipInfoFetcher(mockClient, mockInstancesMetadata, 9043, 5, 5);
        Future<GossipInfoResponse> result = fetcher.fetchGossipInfo();

        awaitForFuture(result);

        assertThat(result.failed()).isTrue();
        assertThat(result.cause().getMessage()).contains("Failed to fetch gossip after 5 attempts across 25 instances");
    }

    @Test
    void testFetchesFromMultipleBatches() throws InterruptedException
    {
        SidecarClient mockClient = mock(SidecarClient.class);
        InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);

        // Create 7 instances - first batch of 5 fails, second batch of 2 succeeds on first instance
        List<InstanceMetadata> instances = createMockInstances(7);
        when(mockInstancesMetadata.instances()).thenReturn(instances);

        GossipInfoResponse mockResponse = new GossipInfoResponse();

        // First 5 calls fail, 6th succeeds
        when(mockClient.gossipInfo(any(SidecarInstance.class)))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")))
        .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection failed")))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

        GossipInfoFetcher fetcher = spy(new GossipInfoFetcher(mockClient, mockInstancesMetadata, 9043, 5, 5));
        Future<GossipInfoResponse> result = fetcher.fetchGossipInfo();

        awaitForFuture(result);

        assertThat(result.succeeded()).isTrue();
        assertThat(result.result()).isEqualTo(mockResponse);
        verify(mockClient, times(7)).gossipInfo(any(SidecarInstance.class));
        verify(fetcher, times(2))
        .fetchGossipFromBatch(anyList(), anyInt(), anyInt(), anyInt(), anyInt());
    }

    private InstanceMetadata createMockInstance(String host, int port)
    {
        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.host()).thenReturn(host);
        when(instance.port()).thenReturn(port);
        return instance;
    }

    private List<InstanceMetadata> createMockInstances(int count)
    {
        List<InstanceMetadata> instances = new ArrayList<>(count);
        for (int i = 1; i <= count; i++)
        {
            instances.add(createMockInstance("host" + i, 9043));
        }
        return instances;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private <T> void awaitForFuture(Future<T> future) throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(1);
        future.onComplete(res -> latch.countDown());
        latch.await(5, TimeUnit.SECONDS);
    }
}
