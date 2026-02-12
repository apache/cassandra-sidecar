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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.GossipInfoResponse;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Responsible for fetching gossip information from cluster instances.
 * Uses a batch-based parallel approach to minimize network calls and maximize efficiency.
 */
class GossipInfoFetcher
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GossipInfoFetcher.class);

    private final SidecarClient sidecarClient;
    private final InstancesMetadata instancesMetadata;
    private final int batchSize;
    private final int maxRetries;
    private final int sidecarPort;

    GossipInfoFetcher(SidecarClient sidecarClient,
                      InstancesMetadata instancesMetadata,
                      int sidecarPort,
                      int batchSize,
                      int maxRetries)
    {
        this.sidecarClient = sidecarClient;
        this.instancesMetadata = instancesMetadata;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
        this.sidecarPort = sidecarPort;
    }

    /**
     * Fetches gossip info from cluster instances using a batch-based parallel approach.
     * Tries instances in batches, returning as soon as any instance successfully provides gossip info.
     *
     * @return Future with gossip response from the first successful instance
     */
    Future<GossipInfoResponse> fetchGossipInfo()
    {
        List<InstanceMetadata> allInstances = new ArrayList<>(instancesMetadata.instances());

        if (allInstances.isEmpty())
        {
            return Future.failedFuture("No instances available for gossip fetch");
        }

        LOGGER.info("Fetching gossip info from {} instances in batches of {}", allInstances.size(), batchSize);

        // Shuffle to ensure that random instances will be contacted in random manner
        Collections.shuffle(allInstances);

        return fetchGossipFromBatch(allInstances, 0, batchSize, maxRetries, 1);
    }

    /**
     * Recursively fetches gossip from a batch of instances.
     * Tries instances in parallel within each batch, returns on first success.
     *
     * @param instances  all available instances
     * @param startIndex starting index for this batch
     * @param batchSize  number of instances to try in parallel
     * @param maxRetries maximum number of batch attempts
     * @param attempt    current attempt number
     * @return Future with gossip response from first successful instance
     */
    @VisibleForTesting
    Future<GossipInfoResponse> fetchGossipFromBatch(List<InstanceMetadata> instances,
                                                    int startIndex,
                                                    int batchSize,
                                                    int maxRetries,
                                                    int attempt)
    {
        if (attempt > maxRetries)
        {
            String errorMsg = String.format("Failed to fetch gossip after %d attempts across %d instances",
                                            maxRetries, instances.size());
            LOGGER.error(errorMsg);
            return Future.failedFuture(errorMsg);
        }

        List<InstanceMetadata> batch = new ArrayList<>();
        for (int i = startIndex; i < Math.min(startIndex + batchSize, instances.size()); i++)
        {
            batch.add(instances.get(i));
        }

        if (batch.isEmpty())
        {
            // No more instances to try, return failure
            String errorMsg = "Failed to fetch gossip after trying all " + instances.size() + " instances";
            LOGGER.error(errorMsg);
            return Future.failedFuture(errorMsg);
        }

        LOGGER.info("Trying batch of {} instances (attempt {}/{})", batch.size(), attempt, maxRetries);

        // Try all instances in this batch in parallel
        List<Future<GossipInfoResponse>> batchFutures = batch.stream()
                                                             .map(this::fetchGossipFromInstance)
                                                             .collect(Collectors.toList());

        return Future.any(batchFutures)
                     .compose(this::extractSuccessfulResult,
                              cause -> retryFetchGossipInfo(instances, startIndex, batchSize,
                                                            maxRetries, attempt, cause));
    }

    private Future<GossipInfoResponse> extractSuccessfulResult(CompositeFuture cf)
    {
        for (int i = 0; i < cf.size(); i++)
        {
            if (cf.succeeded(i))
            {
                LOGGER.info("Successfully fetched gossip info");
                return Future.succeededFuture(cf.resultAt(i));
            }
        }
        return Future.failedFuture("Failed to fetch gossip info");
    }

    private Future<GossipInfoResponse> retryFetchGossipInfo(List<InstanceMetadata> instances,
                                                            int startIndex,
                                                            int batchSize,
                                                            int maxRetries,
                                                            int attempt,
                                                            Throwable cause)
    {
        LOGGER.debug("Batch failed, trying next batch: {}", cause.getMessage());
        int nextIndex = startIndex + batchSize;
        return fetchGossipFromBatch(instances, nextIndex, batchSize, maxRetries, attempt + 1);
    }

    /**
     * Fetches gossip info from a single instance using the default retry policy of the SidecarClient.
     *
     * @param instance the instance to fetch gossip from
     * @return Future with gossip response
     */
    private Future<GossipInfoResponse> fetchGossipFromInstance(InstanceMetadata instance)
    {
        SidecarInstanceImpl sidecarInstance = new SidecarInstanceImpl(instance.host(), sidecarPort);

        LOGGER.debug("Fetching gossip from {}", instance.host());

        return Future.fromCompletionStage(sidecarClient.gossipInfo(sidecarInstance))
                     .onSuccess(response ->
                                LOGGER.debug("Successfully fetched gossip from {}", instance.host()))
                     .onFailure(error ->
                                LOGGER.debug("Failed to fetch gossip from {}: {}",
                                             instance.host(), error.getMessage()));
    }
}
