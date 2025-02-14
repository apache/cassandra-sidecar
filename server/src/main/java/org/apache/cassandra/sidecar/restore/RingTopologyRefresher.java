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

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.locator.LocalTokenRangesProvider;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Token;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.StringUtils;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Refreshes the Cassandra ring topology fetched via JMX periodically
 * // TODO: this class can be generalized to serve other Sidecar components that need to be aware of topology and topology change
 */
@Singleton
public class RingTopologyRefresher implements PeriodicTask, LocalTokenRangesProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RingTopologyRefresher.class);

    private final InstanceMetadataFetcher metadataFetcher;
    private final ReplicaByTokenRangePerKeyspace replicaByTokenRangePerKeyspace;
    private final RestoreJobConfiguration restoreJobConfiguration;
    private final TaskExecutorPool executorPool;
    private final Map<String, Set<RingTopologyChangeListener>> listenersByKeyspace = new ConcurrentHashMap<>();

    @Inject
    public RingTopologyRefresher(InstanceMetadataFetcher metadataFetcher,
                                 SidecarConfiguration config,
                                 ExecutorPools executorPools)
    {
        this.metadataFetcher = metadataFetcher;
        this.restoreJobConfiguration = config.restoreJobConfiguration();
        this.replicaByTokenRangePerKeyspace = new ReplicaByTokenRangePerKeyspace(this::dispatchRingTopologyChangeAsync);
        this.executorPool = executorPools.internal();
    }

    @Override
    public DurationSpec delay()
    {
        return restoreJobConfiguration.ringTopologyRefreshDelay();
    }

    @Override
    public ScheduleDecision scheduleDecision()
    {
        return replicaByTokenRangePerKeyspace.isEmpty() ? ScheduleDecision.SKIP : ScheduleDecision.EXECUTE;
    }

    /**
     * Execute the periodic task to refresh the topology layout for all registered keyspaces
     *
     * <p>It is synchronized as there is potential contention from {@link #localTokenRanges(String)}
     * @param promise a promise when the execution completes
     */
    @Override
    public synchronized void execute(Promise<Void> promise)
    {
        prepareAndFetch(this::loadAll);
        promise.tryComplete();
    }

    public Set<UUID> allRestoreJobsOfKeyspace(String keyspace)
    {
        return replicaByTokenRangePerKeyspace.jobsByKeyspace.get(keyspace);
    }

    public void register(RestoreJob restoreJob, RingTopologyChangeListener listener)
    {
        replicaByTokenRangePerKeyspace.register(restoreJob);
        addRingTopologyChangeListener(restoreJob.keyspaceName, listener);
    }

    public void unregister(RestoreJob restoreJob, RingTopologyChangeListener listener)
    {
        boolean allRemoved = replicaByTokenRangePerKeyspace.unregister(restoreJob);
        if (allRemoved)
        {
            removeRingTopologyChangeListener(restoreJob.keyspaceName, listener);
        }
    }

    public void addRingTopologyChangeListener(String keyspace, RingTopologyChangeListener listener)
    {
        listenersByKeyspace.computeIfAbsent(keyspace, key -> ConcurrentHashMap.newKeySet())
                           .add(listener);
    }

    public void removeRingTopologyChangeListener(String keyspace, RingTopologyChangeListener listener)
    {
        listenersByKeyspace.computeIfPresent(keyspace, (k, v) -> {
            v.remove(listener);
            return v.isEmpty() ? null : v;
        });
    }

    @Nullable
    public TokenRangeReplicasResponse cachedReplicaByTokenRange(RestoreJob restoreJob)
    {
        return replicaByTokenRangePerKeyspace.forRestoreJob(restoreJob);
    }

    /**
     * Fetch the latest topology view
     * <p>It is synchronized when force refreshing as there is potential contention from {@link #execute(Promise)}
     *
     * @param keyspace keyspace to determine replication
     * @param forceRefresh whether refresh the topology view forcibly or not
     * @return token ranges of the local Cassandra instances or an empty map of nothing is found
     */
    @Override
    public Map<Integer, Set<TokenRange>> localTokenRanges(String keyspace, boolean forceRefresh)
    {
        TokenRangeReplicasResponse topology;
        if (forceRefresh) // fetch the latest topology and load into cache
        {
            synchronized (this)
            {
                topology = prepareAndFetch((storageOperations, nodeSettings) -> {
                    String partitioner = nodeSettings.partitioner();
                    return replicaByTokenRangePerKeyspace.loadOne(keyspace, k -> storageOperations.tokenRangeReplicas(new Name(keyspace), partitioner));
                });
            }
        }
        else // get the cached value
        {
            topology = replicaByTokenRangePerKeyspace.topologyOfKeyspace(keyspace);
        }

        return calculateLocalTokenRanges(metadataFetcher, topology);
    }

    // todo: refactor to a utility class _when_ refactoring TokenRangeReplicasResponse data structure (separate out server and http data representations)
    @NotNull
    public static Map<Integer, Set<TokenRange>> calculateLocalTokenRanges(InstanceMetadataFetcher metadataFetcher, TokenRangeReplicasResponse topology)
    {
        if (topology == null)
        {
            return Map.of();
        }

        // todo: this assumes one C* node per IP address
        Map<String, Integer> allNodes = topology.replicaMetadata().values().stream()
                                                .collect(Collectors.toMap(TokenRangeReplicasResponse.ReplicaMetadata::address,
                                                                          TokenRangeReplicasResponse.ReplicaMetadata::port));

        List<InstanceMetadata> localNodes = metadataFetcher.allLocalInstances();
        Map<String, InstanceMetadata> localEndpointsToMetadata = new HashMap<>(localNodes.size());
        for (InstanceMetadata instanceMetadata : localNodes)
        {
            populateEndpointToMetadata(instanceMetadata, allNodes, localEndpointsToMetadata);
        }

        Map<Integer, Set<TokenRange>> localTokenRanges = new HashMap<>(localEndpointsToMetadata.size());
        for (TokenRangeReplicasResponse.ReplicaInfo ri : topology.writeReplicas())
        {
            TokenRange range = new TokenRange(Token.from(ri.start()), Token.from(ri.end()));
            for (List<String> instanceOfDc : ri.replicasByDatacenter().values())
            {
                for (String instanceEndpoint : instanceOfDc)
                {
                    InstanceMetadata instanceMetadata = localEndpointsToMetadata.get(instanceEndpoint);
                    if (instanceMetadata == null)
                    {
                        // skip the non-local nodes
                        continue;
                    }
                    localTokenRanges.computeIfAbsent(instanceMetadata.id(), k -> new HashSet<>())
                                    .add(range);
                }
            }
        }
        return localTokenRanges;
    }

    /**
     * Retrieve {@link TokenRangeReplicasResponse} that matters to the input restoreJob asynchronously
     * If {@link RingTopologyRefresher} has not retrieved anything yet, the returned future should reflect the initial
     * {@link TokenRangeReplicasResponse} object present; otherwise, it returns the latest value, if changed
     *
     * @return a future of TokenRangeReplicasResponse
     */
    public Future<TokenRangeReplicasResponse> replicaByTokenRangeAsync(RestoreJob restoreJob)
    {
        TokenRangeReplicasResponse cached = cachedReplicaByTokenRange(restoreJob);
        if (cached != null)
        {
            return Future.succeededFuture(cached);
        }

        return replicaByTokenRangePerKeyspace.futureOf(restoreJob);
    }

    // Declaring the Void return type to be compliant with the BiFunction parameter
    private Void loadAll(StorageOperations storageOperations, NodeSettings nodeSettings)
    {
        replicaByTokenRangePerKeyspace.load(keyspace -> storageOperations.tokenRangeReplicas(new Name(keyspace), nodeSettings.partitioner()));
        return null;
    }

    private <T> T prepareAndFetch(BiFunction<StorageOperations, NodeSettings, T> fetcher)
    {
        return metadataFetcher.callOnFirstAvailableInstance(instance -> {
            CassandraAdapterDelegate delegate = instance.delegate();
            StorageOperations storageOperations = delegate.storageOperations();
            NodeSettings nodeSettings = delegate.nodeSettings();
            return fetcher.apply(storageOperations, nodeSettings);
        });
    }

    private void dispatchRingTopologyChangeAsync(String keyspace, TokenRangeReplicasResponse oldTopology, TokenRangeReplicasResponse newTopology)
    {
        // Dispatch onRingTopologyChanged in another thread as it might block
        // This method cannot block, since it is invoked by org.apache.cassandra.sidecar.restore.RingTopologyRefresher.ReplicaByTokenRangePerKeyspace.loadOne
        // in the `compute` lambda.
        listenersByKeyspace.getOrDefault(keyspace, Collections.emptySet()).forEach(listener -> {
            executorPool.runBlocking(() -> listener.onRingTopologyChanged(keyspace, oldTopology, newTopology));
        });
    }

    private static void populateEndpointToMetadata(InstanceMetadata instanceMetadata,
                                                   Map<String, Integer> allNodes,
                                                   Map<String, InstanceMetadata> localEndpointsToMetadata)
    {
        String ip = ipOf(instanceMetadata);
        Integer port = allNodes.get(ip);
        if (ip == null || port == null)
        {
            // the configured local instance is not in the C* ring yet (not even joining). Exit early for such node
            return;
        }
        String endpointWithPort = StringUtils.cassandraFormattedHostAndPort(ip, port);
        localEndpointsToMetadata.put(endpointWithPort, instanceMetadata);
    }

    private static String ipOf(InstanceMetadata instanceMetadata)
    {
        String ipAddress = instanceMetadata.ipAddress();
        if (ipAddress == null)
        {
            try
            {
                return instanceMetadata.refreshIpAddress();
            }
            catch (UnknownHostException uhe)
            {
                LOGGER.debug("Failed to resolve IP address. host={}", instanceMetadata.host());
            }
        }
        return ipAddress;
    }

    /**
     * Core data class of the {@link RingTopologyRefresher}.
     * It groups the ReplicaByTokenRange by keyspace and provides cache capability
     */
    @ThreadSafe
    static class ReplicaByTokenRangePerKeyspace
    {
        private final Set<UUID> allJobs = ConcurrentHashMap.newKeySet();
        private final Map<String, Set<UUID>> jobsByKeyspace = new ConcurrentHashMap<>();
        private final Map<String, TokenRangeReplicasResponse> mapping = new ConcurrentHashMap<>();
        private final Map<String, Promise<TokenRangeReplicasResponse>> promises = new ConcurrentHashMap<>();
        private final RingTopologyChangeListener asyncDispatcher;

        ReplicaByTokenRangePerKeyspace(@NotNull RingTopologyChangeListener asyncDispatcher)
        {
            this.asyncDispatcher = asyncDispatcher;
        }

        boolean isEmpty()
        {
            return allJobs.isEmpty();
        }

        // returns keyspace name
        String register(RestoreJob restoreJob)
        {
            // the job is not yet registered
            if (allJobs.add(restoreJob.jobId))
            {
                jobsByKeyspace.computeIfAbsent(restoreJob.keyspaceName, ks -> ConcurrentHashMap.newKeySet())
                              .add(restoreJob.jobId);
            }

            return restoreJob.keyspaceName;
        }

        /**
         * Unregister a restore job
         *
         * @param restoreJob restore job to unregister
         * @return true when all jobs of the same keyspace are removed; false otherwise
         */
        boolean unregister(RestoreJob restoreJob)
        {
            boolean containsJob = allJobs.remove(restoreJob.jobId);
            if (containsJob) // enter this block only once per jobId
            {
                Set<UUID> jobIdsByKeyspace = jobsByKeyspace.get(restoreJob.keyspaceName);
                if (jobIdsByKeyspace == null
                    || jobIdsByKeyspace.isEmpty()
                    || !jobIdsByKeyspace.remove(restoreJob.jobId))
                {
                    LOGGER.warn("Unable to find the restore job id to unregister. jobId={}", restoreJob.jobId);
                    return true;
                }

                if (!jobIdsByKeyspace.isEmpty())
                {
                    // do not remove mapping if there are still jobs of the keyspace
                    return false;
                }

                // Multiple threads could reach here and run the following code to remove entries from maps
                // The removal is idempotent. It is fine to run the code multiple times.
                LOGGER.info("All jobs of the keyspace are unregistered. keyspace={}", restoreJob.keyspaceName);
                jobsByKeyspace.remove(restoreJob.keyspaceName);
                mapping.remove(restoreJob.keyspaceName);
                Promise<?> p = promises.remove(restoreJob.keyspaceName);
                if (p != null)
                {
                    // try to finish the promise, if not yet.
                    p.tryFail("Unable to retrieve topology for restoreJob. " +
                              "jobId=" + restoreJob.jobId + " keyspace=" + restoreJob.keyspaceName);
                }
            }
            return true;
        }

        // returns the future of TokenRangeReplicasResponse for the restore job
        Future<TokenRangeReplicasResponse> futureOf(RestoreJob restoreJob)
        {
            String keyspace = register(restoreJob);
            return promises.computeIfAbsent(keyspace, k -> Promise.promise()).future();
        }

        @Nullable
        TokenRangeReplicasResponse forRestoreJob(RestoreJob restoreJob)
        {
            if (!allJobs.contains(restoreJob.jobId))
            {
                return null;
            }
            return mapping.get(restoreJob.keyspaceName);
        }

        void load(Function<String, TokenRangeReplicasResponse> loader)
        {
            // loop through the unique names
            Set<String> distinctKeyspaces = jobsByKeyspace.keySet();
            distinctKeyspaces.forEach(keyspace -> loadOne(keyspace, loader));
        }

        // suppress any Exception when loading topology of each keyspace and continue; returns null if fails to load
        TokenRangeReplicasResponse loadOne(String keyspace, Function<String, TokenRangeReplicasResponse> loader)
        {
            try
            {
                TokenRangeReplicasResponse topology = loader.apply(keyspace);
                RingTopologyChangeContext context = new RingTopologyChangeContext(keyspace, topology);
                mapping.compute(keyspace, (key, existing) -> {
                    context.existing = existing;
                    if (existing == null)
                    {
                        // fulfill promise after retrieving the initial topology
                        // the promise for the job is made via `promise(RestoreJob)` method
                        promises.computeIfPresent(keyspace, (k, promise) -> {
                            promise.tryComplete(topology);
                            return promise;
                        });
                        context.shouldDispatch = true;
                        return topology;
                    }
                    else if (existing.writeReplicas().equals(topology.writeReplicas()))
                    {
                        LOGGER.debug("Ring topology of keyspace is unchanged. keyspace={}", keyspace);
                        return existing;
                    }
                    else
                    {
                        LOGGER.info("Ring topology of keyspace is changed. keyspace={}", keyspace);
                        context.shouldDispatch = true;
                        return topology;
                    }
                });
                if (context.shouldDispatch)
                {
                    asyncDispatcher.onRingTopologyChanged(context.keyspace, context.existing, context.current);
                }
                return topology;
            }
            catch (Throwable cause)
            {
                LOGGER.warn("Failure during load topology for keyspace. keyspace={}", keyspace, cause);
                promises.computeIfPresent(keyspace, (k, promise) -> {
                    promise.tryFail(new IllegalStateException("Failed to load topology for keyspace: " + keyspace, cause));
                    // return null to remove the promise
                    return null;
                });
            }
            return null;
        }

        @Nullable
        TokenRangeReplicasResponse topologyOfKeyspace(String keyspace)
        {
            return mapping.get(keyspace);
        }

        @VisibleForTesting
        Set<UUID> allJobsUnsafe()
        {
            return allJobs;
        }

        @VisibleForTesting
        Map<String, Set<UUID>> jobsByKeyspaceUnsafe()
        {
            return jobsByKeyspace;
        }

        @VisibleForTesting
        Map<String, TokenRangeReplicasResponse> mappingUnsafe()
        {
            return mapping;
        }

        @VisibleForTesting
        Map<String, Promise<TokenRangeReplicasResponse>> promisesUnsafe()
        {
            return promises;
        }

        private static class RingTopologyChangeContext
        {
            final String keyspace;
            final TokenRangeReplicasResponse current;
            TokenRangeReplicasResponse existing;
            boolean shouldDispatch = false;

            RingTopologyChangeContext(String keyspace, TokenRangeReplicasResponse current)
            {
                this.keyspace = keyspace;
                this.current = current;
            }
        }
    }
}
