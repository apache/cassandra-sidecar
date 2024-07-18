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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Refreshes the Cassandra ring topology fetched via JMX periodically
 */
@Singleton
public class RingTopologyRefresher implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RingTopologyRefresher.class);

    private final InstanceMetadataFetcher metadataFetcher;
    private final ReplicaByTokenRangePerKeyspace replicaByTokenRangePerKeyspace;
    private final RestoreJobConfiguration restoreJobConfiguration;

    @Inject
    public RingTopologyRefresher(InstanceMetadataFetcher metadataFetcher,
                                 SidecarConfiguration config)
    {
        this.metadataFetcher = metadataFetcher;
        this.restoreJobConfiguration = config.restoreJobConfiguration();
        this.replicaByTokenRangePerKeyspace = new ReplicaByTokenRangePerKeyspace();
    }

    @Override
    public long delay()
    {
        return restoreJobConfiguration.ringTopologyRefreshDelayMillis();
    }

    @Override
    public boolean shouldSkip()
    {
        return replicaByTokenRangePerKeyspace.isEmpty();
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        executeBlocking();
        promise.tryComplete();
    }

    public void register(RestoreJob restoreJob)
    {
        replicaByTokenRangePerKeyspace.register(restoreJob);
    }

    public void unregister(RestoreJob restoreJob)
    {
        replicaByTokenRangePerKeyspace.unregister(restoreJob);
    }

    @Nullable
    public TokenRangeReplicasResponse cachedReplicaByTokenRange(RestoreJob restoreJob)
    {
        return replicaByTokenRangePerKeyspace.forRestoreJob(restoreJob);
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

    private void executeBlocking()
    {
        CassandraAdapterDelegate delegate = metadataFetcher.anyInstance().delegate();
        StorageOperations storageOperations = delegate == null ? null : delegate.storageOperations();
        NodeSettings nodeSettings = delegate == null ? null : delegate.nodeSettings();
        if (storageOperations == null || nodeSettings == null)
        {
            LOGGER.debug("Not yet connect to Cassandra");
            return;
        }
        String partitioner = nodeSettings.partitioner();
        replicaByTokenRangePerKeyspace.load(keyspace -> storageOperations.tokenRangeReplicas(new Name(keyspace), partitioner));
    }

    /**
     * Core data class of the {@link RingTopologyRefresher}.
     * It groups the ReplicaByTokenRange by keyspace and provides cache capability
     * <p>
     * Not thread-safe; only accessed within the single-threaded execution of RingTopologyRefresher
     */
    static class ReplicaByTokenRangePerKeyspace
    {
        private final Set<UUID> allJobs = new HashSet<>();
        private final Map<String, Set<UUID>> jobsByKeyspace = new HashMap<>();
        private final Map<String, TokenRangeReplicasResponse> mapping = new HashMap<>();
        private final Map<String, Promise<TokenRangeReplicasResponse>> promises = new HashMap<>();

        boolean isEmpty()
        {
            return allJobs.isEmpty();
        }

        // returns keyspace name
        String register(RestoreJob restoreJob)
        {
            allJobs.add(restoreJob.jobId);
            jobsByKeyspace.compute(restoreJob.keyspaceName, (ks, jobs) -> {
                Set<UUID> jobsSet = jobs == null ? new HashSet<>() : jobs;
                jobsSet.add(restoreJob.jobId);
                return jobsSet;
            });
            return restoreJob.keyspaceName;
        }

        void unregister(RestoreJob restoreJob)
        {
            boolean containsJob = allJobs.remove(restoreJob.jobId);
            if (containsJob)
            {
                Set<UUID> jobIdsByKeyspace = jobsByKeyspace.get(restoreJob.keyspaceName);
                if (jobIdsByKeyspace == null
                    || jobIdsByKeyspace.isEmpty()
                    || !jobIdsByKeyspace.remove(restoreJob.jobId))
                {
                    LOGGER.warn("Unable to find the restore job id to unregister. jobId={}", restoreJob.jobId);
                    return;
                }

                if (!jobIdsByKeyspace.isEmpty())
                {
                    // do not remove mapping if there are still jobs of the keyspace
                    return;
                }

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
            distinctKeyspaces.forEach(keyspace -> loadEach(keyspace, loader));
        }

        // suppress any Exception when loading topology of each keyspace and continue
        void loadEach(String keyspace, Function<String, TokenRangeReplicasResponse> loader)
        {
            try
            {
                TokenRangeReplicasResponse topology = loader.apply(keyspace);
                mapping.compute(keyspace, (key, existing) -> {
                    if (existing == null)
                    {
                        // fulfill promise after retrieving the initial topology
                        // the promise for the job is made via `promise(RestoreJob)` method
                        promises.computeIfPresent(keyspace, (k, promise) -> {
                            promise.tryComplete(topology);
                            return promise;
                        });
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
                        return topology;
                    }
                });
            }
            catch (Exception ex)
            {
                LOGGER.warn("Failure during load topology for keyspace. keyspace={}", keyspace, ex);
                promises.computeIfPresent(keyspace, (k, promise) -> {
                    promise.tryFail(new IllegalStateException("Failed to load topology for keyspace: " + keyspace, ex));
                    // return null to remove the promise
                    return null;
                });

            }
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
    }
}
