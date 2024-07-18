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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.cluster.ConsistencyVerifier;
import org.apache.cassandra.sidecar.cluster.ConsistencyVerifiers;
import org.apache.cassandra.sidecar.cluster.locator.InstanceSetByDc;
import org.apache.cassandra.sidecar.common.data.RestoreJobProgressFetchPolicy;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Token;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.common.utils.StringUtils;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

/**
 * Checks restore job with the configured consistency level
 */
@Singleton
public class RestoreJobConsistencyLevelChecker
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreJobConsistencyLevelChecker.class);

    private final RingTopologyRefresher ringTopologyRefresher;
    private final RestoreRangeDatabaseAccessor rangeDatabaseAccessor;
    private final TaskExecutorPool taskExecutorPool;

    @Inject
    public RestoreJobConsistencyLevelChecker(RingTopologyRefresher ringTopologyRefresher,
                                             RestoreRangeDatabaseAccessor rangeDatabaseAccessor,
                                             ExecutorPools executorPools)
    {
        this.ringTopologyRefresher = ringTopologyRefresher;
        this.rangeDatabaseAccessor = rangeDatabaseAccessor;
        this.taskExecutorPool = executorPools.internal();
    }

    public Future<RestoreJobProgress> check(RestoreJob restoreJob, RestoreJobProgressFetchPolicy fetchPolicy)
    {
        Preconditions.checkArgument(restoreJob.consistencyLevel != null, "Consistency level of the job must present");
        Preconditions.checkArgument(!restoreJob.consistencyLevel.isLocalDcOnly
                                    || StringUtils.isNotEmpty(restoreJob.localDatacenter),
                                    "When using local consistency level, localDatacenter must present");
        RestoreJobProgressCollector collector = RestoreJobProgressCollectors.create(restoreJob, fetchPolicy);
        RestoreRangeStatus successCriteria = restoreJob.expectedNextRangeStatus();
        ConsistencyVerifier verifier = ConsistencyVerifiers.forConsistencyLevel(restoreJob.consistencyLevel, restoreJob.localDatacenter);
        return ringTopologyRefresher.replicaByTokenRangeAsync(restoreJob)
                                    .compose(topology -> findRangesAndConclude(restoreJob, successCriteria, topology, verifier, collector));
    }

    private Future<RestoreJobProgress> findRangesAndConclude(RestoreJob restoreJob,
                                                             RestoreRangeStatus successCriteria,
                                                             TokenRangeReplicasResponse topology,
                                                             ConsistencyVerifier verifier,
                                                             RestoreJobProgressCollector collector)
    {
        return taskExecutorPool
               .executeBlocking(() -> {
                   short bucketId = 0; // todo: replace with looping through all bucketIds
                   return rangeDatabaseAccessor.findAll(restoreJob.jobId, bucketId);
               })
               .map(ranges -> {
                   if (ranges.isEmpty())
                   {
                       LOGGER.error("No restore ranges found. jobId={}", restoreJob.jobId);
                       throw new IllegalStateException("No restore ranges found for job: " + restoreJob.jobId);
                   }
                   concludeRanges(ranges, topology, verifier, successCriteria, collector);
                   return collector.toRestoreJobProgress();
               });
    }

    private static void concludeRanges(List<RestoreRange> ranges,
                                       TokenRangeReplicasResponse topology,
                                       ConsistencyVerifier verifier,
                                       RestoreRangeStatus successCriteria,
                                       RestoreJobProgressCollector collector)
    {
        for (RestoreRange range : ranges)
        {
            if (!collector.canCollectMore())
            {
                return;
            }

            ConsistencyVerifier.Result res = concludeOneRange(topology, verifier, successCriteria, range);
            collector.collect(range, res);
        }
    }

    /**
     * Examine a range and all its replica should be in the expected status, i.e. {@param successCriteria}.
     * If enough replicas are in the expected status, the conclusion can be made that the range has satisfied.
     * If enough replicas are {@link RestoreRangeStatus.FAILED}, it concludes that the range has failed.
     * Otherwise, no conclusion is made and the range is pending.
     *
     * @param topology current cluster topology
     * @param verifier check whether the replicas status can satisfy the consistency level
     * @param successCriteria the expected {@link RestoreRangeStatus} for replicas
     * @param range range to check
     * @return result of the consistency verification
     */
    private static ConsistencyVerifier.Result concludeOneRange(TokenRangeReplicasResponse topology,
                                                               ConsistencyVerifier verifier,
                                                               RestoreRangeStatus successCriteria,
                                                               RestoreRange range)
    {
        Map<RestoreRangeStatus, Set<String>> groupByStatus = groupReplicaByStatus(range.statusByReplica());
        Set<String> succeeded = groupByStatus.getOrDefault(successCriteria, Collections.emptySet());
        Set<String> failed = groupByStatus.getOrDefault(RestoreRangeStatus.FAILED, Collections.emptySet());
        InstanceSetByDc replicaSet = replicaSetForRange(range, topology);
        if (replicaSet == null) // cannot proceed to verify yet. Return pending
        {
            return ConsistencyVerifier.Result.PENDING;
        }

        ConsistencyVerifier.Result result = verifier.verify(succeeded, failed, replicaSet);
        switch (result)
        {
            case FAILED:
                return ConsistencyVerifier.Result.FAILED;
            case PENDING:
                return ConsistencyVerifier.Result.PENDING;
            default:
                return ConsistencyVerifier.Result.SATISFIED;
        }
    }

    private static Map<RestoreRangeStatus, Set<String>> groupReplicaByStatus(Map<String, RestoreRangeStatus> statusMap)
    {
        return statusMap.entrySet()
                        .stream()
                        // group by status and put the replicas of the same status in a set
                        .collect(groupingBy(Map.Entry::getValue, mapping(Map.Entry::getKey, toSet())));
    }

    /**
     * Find the replica set for the token range. Returns null if no complete match can be found or topology has changed.
     */
    private static @Nullable InstanceSetByDc replicaSetForRange(RestoreRange range, TokenRangeReplicasResponse topology)
    {
        TokenRange restoreRange = new TokenRange(range.startToken(), range.endToken());
        for (TokenRangeReplicasResponse.ReplicaInfo replicaInfo : topology.writeReplicas())
        {
            TokenRange tokenRange = new TokenRange(Token.from(replicaInfo.start()), Token.from(replicaInfo.end()));
            if (tokenRange.encloses(restoreRange))
            {
                Map<String, List<String>> replicasByDc = replicaInfo.replicasByDatacenter();
                Map<String, Set<String>> mapping = new HashMap<>(replicasByDc.size());
                replicasByDc.forEach((k, instances) -> mapping.put(k, new HashSet<>(instances)));
                return new InstanceSetByDc(mapping);
            }
            else if (tokenRange.overlaps(restoreRange))
            {
                LOGGER.info("Topology change detected");
                return null;
            }

            if (tokenRange.largerThan(restoreRange))
            {
                // all following ranges are larger the original range; exit the iteration early.
                break;
            }
        }

        LOGGER.warn("Unable to find a complete match for range. startToken={} endToken={}",
                    range.startToken(), range.endToken());
        return null;
    }

    @VisibleForTesting
    static ConsistencyVerifier.Result concludeOneRangeUnsafe(TokenRangeReplicasResponse topology,
                                                             ConsistencyVerifier verifier,
                                                             RestoreRangeStatus successCriteria,
                                                             RestoreRange range)
    {
        return concludeOneRange(topology, verifier, successCriteria, range);
    }

    @VisibleForTesting
    static InstanceSetByDc replicaSetForRangeUnsafe(RestoreRange range, TokenRangeReplicasResponse topology)
    {
        return replicaSetForRange(range, topology);
    }
}
