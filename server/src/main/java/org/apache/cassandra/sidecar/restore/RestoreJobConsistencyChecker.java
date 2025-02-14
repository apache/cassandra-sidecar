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
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.cluster.ConsistencyVerifier;
import org.apache.cassandra.sidecar.cluster.ConsistencyVerifiers;
import org.apache.cassandra.sidecar.cluster.locator.InstanceSetByDc;
import org.apache.cassandra.sidecar.common.data.ConsistencyVerificationResult;
import org.apache.cassandra.sidecar.common.data.RestoreJobProgressFetchPolicy;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
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
import org.apache.cassandra.sidecar.metrics.RestoreMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.StopWatch;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

/**
 * Checks restore job import consistency with the configured consistency level
 */
@Singleton
public class RestoreJobConsistencyChecker
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreJobConsistencyChecker.class);

    private final RingTopologyRefresher ringTopologyRefresher;
    private final RestoreJobDiscoverer restoreJobDiscoverer;
    private final RestoreRangeDatabaseAccessor rangeDatabaseAccessor;
    private final TaskExecutorPool taskExecutorPool;
    private final RestoreMetrics restoreMetrics;
    private volatile boolean firstTimeSinceImportReady = true;

    @Inject
    public RestoreJobConsistencyChecker(RingTopologyRefresher ringTopologyRefresher,
                                        RestoreJobDiscoverer restoreJobDiscoverer,
                                        RestoreRangeDatabaseAccessor rangeDatabaseAccessor,
                                        ExecutorPools executorPools,
                                        SidecarMetrics sidecarMetrics)
    {
        this.ringTopologyRefresher = ringTopologyRefresher;
        this.restoreJobDiscoverer = restoreJobDiscoverer;
        this.rangeDatabaseAccessor = rangeDatabaseAccessor;
        this.taskExecutorPool = executorPools.internal();
        this.restoreMetrics = sidecarMetrics.server().restore();
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
        Future<RestoreJobProgress> future = ringTopologyRefresher
                                            .replicaByTokenRangeAsync(restoreJob)
                                            .compose(topology -> findRangesAndConclude(restoreJob, successCriteria, topology, verifier, collector));
        return StopWatch.measureTimeTaken(future, durationNanos -> restoreMetrics.consistencyCheckTime.metric.update(durationNanos, TimeUnit.NANOSECONDS));
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
                   if (shouldForceRestoreJobDiscoverRun(restoreJob, ranges))
                   {
                       // schedule the adhoc restore job discovery in a background thread.
                       taskExecutorPool.runBlocking(restoreJobDiscoverer::tryExecuteDiscovery, false);
                       return RestoreJobProgress.pending(restoreJob);
                   }
                   concludeRanges(ranges, topology, verifier, successCriteria, collector);
                   return collector.toRestoreJobProgress();
               });
    }

    private boolean shouldForceRestoreJobDiscoverRun(RestoreJob restoreJob, List<RestoreRange> ranges)
    {
        long foundSliceCount = sliceCountFromRanges(ranges);
        if (foundSliceCount < restoreJob.sliceCount)
        {
            LOGGER.warn("Not all restore ranges are found. Mark the progress as pending and force restore job discover run. " +
                        "jobId={} expectedSliceCount={} foundSliceCount={}", restoreJob.jobId, restoreJob.sliceCount, foundSliceCount);
            return true;
        }

        if (restoreJob.status == RestoreJobStatus.IMPORT_READY && firstTimeSinceImportReady)
        {
            firstTimeSinceImportReady = false;
            LOGGER.info("First time checking consistency of the restore job after import_ready. " +
                        "Mark the progress as pending and force restore job discover run. jobId={}", restoreJob.jobId);
            return true;
        }
        return false;
    }

    @SuppressWarnings("UnstableApiUsage")
    private static void concludeRanges(List<RestoreRange> ranges,
                                       TokenRangeReplicasResponse topology,
                                       ConsistencyVerifier verifier,
                                       RestoreRangeStatus successCriteria,
                                       RestoreJobProgressCollector collector)
    {
        RangeMap<Token, InstanceSetByDc> replicasPerRange = populateReplicas(topology);
        Map<Range<Token>, Pair<Map<String, RestoreRangeStatus>, RestoreRange>> statusPerRange = populateStatusByReplica(ranges);

        for (Map.Entry<Range<Token>, Pair<Map<String, RestoreRangeStatus>, RestoreRange>> entry : statusPerRange.entrySet())
        {
            if (!collector.canCollectMore())
            {
                return;
            }

            Range<Token> tokenRange = entry.getKey();
            Map<String, RestoreRangeStatus> status = entry.getValue().getLeft();
            RestoreRange relevantRestoreRange = entry.getValue().getRight();
            ConsistencyVerificationResult res = concludeOneRange(replicasPerRange, verifier, successCriteria, tokenRange, status);
            collector.collect(relevantRestoreRange, res);
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    static RangeMap<Token, InstanceSetByDc> populateReplicas(TokenRangeReplicasResponse topology)
    {
        RangeMap<Token, InstanceSetByDc> replicasPerRange = TreeRangeMap.create();
        for (TokenRangeReplicasResponse.ReplicaInfo replicaInfo : topology.writeReplicas())
        {
            TokenRange tokenRange = new TokenRange(Token.from(replicaInfo.start()), Token.from(replicaInfo.end()));
            Map<String, List<String>> replicasByDc = replicaInfo.replicasByDatacenter();
            Map<String, Set<String>> mapping = new HashMap<>(replicasByDc.size());
            replicasByDc.forEach((dc, instances) -> mapping.put(dc, new HashSet<>(instances)));
            InstanceSetByDc instanceSetByDc = new InstanceSetByDc(mapping);
            replicasPerRange.put(tokenRange.range, instanceSetByDc);
        }
        return replicasPerRange;
    }

    @SuppressWarnings("UnstableApiUsage")
    static Map<Range<Token>, Pair<Map<String, RestoreRangeStatus>, RestoreRange>> populateStatusByReplica(List<RestoreRange> ranges)
    {
        RangeMap<Token, Pair<Map<String, RestoreRangeStatus>, RestoreRange>> rangeMap = TreeRangeMap.create();
        rangeMap.put(Range.all(), Pair.of(Collections.emptyMap(), null));
        for (RestoreRange restoreRange : ranges)
        {
            Range<Token> tokenRange = restoreRange.tokenRange().range;
            RangeMap<Token, Pair<Map<String, RestoreRangeStatus>, RestoreRange>> sub = rangeMap.subRangeMap(tokenRange);
            Map<Range<Token>, Pair<Map<String, RestoreRangeStatus>, RestoreRange>> map = sub.asMapOfRanges();
            if (map.isEmpty())
            {
                rangeMap.put(tokenRange, Pair.of(restoreRange.statusByReplica(), restoreRange));
            }
            else
            {
                // If subrange map is non-empty, i.e. RestoreRanges has overlapping, the overlapping token range should belong to the same RestoreSlice.
                // Because RestoreRange is derived from RestoreSlice and RestoreSlices do not overlap.
                RangeMap<Token, Pair<Map<String, RestoreRangeStatus>, RestoreRange>> updated = TreeRangeMap.create();
                map.forEach((key, value) -> {
                    Map<String, RestoreRangeStatus> statusMap = new HashMap<>(value.getKey());
                    statusMap.putAll(restoreRange.statusByReplica());
                    updated.put(key, Pair.of(statusMap, restoreRange));
                });
                rangeMap.putAll(updated);
            }
        }
        Map<Range<Token>, Pair<Map<String, RestoreRangeStatus>, RestoreRange>> result = rangeMap.asMapOfRanges();
        result.entrySet().removeIf(e -> e.getValue().getValue() == null);
        return result;
    }

    /**
     * Examine a range and all its replica should be in the expected status, i.e. {@param successCriteria}.
     * If enough replicas are in the expected status, the conclusion can be made that the range has satisfied.
     * If enough replicas are {@link RestoreRangeStatus#FAILED}, it concludes that the range has failed.
     * Otherwise, no conclusion is made and the range is pending.
     *
     * @param replicasByRange replica set of each token range
     * @param verifier check whether the replicas status can satisfy the consistency level
     * @param successCriteria the expected {@link RestoreRangeStatus} for replicas
     * @param range range to check
     * @param statusByReplica restore progress of each instance
     * @return result of the consistency verification
     */
    @SuppressWarnings("UnstableApiUsage")
    private static ConsistencyVerificationResult concludeOneRange(RangeMap<Token, InstanceSetByDc> replicasByRange,
                                                                  ConsistencyVerifier verifier,
                                                                  RestoreRangeStatus successCriteria,
                                                                  Range<Token> range,
                                                                  Map<String, RestoreRangeStatus> statusByReplica)
    {
        Map<RestoreRangeStatus, Set<String>> groupByStatus = groupReplicaByStatus(statusByReplica);
        Set<String> succeeded = groupByStatus.getOrDefault(successCriteria, Collections.emptySet());
        Set<String> failed = groupByStatus.getOrDefault(RestoreRangeStatus.FAILED, Collections.emptySet());
        InstanceSetByDc replicaSet = replicaSetForRange(range, replicasByRange);
        if (replicaSet == null) // cannot proceed to verify yet. Return pending
        {
            return ConsistencyVerificationResult.PENDING;
        }

        ConsistencyVerificationResult result = verifier.verify(succeeded, failed, replicaSet);
        switch (result)
        {
            case FAILED:
                return ConsistencyVerificationResult.FAILED;
            case PENDING:
                return ConsistencyVerificationResult.PENDING;
            default:
                return ConsistencyVerificationResult.SATISFIED;
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
     * The input tokenRange should be fully enclosed by one of the range in the replicasByRange
     */
    @SuppressWarnings("UnstableApiUsage")
    private static @Nullable InstanceSetByDc replicaSetForRange(Range<Token> tokenRange, RangeMap<Token, InstanceSetByDc> replicasByRange)
    {
        Map<Range<Token>, InstanceSetByDc> subRange = replicasByRange.subRangeMap(tokenRange).asMapOfRanges();
        if (subRange.size() == 1)
        {
            // finding one range does not necessarily mean the found range fully encloses the token range
            // subRangeMap could contain the partially overlapping range
            Map.Entry<Range<Token>, InstanceSetByDc> foundEntry = subRange.entrySet().iterator().next();
            if (foundEntry.getKey().encloses(tokenRange))
            {
                return foundEntry.getValue();
            }
            else
            {
                LOGGER.info("Topology is changed");
                return null;
            }
        }
        else
        {
            LOGGER.warn("Unable to find a single match for range. tokenRange={} matchingRanges={}", tokenRange, subRange);
            return null;
        }
    }

    private static long sliceCountFromRanges(List<RestoreRange> ranges)
    {
        return ranges.stream().map(RestoreRange::sliceId).distinct().count();
    }

    @VisibleForTesting
    static ConsistencyVerificationResult concludeOneRangeUnsafe(TokenRangeReplicasResponse topology,
                                                                ConsistencyVerifier verifier,
                                                                RestoreRangeStatus successCriteria,
                                                                RestoreRange range)
    {
        return concludeOneRange(populateReplicas(topology), verifier, successCriteria, range.tokenRange().range, range.statusByReplica());
    }

    @VisibleForTesting
    static InstanceSetByDc replicaSetForRangeUnsafe(RestoreRange range, TokenRangeReplicasResponse topology)
    {
        return replicaSetForRange(range.tokenRange().range, populateReplicas(topology));
    }
}
