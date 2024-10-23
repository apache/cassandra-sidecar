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

package org.apache.cassandra.sidecar.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.cluster.locator.InstanceSetByDc;
import org.apache.cassandra.sidecar.common.data.ConsistencyLevel;
import org.apache.cassandra.sidecar.common.data.ConsistencyVerificationResult;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A collection of {@link ConsistencyVerifier} to various consistency levels
 */
public class ConsistencyVerifiers
{
    private static final String UNKNOWN_DC = "ConsistencyVerifiers.UnknownDc";

    private ConsistencyVerifiers()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Similar to {@link #forConsistencyLevel(ConsistencyLevel, String)}, but set {@code localDatacenter} to null.
     */
    public static ConsistencyVerifier forConsistencyLevel(ConsistencyLevel consistencyLevel)
    {
        Preconditions.checkArgument(!consistencyLevel.isLocalDcOnly,
                                    "Cannot create verifier with local consistency level");
        return forConsistencyLevel(consistencyLevel, null);
    }

    /**
     * Create {@link ConsistencyVerifier} based on the consistency level and the local datacenter name.
     * @param consistencyLevel consistency level to verify
     * @param localDatacenter local datacenter name when the consistency level is local, e.g. LOCAL_QUORUM
     * @return consistency verifier
     */
    public static ConsistencyVerifier forConsistencyLevel(ConsistencyLevel consistencyLevel, @Nullable String localDatacenter)
    {
        switch (consistencyLevel)
        {
            case ONE:
                return ForOne.INSTANCE;
            case TWO:
                return ForTwo.INSTANCE;
            case QUORUM:
                return ForQuorum.INSTANCE;
            case ALL:
                return ForAll.INSTANCE;
            case EACH_QUORUM:
                return ForEachQuorum.INSTANCE;
            case LOCAL_ONE:
                return new ForLocalOne(localDatacenter);
            case LOCAL_QUORUM:
                return new ForLocalQuorum(localDatacenter);
            default:
                throw new IllegalStateException("Encountered unknown consistency level: " + consistencyLevel);
        }
    }

    /**
     * Base verifier to provide shared utility methods
     */
    public abstract static class BaseVerifier implements ConsistencyVerifier
    {
        protected int quorum(int total)
        {
            return total / 2 + 1;
        }

        protected int sum(InstanceSetByDc instanceSetByDc)
        {
            return instanceSetByDc.mapping.values().stream().mapToInt(Set::size).sum();
        }

        protected boolean geQuorum(int count, int total)
        {
            return count >= quorum(total);
        }

        protected InstanceSetByDc groupByDc(Set<String> instances, UnaryOperator<String> dcClassifier)
        {
            return new InstanceSetByDc(instances.stream()
                                                .collect(Collectors.groupingBy(dcClassifier, Collectors.toSet())));
        }

        protected InstanceSetByDc filterByDc(Set<String> instances, String dcName, Predicate<String> dcFilter)
        {
            Set<String> localDcInstances = instances.stream().filter(dcFilter).collect(Collectors.toSet());
            return new InstanceSetByDc(dcName, localDcInstances);
        }

        protected void validateNoneFromUnknownDc(InstanceSetByDc instanceSetByDc, String kind)
        {
            if (instanceSetByDc.mapping.containsKey(UNKNOWN_DC))
            {
                throw new IllegalStateException("Instances from the " + kind + " set belongs to unknown datacenter. " +
                                                "Instances: " + instanceSetByDc.mapping.get(UNKNOWN_DC));
            }
        }
    }

    /**
     * Verifier for consistency level ONE
     * It concludes result
     * - SATISFIED: one instance succeeds
     * - FAILED: all instances fail
     * - PENDING: default
     */
    public static class ForOne extends BaseVerifier
    {
        public static final ForOne INSTANCE = new ForOne();

        @Override
        public ConsistencyVerificationResult verify(Set<String> succeeded, Set<String> failed, InstanceSetByDc all)
        {
            if (!succeeded.isEmpty())
            {
                return ConsistencyVerificationResult.SATISFIED;
            }

            if (failed.size() == sum(all))
            {
                return ConsistencyVerificationResult.FAILED;
            }

            return ConsistencyVerificationResult.PENDING;
        }
    }

    /**
     * Verifier for consistency level TWO
     * It concludes result
     * - SATISFIED: one instance succeeds
     * - FAILED: all instances fail
     * - PENDING: default
     */
    public static class ForTwo extends BaseVerifier
    {
        public static final ForTwo INSTANCE = new ForTwo();

        @Override
        public ConsistencyVerificationResult verify(Set<String> succeeded, Set<String> failed, InstanceSetByDc all)
        {
            if (succeeded.size() >= 2)
            {
                return ConsistencyVerificationResult.SATISFIED;
            }

            // When it can at most pass on one instance, it must fail
            if (failed.size() == sum(all) - 1)
            {
                return ConsistencyVerificationResult.FAILED;
            }

            return ConsistencyVerificationResult.PENDING;
        }
    }

    /**
     * Verifier for consistency level EACH_QUORUM
     * It concludes result
     * - SATISFIED: quorum instances in all datacenters succeed
     * - FAILED: quorum instances in any datacenter fail
     * - PENDING: default
     */
    public static class ForEachQuorum extends BaseVerifier
    {
        public static final ForEachQuorum INSTANCE = new ForEachQuorum();

        @Override
        public ConsistencyVerificationResult verify(Set<String> succeeded, Set<String> failed, InstanceSetByDc all)
        {
            // flatten and invert to get the mapping from instance to dc name
            Map<String, String> dcByInstance = new HashMap<>(sum(all));
            all.forEach((dcName, replicaSet) -> {
                replicaSet.forEach(instance -> {
                    dcByInstance.put(instance, dcName);
                });
            });

            UnaryOperator<String> dcClassifier = s -> {
                String dc = dcByInstance.get(s);
                return dc != null ? dc : UNKNOWN_DC;
            };
            InstanceSetByDc passedByDc = groupByDc(succeeded, dcClassifier);
            validateNoneFromUnknownDc(passedByDc, "passed");
            InstanceSetByDc failedByDc = groupByDc(failed, dcClassifier);
            validateNoneFromUnknownDc(failedByDc, "failed");

            // check whether passed instance can satisfy quorum in all DCs
            boolean allSatisfied = true;
            boolean anyFailed = false;
            for (String dcName : all.keySet())
            {
                // if any of the datacenter cannot satisfy quorum locally
                if (!geQuorum(passedByDc.get(dcName).size(), all.get(dcName).size()))
                {
                    allSatisfied = false;
                }

                // if any of the datacenter has failed quorum locally
                if (geQuorum(failedByDc.get(dcName).size(), all.get(dcName).size()))
                {
                    anyFailed = true;
                }
            }
            if (allSatisfied)
            {
                return ConsistencyVerificationResult.SATISFIED;
            }

            if (anyFailed)
            {
                return ConsistencyVerificationResult.FAILED;
            }

            return ConsistencyVerificationResult.PENDING;
        }
    }

    /**
     * Verifier for consistency level QUORUM
     * It concludes result
     * - SATISFIED: quorum instances succeed
     * - FAILED: quorum instances fail
     * - PENDING: default
     */
    public static class ForQuorum extends BaseVerifier
    {
        public static final ForQuorum INSTANCE = new ForQuorum();

        @Override
        public ConsistencyVerificationResult verify(Set<String> succeeded, Set<String> failed, InstanceSetByDc all)
        {
            if (geQuorum(succeeded.size(), sum(all)))
            {
                return ConsistencyVerificationResult.SATISFIED;
            }

            if (geQuorum(failed.size(), sum(all)))
            {
                return ConsistencyVerificationResult.FAILED;
            }

            return ConsistencyVerificationResult.PENDING;
        }
    }

    /**
     * Verifier for consistency level ALL
     * It concludes result
     * - SATISFIED: all instances succeed
     * - FAILED: one instance fails
     * - PENDING: default
     */
    public static class ForAll extends BaseVerifier
    {
        public static final ForAll INSTANCE = new ForAll();

        @Override
        public ConsistencyVerificationResult verify(Set<String> succeeded, Set<String> failed, InstanceSetByDc all)
        {
            if (succeeded.size() == sum(all))
            {
                return ConsistencyVerificationResult.SATISFIED;
            }

            if (!failed.isEmpty())
            {
                return ConsistencyVerificationResult.FAILED;
            }

            return ConsistencyVerificationResult.PENDING;
        }
    }

    /**
     * Verifier for consistency level LOCAL_ONE
     * It concludes result
     * - SATISFIED: one instance in local datacenter succeeds
     * - FAILED: all instances in local datacenter fail
     * - PENDING: default
     */
    public static class ForLocalOne extends BaseLocalDcVerifier
    {
        ForLocalOne(String localDatacenter)
        {
            super(localDatacenter);
        }

        @Override
        protected ConsistencyVerificationResult verifyForLocalDC(InstanceSetByDc localPassed, InstanceSetByDc localFailed, InstanceSetByDc localAll)
        {
            if (sum(localPassed) > 0)
            {
                return ConsistencyVerificationResult.SATISFIED;
            }

            if (sum(localFailed) == sum(localAll))
            {
                return ConsistencyVerificationResult.FAILED;
            }

            return ConsistencyVerificationResult.PENDING;
        }
    }

    /**
     * Verifier for consistency level LOCAL_QUORUM
     * It concludes result
     * - SATISFIED: quorum instances in local datacenter succeed
     * - FAILED: quorum instances in local datacenter fail
     * - PENDING: default
     */
    public static class ForLocalQuorum extends BaseLocalDcVerifier
    {
        ForLocalQuorum(String localDatacenter)
        {
            super(localDatacenter);
        }

        @Override
        protected ConsistencyVerificationResult verifyForLocalDC(InstanceSetByDc localPassed, InstanceSetByDc localFailed, InstanceSetByDc localAll)
        {
            // Over quorum instances have passed
            if (geQuorum(sum(localPassed), sum(localAll)))
            {
                return ConsistencyVerificationResult.SATISFIED;
            }

            // Over quorum instances have failed
            if (geQuorum(sum(localFailed), sum(localAll)))
            {
                return ConsistencyVerificationResult.FAILED;
            }

            return ConsistencyVerificationResult.PENDING;
        }
    }

    private abstract static class BaseLocalDcVerifier extends BaseVerifier
    {
        protected final String localDatacenter;

        BaseLocalDcVerifier(String localDatacenter)
        {
            Preconditions.checkArgument(localDatacenter != null && !localDatacenter.isEmpty(),
                                        "localDatacenter must present for local DC consistency verifier");
            this.localDatacenter = localDatacenter;
        }

        @Override
        public ConsistencyVerificationResult verify(@NotNull Set<String> succeeded, @NotNull Set<String> failed, @NotNull InstanceSetByDc all)
        {
            Preconditions.checkArgument(all.containsDatacenter(localDatacenter),
                                        "Parameter 'all' should contain the local datacenter: " + localDatacenter);
            Set<String> localReplicas = all.get(localDatacenter);
            InstanceSetByDc localAll = new InstanceSetByDc(localDatacenter, localReplicas);
            InstanceSetByDc localPassed = filterByDc(succeeded, localDatacenter, localReplicas::contains);
            InstanceSetByDc localFailed = filterByDc(failed, localDatacenter, localReplicas::contains);
            return verifyForLocalDC(localPassed, localFailed, localAll);
        }

        protected abstract ConsistencyVerificationResult verifyForLocalDC(InstanceSetByDc localPassed,
                                                                          InstanceSetByDc localFailed,
                                                                          InstanceSetByDc localAll);
    }
}
