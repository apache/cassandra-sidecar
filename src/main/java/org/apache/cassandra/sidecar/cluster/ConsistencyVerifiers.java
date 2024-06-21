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
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.cluster.locator.InstanceSetByDc;
import org.apache.cassandra.sidecar.common.data.ConsistencyLevel;
import org.apache.cassandra.sidecar.common.utils.Preconditions;

/**
 * A collection of {@link ConsistencyVerifier} to various consistency levels
 */
public class ConsistencyVerifiers
{
    private static final String UNKNOWN_DC = "ConsistencyVerifiers.UnknownDc";
    private static final String ONLY_LOCAL_DC_CHECK_ERR_MSG = "Parameter 'all' should contain the write replicas " +
                                                              "in local datacenter only";

    private ConsistencyVerifiers()
    {
        throw new UnsupportedOperationException();
    }

    public static ConsistencyVerifier forConsistencyLevel(ConsistencyLevel consistencyLevel)
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
            case LOCAL_ONE:
                return ForLocalOne.INSTANCE;
            case LOCAL_QUORUM:
                return ForLocalQuorum.INSTANCE;
            case EACH_QUORUM:
                return ForEachQuorum.INSTANCE;
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
        public Result verify(Set<String> succeeded, Set<String> failed, InstanceSetByDc all)
        {
            if (!succeeded.isEmpty())
            {
                return Result.SATISFIED;
            }

            if (failed.size() == sum(all))
            {
                return Result.FAILED;
            }

            return Result.PENDING;
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
        public Result verify(Set<String> succeeded, Set<String> failed, InstanceSetByDc all)
        {
            if (succeeded.size() >= 2)
            {
                return Result.SATISFIED;
            }

            // When it can at most pass on one instance, it must fail
            if (failed.size() == sum(all) - 1)
            {
                return Result.FAILED;
            }

            return Result.PENDING;
        }
    }

    /**
     * Verifier for consistency level LOCAL_ONE
     * It concludes result
     * - SATISFIED: one instance in local datacenter succeeds
     * - FAILED: all instances in local datacenter fail
     * - PENDING: default
     */
    public static class ForLocalOne extends BaseVerifier
    {
        public static final ForLocalOne INSTANCE = new ForLocalOne();

        @Override
        public Result verify(Set<String> succeeded, Set<String> failed, InstanceSetByDc all)
        {
            Preconditions.checkArgument(all.size() == 1, ONLY_LOCAL_DC_CHECK_ERR_MSG);
            String dcName = all.keySet().iterator().next();
            Set<String> localReplicas = all.get(dcName);
            UnaryOperator<String> dcClassifier = s -> localReplicas.contains(s) ? dcName : UNKNOWN_DC;
            InstanceSetByDc passedByDc = groupByDc(succeeded, dcClassifier);
            validateNoneFromUnknownDc(passedByDc, "passed");

            if (sum(passedByDc) > 0)
            {
                return Result.SATISFIED;
            }

            InstanceSetByDc failedByDc = groupByDc(failed, dcClassifier);
            validateNoneFromUnknownDc(failedByDc, "failed");

            if (sum(failedByDc) == sum(all))
            {
                return Result.FAILED;
            }

            return Result.PENDING;
        }
    }

    /**
     * Verifier for consistency level LOCAL_QUORUM
     * It concludes result
     * - SATISFIED: quorum instances in local datacenter succeed
     * - FAILED: quorum instances in local datacenter fail
     * - PENDING: default
     */
    public static class ForLocalQuorum extends BaseVerifier
    {
        public static final ForLocalQuorum INSTANCE = new ForLocalQuorum();

        @Override
        public Result verify(Set<String> succeeded, Set<String> failed, InstanceSetByDc all)
        {
            Preconditions.checkArgument(all.size() == 1, ONLY_LOCAL_DC_CHECK_ERR_MSG);
            String dcName = all.keySet().iterator().next();
            Set<String> localReplicas = all.get(dcName);
            UnaryOperator<String> dcClassifier = s -> localReplicas.contains(s) ? dcName : UNKNOWN_DC;
            InstanceSetByDc passedByDc = groupByDc(succeeded, dcClassifier);
            validateNoneFromUnknownDc(passedByDc, "passed");

            // Over quorum instances have passed
            if (geQuorum(sum(passedByDc), sum(all)))
            {
                return Result.SATISFIED;
            }

            InstanceSetByDc failedByDc = groupByDc(failed, dcClassifier);
            validateNoneFromUnknownDc(failedByDc, "failed");

            // Over quorum instances have failed
            if (geQuorum(sum(failedByDc), sum(all)))
            {
                return Result.FAILED;
            }

            return Result.PENDING;
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
        public Result verify(Set<String> succeeded, Set<String> failed, InstanceSetByDc all)
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
                return Result.SATISFIED;
            }

            if (anyFailed)
            {
                return Result.FAILED;
            }

            return Result.PENDING;
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
        public Result verify(Set<String> succeeded, Set<String> failed, InstanceSetByDc all)
        {
            if (geQuorum(succeeded.size(), sum(all)))
            {
                return Result.SATISFIED;
            }

            if (geQuorum(failed.size(), sum(all)))
            {
                return Result.FAILED;
            }

            return Result.PENDING;
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
        public Result verify(Set<String> succeeded, Set<String> failed, InstanceSetByDc all)
        {
            if (succeeded.size() == sum(all))
            {
                return Result.SATISFIED;
            }

            if (!failed.isEmpty())
            {
                return Result.FAILED;
            }

            return Result.PENDING;
        }
    }
}
