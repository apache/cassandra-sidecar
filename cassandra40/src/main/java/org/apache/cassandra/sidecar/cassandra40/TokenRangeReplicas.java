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

package org.apache.cassandra.sidecar.cassandra40;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;


/**
 * Representation of a token range and the corresponding mapping to replica-set hosts
 */
public class TokenRangeReplicas implements Comparable<TokenRangeReplicas>
{
    private final BigInteger start;
    private final BigInteger end;

    private final Partitioner partitioner;

    private final Set<String> replicaSet;

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenRangeReplicas.class);

    public TokenRangeReplicas(BigInteger start, BigInteger end, Partitioner partitioner, Set<String> replicaSet)
    {
        this.start = start;
        this.end = end;
        this.partitioner = partitioner;
        this.replicaSet = replicaSet;
    }


    public BigInteger start()
    {
        return start;
    }

    public BigInteger end()
    {
        return end;
    }

    public Set<String> replicaSet()
    {
        return replicaSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(@NotNull TokenRangeReplicas other)
    {
        if (this.partitioner != other.partitioner)
            throw new IllegalStateException("Token ranges being compared do not have the same partitioner");

        BigInteger maxValue = this.partitioner.maxToken;
        if (this.start.compareTo(other.start) == 0)
        {
            if (this.end.equals(maxValue)) return 1;
            else if (other.end.equals(maxValue)) return -1;
            else return this.end.compareTo(other.end);
        }
        else return this.start.compareTo(other.start);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof TokenRangeReplicas))
        {
            return false;
        }
        TokenRangeReplicas that = (TokenRangeReplicas) o;
        return (this.start.equals(that.start) && this.end.equals(that.end) && this.partitioner == that.partitioner);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return Objects.hashCode(start, end, partitioner);
    }

    private boolean isWrapAround()
    {
        return start.compareTo(end) >= 0;
    }

    private boolean isSubsetOf(TokenRangeReplicas other)
    {
        if (this.partitioner != other.partitioner)
            throw new IllegalStateException("Token ranges being compared do not have the same partitioner");

        BigInteger maxValue = this.partitioner.maxToken;
        if (this.start.compareTo(other.start) >= 0)
        {
            if (other.end.equals(maxValue)) return true;
            if (this.end.equals(maxValue)) return false;
            if (this.end.compareTo(other.end) <= 0) return true;
        }
        return false;
    }

    /**
     * For subset ranges, this is used to determine if a range is larger than the other by comparing start-end lengths
     * If both ranges end at the min, we compare starting points to determine the result.
     * When the left range is the only one ending at min, it is always the larger one since all subsequent ranges
     * in the sorted range list have to be smaller.
     *
     * @param other the next range in the range list to compare
     * @return true if "this" range is larger than the other
     */
    private boolean isLarger(TokenRangeReplicas other)
    {
        if (this.partitioner != other.partitioner)
            throw new IllegalStateException("Token ranges being compared do not have the same partitioner");

        // If both ranges end at min, we compare start of ranges
        if (this.end.equals(partitioner.maxToken) && other.end.equals(partitioner.maxToken))
        {
            return this.start.compareTo(other.start) < 0;
        }

        if (this.end.equals(partitioner.maxToken)) return true;
        if (other.end.equals(partitioner.maxToken)) return false;

        return this.end.subtract(this.start).compareTo(other.end.subtract(other.start)) > 0;
    }

    /**
     * Determines intersection if the next range starts before the current range ends.
     * When the current range ending at min, we determine intersection merely if the next range starts after the current
     * since all subsequent ranges have to be subsets.
     *
     * @param other the range we are currently processing to check if "this" intersects it
     * @return true if "this" range intersects the other
     */
    private boolean intersects(TokenRangeReplicas other)
    {
        return (other.end.compareTo(partitioner.maxToken) == 0 && this.start.compareTo(other.start) > 0) ||
               this.start.compareTo(other.end) < 0;
    }

    /**
     * Unwraps the token range if it wraps-around to end either on or after the least token by overriding such
     * ranges to end at the partitioner max-token value in the former case and splitting into 2 ranges in the latter
     * case.
     *
     * @return list of split ranges
     */
    public List<TokenRangeReplicas> unwrap()
    {
        if (!isWrapAround())
        {
            return Collections.singletonList(this);
        }
        // Wrap-around range ends at the "min-token"
        if (end.compareTo(partitioner.minToken) == 0)
        {
            return Collections.singletonList(
                    new TokenRangeReplicas(start, partitioner.maxToken, partitioner, replicaSet));
        }

        // Wrap-around range goes beyond at the "min-token" and is therefore split into two.
        List<TokenRangeReplicas> unwrapped = new ArrayList<>(2);
        unwrapped.add(new TokenRangeReplicas(start, partitioner.maxToken, partitioner, replicaSet));
        unwrapped.add(new TokenRangeReplicas(partitioner.minToken, end, partitioner, replicaSet));
        return unwrapped;
    }


    /**
     * Given a list of token ranges with replica-sets, normalizes them by unwrapping around the beginning/min
     * of the range and removing overlaps to return a sorted list of non-overlapping ranges.
     *
     * @param ranges
     * @return sorted list of non-overlapping ranges and replica-sets
     */
    public static List<TokenRangeReplicas> normalize(List<TokenRangeReplicas> ranges)
    {
        List<TokenRangeReplicas> output = new ArrayList<>(ranges.size());

        if (ranges.stream().noneMatch(r -> r.partitioner.minToken.compareTo(r.start()) == 0))
        {
            LOGGER.warn("{} based minToken does not exist in the token ranges", Partitioner.class.getName());
        }

        for (TokenRangeReplicas range : ranges)
            output.addAll(range.unwrap());

        return deoverlap(output);
    }

    /**
     * Given a list of unwrapped (around the starting/min value) token ranges and their replica-sets, return list of
     * ranges with no overlaps. Any impacted range absorbs the replica-sets from the overlapping range.
     * This is to ensure that we have most coverage while using the replica-sets as write-replicas.
     * Overlaps are removed by splitting the original range around the overlap boundaries, resulting in sub-ranges
     * with replicas from all the overlapping replicas.
     *
     *
     * <pre>
     * Illustration:
     * Input with C overlapping with A and B
     *   |----------A-----------||----------B-------------|
     *                  |--------C----------|
     *
     * Split result: C is split first which further splits A and B to create
     *  |-----------A----------||----------B-------------|
     *                 |---C---|----C'----|
     *
     * Subsets C & C' are merged into supersets A and B by splitting them. Replica-sets for A,C and B,C are merged
     * for the resulting ranges.
     *  |-----A------|----AC---||---BC-----|-----B------|
     *  </pre>
     */
    private static List<TokenRangeReplicas> deoverlap(List<TokenRangeReplicas> ranges)
    {
        if (ranges.isEmpty())
            return ranges;

        Collections.sort(ranges);
        List<TokenRangeReplicas> output = new ArrayList<>();

        Iterator<TokenRangeReplicas> iter = ranges.iterator();
        TokenRangeReplicas current = iter.next();
        Partitioner partitioner = current.partitioner;

        while (iter.hasNext())
        {
            TokenRangeReplicas next = iter.next();
            if (!next.intersects(current))
            {
                // No overlaps found add current range
                output.add(current);
                current = next;
                continue;
            }

            if (current.equals(next))
            {
                current = new TokenRangeReplicas(current.start, current.end, current.partitioner,
                                                 mergeReplicas(current, next));
                LOGGER.debug("Merged identical token ranges. TokenRangeReplicas={}", current);
            }
            // Intersection handling of subset case i.e. when either range is a subset of the other
            // Since ranges are "unwrapped" and sorted, we  also treat intersections from a range that ends at the
            // "min" (and following ranges) to fall under the subset case.
            else if (current.end.compareTo(partitioner.minToken) == 0
                     || current.isSubsetOf(next) || next.isSubsetOf(current))
            {
                current = processSubsetCases(partitioner, output, iter, current, next);
            }
            else
            {
                current = processOverlappingRanges(partitioner, output, current, next);
            }
        }
        if (current != null)
            output.add(current);
        return output;
    }

    private static TokenRangeReplicas processOverlappingRanges(Partitioner partitioner,
                                                               List<TokenRangeReplicas> output,
                                                               TokenRangeReplicas current,
                                                               TokenRangeReplicas next)
    {
        output.addAll(splitOverlappingRanges(current, next));
        current = new TokenRangeReplicas(current.end, next.end, partitioner, next.replicaSet);
        return current;
    }

    private static TokenRangeReplicas processSubsetCases(Partitioner partitioner,
                                                         List<TokenRangeReplicas> output,
                                                         Iterator<TokenRangeReplicas> iter,
                                                         TokenRangeReplicas current,
                                                         TokenRangeReplicas next)
    {
        LOGGER.debug("Processing subset token ranges. current={}, next={}", current, next);
        TokenRangeReplicas outer = current.isLarger(next) ? current : next;
        TokenRangeReplicas inner = outer.equals(current) ? next : current;

        // Pre-calculate the overlap segment which is common for the sub-cases below
        processOverlappingPartOfRange(partitioner, output, current, next, inner);

        // 1. Subset shares the start of the range
        if (outer.start.compareTo(inner.start) == 0)
        {
            // Override last segment as "current" as there could be other overlaps with subsequent segments
            current = new TokenRangeReplicas(inner.end, outer.end, partitioner, outer.replicaSet);
        }
        // 2. Subset shares the end of the range
        else if (outer.end.compareTo(inner.end) == 0)
        {
            current = processSubsetWithSharedEnd(partitioner, output, iter, outer, inner);
        }
        // 3. Subset is in-between the range
        else
        {
            current = processContainedSubset(partitioner, output, outer, inner);
        }
        return current;
    }

    private static TokenRangeReplicas processContainedSubset(Partitioner partitioner,
                                                             List<TokenRangeReplicas> output,
                                                             TokenRangeReplicas outer,
                                                             TokenRangeReplicas inner)
    {
        TokenRangeReplicas current;
        TokenRangeReplicas previous = new TokenRangeReplicas(outer.start, inner.start, partitioner, outer.replicaSet);
        output.add(previous);
        // Override last segment as "current" as there could be other overlaps with subsequent segments
        current = new TokenRangeReplicas(inner.end, outer.end, partitioner, outer.replicaSet);
        return current;
    }

    private static TokenRangeReplicas processSubsetWithSharedEnd(Partitioner partitioner,
                                                                 List<TokenRangeReplicas> output,
                                                                 Iterator<TokenRangeReplicas> iter,
                                                                 TokenRangeReplicas outer,
                                                                 TokenRangeReplicas inner)
    {
        TokenRangeReplicas current;
        TokenRangeReplicas previous = new TokenRangeReplicas(outer.start, inner.start, partitioner,
                                                             outer.replicaSet);
        output.add(previous);
        // Move pointer forward since we are done processing both current & next
        // Return if we have processed the last range
        current = (iter.hasNext()) ? iter.next() : null;
        return current;
    }

    private static void processOverlappingPartOfRange(Partitioner partitioner,
                                                      List<TokenRangeReplicas> output,
                                                      TokenRangeReplicas current,
                                                      TokenRangeReplicas next,
                                                      TokenRangeReplicas inner)
    {
        TokenRangeReplicas overlap = new TokenRangeReplicas(inner.start, inner.end, partitioner,
                                                            mergeReplicas(current, next));
        output.add(overlap);
    }

    private static Set<String> mergeReplicas(TokenRangeReplicas current, TokenRangeReplicas next)
    {
        Set<String> merged = new HashSet<>(current.replicaSet);
        merged.addAll(next.replicaSet);
        return merged;
    }

    private static List<TokenRangeReplicas> splitOverlappingRanges(TokenRangeReplicas current,
                                                                   TokenRangeReplicas next)
    {
        Partitioner partitioner = current.partitioner;
        TokenRangeReplicas part = new TokenRangeReplicas(current.start,
                                                         next.start,
                                                         partitioner,
                                                         current.replicaSet);
        // Split current at starting point of next; add to result; add new overlap to set
        Set<String> mergedReplicaSet = Stream.concat(current.replicaSet.stream(), next.replicaSet.stream())
                                             .collect(Collectors.toSet());
        TokenRangeReplicas overlap = new TokenRangeReplicas(next.start,
                                                            current.end,
                                                            partitioner,
                                                            mergedReplicaSet);
        LOGGER.debug("Overlapping token ranges split into part={}, overlap={}", part, overlap);
        return Arrays.asList(part, overlap);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return String.format("%s - %s : %s:%s", start.toString(), end.toString(), replicaSet, partitioner);
    }
}
