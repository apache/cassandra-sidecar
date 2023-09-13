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

package org.apache.cassandra.sidecar.adapters.base;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;


/**
 * Representation of a token range (exclusive start and inclusive end - (start, end]) and the
 * corresponding mapping to replica-set hosts. Static factory ensures that ranges are always unwrapped.
 * Note: Range comparisons are used for ordering of ranges. eg. A.compareTo(B) <= 0 implies that
 * range A occurs before range B, not their sizes.
 */
public class TokenRangeReplicas implements Comparable<TokenRangeReplicas>
{
    private final BigInteger start;
    private final BigInteger end;

    private final Partitioner partitioner;

    private final Set<String> replicaSet;

    private static final Logger LOGGER = LoggerFactory.getLogger(TokenRangeReplicas.class);

    private TokenRangeReplicas(BigInteger start, BigInteger end, Partitioner partitioner, Set<String> replicaSet)
    {
        this.start = start;
        this.end = end;
        this.partitioner = partitioner;
        this.replicaSet = replicaSet;
    }

    public static List<TokenRangeReplicas> generateTokenRangeReplicas(BigInteger start,
                                                                      BigInteger end,
                                                                      Partitioner partitioner,
                                                                      Set<String> replicaSet)
    {
        if (start.compareTo(end) > 0)
        {
            return unwrapRange(start, end, partitioner, replicaSet);
        }

        LOGGER.info("Generating replica-map for range: {} - {} : Replicaset: {}", start, end, replicaSet);
        return Collections.singletonList(new TokenRangeReplicas(start, end, partitioner, replicaSet));
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
        validateRangesForComparison(other);
        int compareStart = this.start.compareTo(other.start);
        return (compareStart != 0) ? compareStart : this.end.compareTo(other.end);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        TokenRangeReplicas that = (TokenRangeReplicas) o;

        return Objects.equals(start, that.start)
               && Objects.equals(end, that.end)
               && partitioner == that.partitioner
               && replicaSet.equals(that.replicaSet);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(start, end, partitioner);
    }

    private void validateRangesForComparison(@NotNull TokenRangeReplicas other)
    {
        if (this.partitioner != other.partitioner)
            throw new IllegalStateException("Token ranges being compared do not have the same partitioner");
    }

    boolean contains(TokenRangeReplicas other)
    {
        validateRangesForComparison(other);
        return (other.start.compareTo(this.start) >= 0 && other.end.compareTo(this.end) <= 0);
    }

    /**
     * Determines intersection if the next range starts before the current range ends. This method assumes that
     * the provided ranges are sorted and unwrapped.
     * When the current range goes all the way to the end, we determine intersection if the next range starts
     * after the current since all subsequent ranges have to be subsets.
     *
     * @param other the range we are currently processing to check if "this" intersects it
     * @return true if "this" range intersects the other
     */
    boolean intersects(TokenRangeReplicas other)
    {
        boolean inOrder = this.compareTo(other) <= 0;
        TokenRangeReplicas first = inOrder ? this : other;
        TokenRangeReplicas last = inOrder ? other : this;
        return first.end.compareTo(last.start) > 0 && first.start.compareTo(last.end) < 0;
    }

    /**
     * Unwraps the token range if it wraps-around to end either on or after the least token by overriding such
     * ranges to end at the partitioner max-token value in the former case and splitting into 2 ranges in the latter
     * case.
     *
     * @return list of split ranges
     */
    private static List<TokenRangeReplicas> unwrapRange(BigInteger start,
                                                        BigInteger end,
                                                        Partitioner partitioner,
                                                        Set<String> replicaSet)
    {

        // Range ending at minToken is "unwrapped" to end at the maxToken.
        // Note: These being open-closed ranges, this will result in exclusion of partitioner's minToken from
        // allocation. This is by-design as it is never assigned to a node in Cassandra:
        // https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/dht/IPartitioner.java#L77
        if (end.compareTo(partitioner.minToken) == 0)
        {
            return Collections.singletonList(
            new TokenRangeReplicas(start, partitioner.maxToken, partitioner, replicaSet));
        }
        else if (start.compareTo(partitioner.maxToken) == 0)
        {
            return Collections.singletonList(
            new TokenRangeReplicas(partitioner.minToken, end, partitioner, replicaSet));
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
     * <p>
     * For an overlapping range that is included in both natural and pending ranges, say R_natural and R_pending
     * (where R_natural == R_pending), the replicas of both R_natural and R_pending should receive writes.
     * Therefore, the write-replicas of such range is the union of both replica sets.
     * This method implements the consolidation process.
     *
     * @param ranges
     * @return sorted list of non-overlapping ranges and replica-sets
     */
    public static List<TokenRangeReplicas> normalize(List<TokenRangeReplicas> ranges)
    {

        if (ranges.stream().noneMatch(r -> r.partitioner.minToken.compareTo(r.start()) == 0))
        {
            LOGGER.warn("{} based minToken does not exist in the token ranges", Partitioner.class.getName());
        }

        return deoverlap(ranges);
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
     *
     *  </pre>
     */
    private static List<TokenRangeReplicas> deoverlap(List<TokenRangeReplicas> allRanges)
    {
        if (allRanges.isEmpty())
            return allRanges;

        LOGGER.debug("Token ranges to be normalized: {}", allRanges);
        List<TokenRangeReplicas> ranges = mergeIdenticalRanges(allRanges);

        List<TokenRangeReplicas> output = new ArrayList<>();
        Iterator<TokenRangeReplicas> iter = ranges.iterator();
        TokenRangeReplicas current = iter.next();

        while (iter.hasNext())
        {
            TokenRangeReplicas next = iter.next();
            if (!current.intersects(next))
            {
                output.add(current);
                current = next;
            }
            else
            {
                current = processIntersectingRanges(output, iter, current, next);
            }
        }
        if (current != null)
            output.add(current);
        return output;
    }

    private static List<TokenRangeReplicas> mergeIdenticalRanges(List<TokenRangeReplicas> ranges)
    {
        Map<TokenRangeReplicas, Set<String>> rangeMapping = new HashMap<>();
        for (TokenRangeReplicas r: ranges)
        {
            if (!rangeMapping.containsKey(r))
            {
                rangeMapping.put(r, r.replicaSet);
            }
            else
            {
                rangeMapping.get(r).addAll(r.replicaSet);
            }
        }

        List<TokenRangeReplicas> merged = new ArrayList<>();
        for (Map.Entry<TokenRangeReplicas, Set<String>> entry : rangeMapping.entrySet())
        {
            TokenRangeReplicas r = entry.getKey();
            if (!r.replicaSet().equals(entry.getValue()))
            {
                r.replicaSet().addAll(entry.getValue());
            }
            merged.add(r);
        }
        Collections.sort(merged);
        return merged;
    }

    /**
     * Splits intersecting token ranges starting from the provided cursors and the iterator, while accumulating
     * overlapping replicas into each sub-range.
     * <p>
     * The algorithm 1) extracts all intersecting ranges at the provided cursor, and 2) Maintains a min-heap of all
     * intersecting ranges ordered by the end of the range, so that the least common sub-range relative to the current
     * range can be extracted.
     *
     * @param output  ongoing list of resulting non-overlapping ranges
     * @param iter    iterator over the list of ranges
     * @param current cursor to the current, intersecting range
     * @param next    cursor to the intersecting range after the current range
     * @return cursor to the subsequent non-intersecting range
     */
    static TokenRangeReplicas processIntersectingRanges(List<TokenRangeReplicas> output,
                                                        Iterator<TokenRangeReplicas> iter,
                                                        TokenRangeReplicas current,
                                                        TokenRangeReplicas next)
    {
        // min-heap with a comparator comparing the ends of ranges
        PriorityQueue<TokenRangeReplicas> rangeHeap =
        new PriorityQueue<>((n1, n2) -> (!n1.end.equals(n2.end())) ?
                                        n1.end().compareTo(n2.end()) : n1.compareTo(n2));

        List<TokenRangeReplicas> intersectingRanges = new ArrayList<>();
        next = extractIntersectingRanges(intersectingRanges::add, iter, current, next);
        rangeHeap.add(intersectingRanges.get(0));
        intersectingRanges.stream().skip(1).forEach(r -> {
            if (!rangeHeap.isEmpty())
            {
                TokenRangeReplicas range = rangeHeap.peek();
                // Use the last processed range's end as the new range's start
                // Except when its the first range, in which case, we use the queue-head's start
                BigInteger newStart = output.isEmpty() ? range.start() : output.get(output.size() - 1).end();

                if (r.start().compareTo(rangeHeap.peek().end()) == 0)
                {
                    output.add(new TokenRangeReplicas(newStart,
                                                      r.start(),
                                                      range.partitioner,
                                                      getBatchReplicas(rangeHeap)));
                    rangeHeap.poll();
                }
                else if (r.start().compareTo(rangeHeap.peek().end()) > 0)
                {
                    output.add(new TokenRangeReplicas(newStart,
                                                      range.end(),
                                                      range.partitioner,
                                                      getBatchReplicas(rangeHeap)));
                    rangeHeap.poll();
                }
                // Start-token is before the first intersecting range end. We have not encountered end of the range, so
                // it is not removed from the heap yet.
                else
                {
                    if (newStart.compareTo(r.start()) != 0)
                    {
                        output.add(new TokenRangeReplicas(newStart,
                                                          r.start(),
                                                          range.partitioner,
                                                          getBatchReplicas(rangeHeap)));
                    }
                }
                rangeHeap.add(r);
            }
        });

        // Remaining intersecting ranges from heap are processed
        while (!rangeHeap.isEmpty())
        {
            LOGGER.debug("Non-empty heap while resolving intersecting ranges:" + rangeHeap.size());
            TokenRangeReplicas nextVal = rangeHeap.peek();
            BigInteger newStart = output.isEmpty() ? nextVal.start() : output.get(output.size() - 1).end();
            // Corner case w/ common end ranges - we do not add redundant single token range
            if (newStart.compareTo(nextVal.end()) != 0)
            {
                output.add(new TokenRangeReplicas(newStart,
                                                  nextVal.end(),
                                                  nextVal.partitioner,
                                                  getBatchReplicas(rangeHeap)));
            }
            rangeHeap.poll();
        }
        return next;
    }

    /**
     * Extract all the intersecting ranges starting from the current cursor, which we know is intersecting with the
     * next range. Note that the cursor is moved forward until a non-intersecting range is found.
     *
     * @param rangeConsumer functional interface to collect candidate intersecting ranges
     * @param iter          ongoing iterator over the entire range-set
     * @param current       cursor to the current, intersecting range
     * @param next          cursor to the next intersecting range
     * @return list of intersecting ranges starting at the specified cursor
     */
    private static TokenRangeReplicas extractIntersectingRanges(Consumer<TokenRangeReplicas> rangeConsumer,
                                                                Iterator<TokenRangeReplicas> iter,
                                                                TokenRangeReplicas current,
                                                                TokenRangeReplicas next)
    {
        // we know that current and next intersect
        rangeConsumer.accept(current);
        rangeConsumer.accept(next);
        current = (current.contains(next)) ? current : next;
        next = null;
        while (iter.hasNext())
        {
            next = iter.next();
            if (!current.intersects(next))
            {
                break;
            }
            rangeConsumer.accept(next);
            // when next is subset of current, we keep tracking current
            current = (current.contains(next)) ? current : next;
            next = null;
        }
        return next;
    }

    // TODO: Verify why we need all replicas from queue
    private static Set<String> getBatchReplicas(PriorityQueue<TokenRangeReplicas> rangeHeap)
    {
        return rangeHeap.stream()
                        .map(TokenRangeReplicas::replicaSet)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
    }

    private static Set<String> mergeReplicas(TokenRangeReplicas current, TokenRangeReplicas next)
    {
        Set<String> merged = new HashSet<>(current.replicaSet);
        merged.addAll(next.replicaSet);
        return merged;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return String.format("Range(%s, %s]: %s:%s", start.toString(), end.toString(), replicaSet, partitioner);
    }
}
