/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.utils;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Host;
import com.google.inject.Singleton;
import org.apache.cassandra.bridge.TokenRange;
import org.apache.cassandra.sidecar.cdc.CdcConfig;
import org.apache.cassandra.sidecar.db.ServiceConfig;
import org.apache.cassandra.sidecar.db.TokenSplitConfigAccessor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

/**
 * Util class that divides token range into fixed number of splits to help distribute load.
 * The number of splits should be proportional to the cluster size, so that small clusters
 * store state in a small number of partitions, and large clusters store state across a
 * larger number of partitions.
 */
@Singleton
public class TokenSplitUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TokenSplitUtil.class);

    public static final Map<Partitioner, BigInteger> PARTITIONER_FULL_RANGE = Map.of(
    Partitioner.Murmur3Partitioner, BigInteger.valueOf(2).pow(64),
    Partitioner.RandomPartitioner, BigInteger.valueOf(2).pow(127)
    );

    public static final String NUM_SPLITS_KEY = "curr_num_splits";
    // we can in the future add a current/prior split count so we can migrate from one to another after a cluster topology event
//    public static final String PRIOR_NUM_SPLITS_KEY = "prior_num_splits";

    public final int numSplits;
    public final List<BigInteger> murmur3Tokens;
    public final List<BigInteger> randomTokens;

    @VisibleForTesting
    public TokenSplitUtil(int numSplits)
    {
        this.numSplits = numSplits;
        this.murmur3Tokens = splitTokens(numSplits, Partitioner.Murmur3Partitioner);
        this.randomTokens = splitTokens(numSplits, Partitioner.RandomPartitioner);
        LOGGER.info("Initialized token split util numSplits={}", numSplits);
    }

    public TokenSplitUtil(TokenSplitConfigAccessor tokenSplitConfigAccessor, CdcConfig cdcConfig, InstanceMetadataFetcher fetcher)
    {
        this(getOrInsert(tokenSplitConfigAccessor, cdcConfig, fetcher));
    }

    protected static int getOrInsert(TokenSplitConfigAccessor tokenSplitConfigAccessor, CdcConfig cdcConfig, InstanceMetadataFetcher fetcher)
    {
        Map<String, String> config = tokenSplitConfigAccessor.getConfig().getConfigs();
        if (config != null && config.containsKey(NUM_SPLITS_KEY))
        {
            return Integer.parseInt(config.get(NUM_SPLITS_KEY));
        }

        int maxDcSize = fetcher.callOnFirstAvailableInstance(instanceMetadata -> instanceMetadata.delegate().metadata())
                               .getAllHosts()
                               .stream()
                               .filter(host -> host.getDatacenter().equals(cdcConfig.datacenter()))
                               .collect(Collectors.groupingBy(Host::getDatacenter, Collectors.toList()))
                               .values().stream()
                               .mapToInt(List::size)
                               .max()
                               .orElseThrow(() -> new RuntimeException("Unable to map "));

        int numSplits = (int) Math.ceil((double) maxDcSize / 2);
        LOGGER.info("Initializing token split maxDcSize={} numSplits={}", maxDcSize, numSplits);
        Optional<ServiceConfig> op = tokenSplitConfigAccessor.storeConfigIfNotExists(Map.of(NUM_SPLITS_KEY, Integer.toString(numSplits)));
        if (op.isPresent())
        {
            // we won the CAS
            return maxDcSize;
        }

        // we lost the CAS so re-load config
        config = tokenSplitConfigAccessor.getConfig().getConfigs();
        if (config == null || !config.containsKey(NUM_SPLITS_KEY))
        {
            throw new RuntimeException("Failed to get or insert max_splits into config");
        }
        return Integer.parseInt(config.get(NUM_SPLITS_KEY));
    }

    public static boolean overlaps(TokenRange range, BigInteger lower, BigInteger upper)
    {
        return overlaps(range, TokenRange.openClosed(lower, upper));
    }

    public static boolean overlaps(TokenRange range, TokenRange otherRange)
    {
        return otherRange.upperEndpoint().compareTo(range.lowerEndpoint()) > 0 &&
               otherRange.lowerEndpoint().compareTo(range.upperEndpoint()) < 0;
    }

    /**
     * Return split identifiers that overlap with `range`
     *
     * @param partitioner cluster partitioner
     * @param range       token range
     * @return subset of splits  in the range 0..&lt;MAX_SPLITS that overlap with token `range`.
     */
    public int[] findOverlappingSplitIds(Partitioner partitioner, TokenRange range)
    {
        int[] ar = findOverlappingSplitIds(splitPartitionerTokens(partitioner), range);
        Arrays.stream(ar).forEach(i -> Preconditions.checkArgument(i >= 0 && i < numSplits));
        return ar;
    }

    public List<BigInteger> splitPartitionerTokens(Partitioner partitioner)
    {
        switch (partitioner)
        {
            case Murmur3Partitioner:
                return murmur3Tokens;
            case RandomPartitioner:
                return randomTokens;
        }
        throw new IllegalArgumentException("Unknown partitioner: " + partitioner);
    }

    // static helper methods

    public static List<BigInteger> splitTokens(int splits, Partitioner partitioner)
    {
        BigInteger fullRange = PARTITIONER_FULL_RANGE.get(partitioner);
        if (fullRange == null)
        {
            throw new IllegalStateException("Unknown partitioner: " + partitioner);
        }
        BigInteger min = partitioner.minToken();
        BigInteger div = fullRange.divide(BigInteger.valueOf(splits));
        return IntStream.range(0, splits)
                        .mapToObj(i -> div.multiply(BigInteger.valueOf(i)).add(min))
                        .collect(Collectors.toList());
    }

    protected static int[] findOverlappingSplitIds(List<BigInteger> tokens, TokenRange range)
    {
        int start = findOverlappingSplitIds(tokens, range.lowerEndpoint(), BoundType.OPEN);
        int end = findOverlappingSplitIds(tokens, range.upperEndpoint(), BoundType.CLOSED);
        return IntStream.rangeClosed(start, end).toArray();
    }

    protected static int findOverlappingSplitIds(List<BigInteger> tokens, BigInteger token, BoundType boundType)
    {
        // NOTE: we assume open-closed bounds for `tokens` list
        int pos = Collections.binarySearch(tokens, token);
        if (pos < 0)
        {
            // Collections.binarySearch returns a negative number as (-(insertion point) - 1) for inexact matches
            // providing the index the where the element would be inserted to maintain sort order.
            pos = -(pos + 2);
        }
        else if (boundType == BoundType.CLOSED)
        {
            // range is open-closed, so if exact match on CLOSED then it is the
            // end marker of the range so appears in preceding token range.
            pos--;
        }
        return pos;
    }
}
