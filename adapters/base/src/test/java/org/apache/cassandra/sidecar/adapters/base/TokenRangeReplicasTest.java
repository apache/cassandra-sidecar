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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for TokenRangeReplicas
 */
public class TokenRangeReplicasTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TokenRangeReplicasTest.class);

    // non-overlapping ranges
    @Test
    public void simpleTest()
    {
        List<TokenRangeReplicas> simpleList = createSimpleTokenRangeReplicaList();
        LOGGER.info("Input:" + simpleList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(simpleList);
        LOGGER.info("Result:" + rangeList);
        assertThat(hasOverlaps(rangeList)).isFalse();
        assertThat(simpleList).containsExactlyInAnyOrderElementsOf(rangeList);
    }

    @Test
    public void subRangeTest()
    {
        List<TokenRangeReplicas> subRangeList = createOverlappingTokenRangeReplicaList();
        LOGGER.info("Input:" + subRangeList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(subRangeList);
        LOGGER.info("Result:" + rangeList);

        assertThat(hasOverlaps(rangeList)).isFalse();
        // Validate that we have 1 additional list as a result of the splits
        assertThat(rangeList).hasSize(subRangeList.size() + 1);

        // Validate that there is a merged range with 20-30 with hosts h4-h7
        List<TokenRangeReplicas> expectedExists =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("20"),
                                                      new BigInteger("30"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h4", "h5", "h6", "h7")));
        // Validate absence of larger list
        List<TokenRangeReplicas> expectedNotExists =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("40"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h4", "h5")));

        assertThat(isPartOfRanges(expectedExists.get(0), rangeList)).isTrue();
        assertThat(isPartOfRanges(expectedNotExists.get(0), rangeList)).isFalse();
    }

    @Test
    public void processIntersectionTest()
    {
        List<TokenRangeReplicas> subRangeList = createIntersectingTokenRangeReplicaList();
        List<TokenRangeReplicas> output = new ArrayList<>();
        Iterator<TokenRangeReplicas> iter = subRangeList.iterator();
        TokenRangeReplicas curr = iter.next();
        TokenRangeReplicas next = iter.next();
        assertThat(hasOverlaps(subRangeList)).isTrue();
        LOGGER.info("Input:" + subRangeList);
        TokenRangeReplicas.processIntersectingRanges(output,
                                                     iter,
                                                     curr,
                                                     next);
        LOGGER.info("Result:" + output);
        assertThat(hasOverlaps(output)).isFalse();
        assertThat(output).hasSize(6);
        List<TokenRangeReplicas> expectedExists =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("20"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h1", "h2", "h3")));

        List<TokenRangeReplicas> expectedExists2 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("20"),
                                                      new BigInteger("30"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h1", "h2", "h3", "h4", "h5")));

        List<TokenRangeReplicas> expectedExists3 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("30"),
                                                      new BigInteger("40"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h1", "h2", "h3", "h4", "h5", "h6", "h7")));

        assertThat(isPartOfRanges(expectedExists.get(0), output)).isTrue();
        assertThat(isPartOfRanges(expectedExists2.get(0), output)).isTrue();
        assertThat(isPartOfRanges(expectedExists3.get(0), output)).isTrue();
    }

    @Test
    public void processIntersectionWithSubsetRangeTest()
    {
        List<TokenRangeReplicas> subRangeList = createIntersectingTokenRangeReplicaList2();
        List<TokenRangeReplicas> output = new ArrayList<>();
        Iterator<TokenRangeReplicas> iter = subRangeList.iterator();
        TokenRangeReplicas curr = iter.next();
        TokenRangeReplicas next = iter.next();
        assertThat(hasOverlaps(subRangeList)).isTrue();
        LOGGER.info("Input:" + subRangeList);
        TokenRangeReplicas.processIntersectingRanges(output,
                                                     iter,
                                                     curr,
                                                     next);
        LOGGER.info("Result:" + output);
        assertThat(hasOverlaps(output)).isFalse();
        assertThat(output).hasSize(5);
        List<TokenRangeReplicas> expectedExists =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("20"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h1", "h2", "h3")));

        List<TokenRangeReplicas> expectedExists2 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("20"),
                                                      new BigInteger("30"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h1", "h2", "h3", "h4", "h5")));

        List<TokenRangeReplicas> expectedExists3 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("30"),
                                                      new BigInteger("40"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h1", "h2", "h3", "h6", "h7")));

        assertThat(isPartOfRanges(expectedExists.get(0), output)).isTrue();
        assertThat(isPartOfRanges(expectedExists2.get(0), output)).isTrue();
        assertThat(isPartOfRanges(expectedExists3.get(0), output)).isTrue();
    }

    @Test
    public void processIntersectionWithMultipleSubsetRangeTest()
    {
        List<TokenRangeReplicas> subRangeList = createIntersectingTokenRangeReplicaList3();
        List<TokenRangeReplicas> output = new ArrayList<>();
        Iterator<TokenRangeReplicas> iter = subRangeList.iterator();
        TokenRangeReplicas curr = iter.next();
        TokenRangeReplicas next = iter.next();
        assertThat(hasOverlaps(subRangeList)).isTrue();
        LOGGER.info("Input:" + subRangeList);
        TokenRangeReplicas.processIntersectingRanges(output,
                                                     iter,
                                                     curr,
                                                     next);
        LOGGER.info("Result:" + output);
        assertThat(hasOverlaps(output)).isFalse();
        assertThat(output).hasSize(6);
        List<TokenRangeReplicas> expectedExists =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("15"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h1", "h2", "h3")));

        List<TokenRangeReplicas> expectedExists2 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("15"),
                                                      new BigInteger("20"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h1", "h2", "h3", "h4", "h5")));

        List<TokenRangeReplicas> expectedExists3 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("30"),
                                                      new BigInteger("35"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h9", "h1", "h2", "h3", "h4", "h5")));

        assertThat(isPartOfRanges(expectedExists.get(0), output)).isTrue();
        assertThat(isPartOfRanges(expectedExists2.get(0), output)).isTrue();
        assertThat(isPartOfRanges(expectedExists3.get(0), output)).isTrue();
    }

    // Validate merge-split resulting from 2 ranges overlapping
    @Test
    public void partialOverlapTest()
    {
        List<TokenRangeReplicas> partialOverlapList = createPartialOverlappingTokenRangeReplicaList();
        LOGGER.info("Input:" + partialOverlapList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(partialOverlapList);
        LOGGER.info("Result:" + rangeList);
        assertThat(hasOverlaps(rangeList)).isFalse();
        // Validate that we have 1 additional list as a result of the splits
        assertThat(rangeList).hasSize(partialOverlapList.size() + 1);

        List<TokenRangeReplicas> expectedExists =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("15"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h4", "h5")));
        List<TokenRangeReplicas> expectedExists2 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("15"),
                                                      new BigInteger("20"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h4", "h5", "h6", "h7")));
        List<TokenRangeReplicas> expectedExists3 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("20"),
                                                      new BigInteger("30"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h6", "h7")));
        List<TokenRangeReplicas> expectedNotExists =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("20"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h4", "h5")));
        List<TokenRangeReplicas> expectedNotExists2 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("15"),
                                                      new BigInteger("30"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h6", "h7")));

        assertThat(checkContains(rangeList, expectedExists.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedExists2.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedExists3.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedNotExists.get(0))).isFalse();
        assertThat(checkContains(rangeList, expectedNotExists2.get(0))).isFalse();
    }

    // Validate merge-split resulting from 3 consecutive ranges overlapping
    @Test
    public void multiOverlapTest()
    {
        List<TokenRangeReplicas> multiOverlapList = createMultipleOverlappingTokenRangeReplicaList();
        LOGGER.info("Input:" + multiOverlapList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(multiOverlapList);
        LOGGER.info("Result:" + rangeList);
        assertThat(hasOverlaps(rangeList)).isFalse();
        // Validate that we have 1 additional list as a result of the splits
        assertThat(rangeList).hasSize(multiOverlapList.size() + 1);

        List<TokenRangeReplicas> expectedExists =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("15"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h1", "h2", "h3", "h4", "h5")));
        List<TokenRangeReplicas> expectedExists2 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("15"),
                                                      new BigInteger("25"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h4", "h5", "h6", "h7")));
        List<TokenRangeReplicas> expectedNotExists =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("25"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h4", "h5")));

        assertThat(checkContains(rangeList, expectedExists.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedExists2.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedNotExists.get(0))).isFalse();
    }

    //     Validate merge-split from wrapped overlapping ranges
    @Test
    public void wrappedMultiOverlapTest()
    {
        List<TokenRangeReplicas> overlapList = createUnwrappedMultipleOverlappingTokenRangeReplicaList();
        LOGGER.info("Input:" + overlapList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(overlapList);
        LOGGER.info("Result:" + rangeList);
        assertThat(hasOverlaps(rangeList)).isFalse();

        // Validate that we have 1 additional ranges as a result of the merges and splits
        assertThat(rangeList).hasSize(overlapList.size() + 1);

        List<TokenRangeReplicas> expectedExists =
        TokenRangeReplicas.generateTokenRangeReplicas(Partitioner.Random.minToken,
                                                      new BigInteger("10"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h9", "h1", "h2", "h3")));
        List<TokenRangeReplicas> expectedExists2 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("30"),
                                                      new BigInteger("35"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9", "h6", "h7")));
        List<TokenRangeReplicas> expectedExists3 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("35"),
                                                      Partitioner.Random.maxToken,
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9")));
        List<TokenRangeReplicas> expectedNotExists =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("30"),
                                                      new BigInteger("10"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9")));

        assertThat(checkContains(rangeList, expectedExists.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedExists2.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedExists3.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedNotExists.get(0))).isFalse();
    }

    @Test
    public void wrappedOverlapTest()
    {
        List<TokenRangeReplicas> wrappedOverlapList = createWrappedOverlappingTokenRangeReplicaList();
        LOGGER.info("Input:" + wrappedOverlapList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(wrappedOverlapList);
        LOGGER.info("Result:" + rangeList);
        assertThat(hasOverlaps(rangeList)).isFalse();

        // Split & Merge should result in same number of ranges
        // (-1, 10] and (-1, 20] results in (-1, 10] and (10, 20]
        // (35, max] and (40, mx] results in (35, 40] and (40, max]
        assertThat(rangeList).hasSize(wrappedOverlapList.size());

        List<TokenRangeReplicas> expectedExists =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("40"),
                                                      Partitioner.Random.maxToken,
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h9", "h1", "h2", "h3")));
        List<TokenRangeReplicas> expectedExists2 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("35"),
                                                      new BigInteger("40"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9")));
        List<TokenRangeReplicas> expectedExists3 =
        TokenRangeReplicas.generateTokenRangeReplicas(Partitioner.Random.minToken,
                                                      new BigInteger("10"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9", "h4", "h5")));
        List<TokenRangeReplicas> expectedNotExists =
        TokenRangeReplicas.generateTokenRangeReplicas(Partitioner.Random.minToken,
                                                      new BigInteger("20"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h4", "h5")));

        assertThat(checkContains(rangeList, expectedExists.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedExists2.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedExists3.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedNotExists.get(0))).isFalse();
    }

    // Validate case when the partitioner min token does not match the least token value in the ring
    @Test
    public void wrappedOverlapNonMatchingMinTokenTest()
    {
        List<TokenRangeReplicas> wrappedOverlapList = createWrappedOvlNonMatchingMinTokenList();
        LOGGER.info("Input:" + wrappedOverlapList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(wrappedOverlapList);
        LOGGER.info("Result:" + rangeList);
        assertThat(hasOverlaps(rangeList)).isFalse();

        // Split & Merge should result in same number of ranges
        // (-1, 5] and (-1, 10] results in (-1, 5] and (5, 10]
        // (5, 20] results in (10, 20]
        // (35, max] and (40, mx] results in (35, 40] and (40, max]
        assertThat(rangeList).hasSize(wrappedOverlapList.size());

        List<TokenRangeReplicas> expectedExists =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("40"),
                                                      Partitioner.Random.maxToken,
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h9", "h1", "h2", "h3")));
        // New Token range resulting from non-matching min token
        List<TokenRangeReplicas> expectedExistsNew =
        TokenRangeReplicas.generateTokenRangeReplicas(Partitioner.Random.minToken,
                                                      new BigInteger("5"),
                                                      Partitioner.Random,
                                                      new HashSet<>(
                                                      Arrays.asList("h9", "h1", "h2", "h3")));
        List<TokenRangeReplicas> expectedExists2 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("35"),
                                                      new BigInteger("40"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Collections.singletonList("h9")));
        // Other split resulting from new range
        List<TokenRangeReplicas> expectedExists3 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("5"),
                                                      new BigInteger("10"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9", "h4", "h5")));
        List<TokenRangeReplicas> expectedNotExists =
        TokenRangeReplicas.generateTokenRangeReplicas(Partitioner.Random.minToken,
                                                      new BigInteger("20"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h4", "h5")));

        assertThat(checkContains(rangeList, expectedExists.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedExistsNew.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedExists2.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedExists3.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedNotExists.get(0))).isFalse();
    }

    // Test using actual ranges from a 3 node cluster
    @Test
    public void wrappedActualOverlapTest()
    {
        List<TokenRangeReplicas> createdList = new ArrayList<>();
        createdList.addAll(
        TokenRangeReplicas.generateTokenRangeReplicas(Partitioner.Murmur3.minToken,
                                                      new BigInteger("-3074457345618258603"),
                                                      Partitioner.Murmur3,
                                                      new HashSet<>(Arrays.asList("h2", "h3", "h1"))));
        createdList.addAll(
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("-3074457345618258603"),
                                                      new BigInteger("3074457345618258602"),
                                                      Partitioner.Murmur3,
                                                      new HashSet<>(Arrays.asList("h3", "h1", "h2"))));
        createdList.addAll(
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("3074457345618258602"),
                                                      new BigInteger("6148914691236517204"),
                                                      Partitioner.Murmur3,
                                                      new HashSet<>(Arrays.asList("h9"))));
        createdList.addAll(
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("-6148914691236517204"),
                                                      new BigInteger("3074457345618258602"),
                                                      Partitioner.Murmur3,
                                                      new HashSet<>(Arrays.asList("h9"))));
        createdList.addAll(
        TokenRangeReplicas.generateTokenRangeReplicas(Partitioner.Murmur3.minToken,
                                                      new BigInteger("-3074457345618258603"),
                                                      Partitioner.Murmur3,
                                                      new HashSet<>(Arrays.asList("h10"))));
        createdList.addAll(
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("6148914691236517204"),
                                                      Partitioner.Murmur3.minToken,
                                                      Partitioner.Murmur3,
                                                      new HashSet<>(Arrays.asList("h1", "h2", "h3"))));

        LOGGER.info("Input:" + createdList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(createdList);
        LOGGER.info("Result:" + rangeList);
        assertThat(hasOverlaps(rangeList)).isFalse();
        assertThat(rangeList).hasSize(5);

        List<TokenRangeReplicas> expectedExists = TokenRangeReplicas.generateTokenRangeReplicas(
        Partitioner.Murmur3.minToken, new BigInteger("-6148914691236517204"),
        Partitioner.Murmur3, new HashSet<>(Arrays.asList("h10", "h1", "h2", "h3")));
        List<TokenRangeReplicas> expectedExists2 = TokenRangeReplicas.generateTokenRangeReplicas(
        new BigInteger("-3074457345618258603"), new BigInteger("3074457345618258602"),
        Partitioner.Murmur3, new HashSet<>(Arrays.asList("h9", "h1", "h2", "h3")));
        List<TokenRangeReplicas> expectedExists3 = TokenRangeReplicas.generateTokenRangeReplicas(
        new BigInteger("3074457345618258602"), new BigInteger("6148914691236517204"),
        Partitioner.Murmur3, new HashSet<>(Arrays.asList("h9")));
        List<TokenRangeReplicas> expectedExists4 = TokenRangeReplicas.generateTokenRangeReplicas(
        new BigInteger("6148914691236517204"), Partitioner.Murmur3.maxToken,
        Partitioner.Murmur3, new HashSet<>(Arrays.asList("h1", "h2", "h3")));
        List<TokenRangeReplicas> expectedNotExists = TokenRangeReplicas.generateTokenRangeReplicas(
        new BigInteger("3074457345618258602"), Partitioner.Murmur3.minToken,
        Partitioner.Murmur3, new HashSet<>(Arrays.asList("h1", "h2", "h3")));

        assertThat(checkContains(rangeList, expectedExists.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedExists2.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedExists3.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedExists4.get(0))).isTrue();
        assertThat(checkContains(rangeList, expectedNotExists.get(0))).isFalse();
    }

    @Test
    void testSubsetRelationship()
    {
        // wraps around
        List<TokenRangeReplicas> range1 = TokenRangeReplicas.generateTokenRangeReplicas(
        BigInteger.valueOf(100), BigInteger.valueOf(-100), Partitioner.Murmur3, new HashSet<>());
        List<TokenRangeReplicas> range2 = TokenRangeReplicas.generateTokenRangeReplicas(
        BigInteger.valueOf(100), Partitioner.Murmur3.maxToken, Partitioner.Murmur3, new HashSet<>());
        List<TokenRangeReplicas> range3 = TokenRangeReplicas.generateTokenRangeReplicas(
        BigInteger.valueOf(100), BigInteger.valueOf(150), Partitioner.Murmur3, new HashSet<>());
        List<TokenRangeReplicas> range4 = TokenRangeReplicas.generateTokenRangeReplicas(
        BigInteger.valueOf(120), BigInteger.valueOf(150), Partitioner.Murmur3, new HashSet<>());
        assertThat(range1).hasSize(2);
        assertThat(range2).hasSize(1);
        assertThat(range3).hasSize(1);
        assertThat(range4).hasSize(1);
        assertThat(isPartOfRanges(range2.get(0), range1)).isTrue();
        assertThat(isPartOfRanges(range3.get(0), range2)).isTrue();
        assertThat(isPartOfRanges(range2.get(0), range3)).isFalse();
        assertThat(isPartOfRanges(range4.get(0), range3)).isTrue();
        assertThat(isPartOfRanges(range3.get(0), range4)).isFalse();
    }

    @Test
    void testIntersectRanges()
    {
        // Simple Intersection
        List<TokenRangeReplicas> range1 = TokenRangeReplicas.generateTokenRangeReplicas(
        BigInteger.valueOf(1), BigInteger.valueOf(10), Partitioner.Murmur3, new HashSet<>());
        List<TokenRangeReplicas> range2 = TokenRangeReplicas.generateTokenRangeReplicas(
        BigInteger.valueOf(9), BigInteger.valueOf(12), Partitioner.Murmur3, new HashSet<>());
        assertThat(intersectsWithRanges(range2.get(0), range1)).isTrue();
        // intersect check with out-of-order ranges
        assertThat(intersectsWithRanges(range1.get(0), range2)).isTrue();

        // Intersection at border
        List<TokenRangeReplicas> range3 = TokenRangeReplicas.generateTokenRangeReplicas(
        BigInteger.valueOf(1), BigInteger.valueOf(10), Partitioner.Murmur3, new HashSet<>());
        List<TokenRangeReplicas> range4 = TokenRangeReplicas.generateTokenRangeReplicas(
        BigInteger.valueOf(11), BigInteger.valueOf(20), Partitioner.Murmur3, new HashSet<>());


        assertThat(intersectsWithRanges(range4.get(0), range3)).isFalse();
        // intersect check with out-of-order ranges
        assertThat(intersectsWithRanges(range3.get(0), range4)).isFalse();

        // Intersection as superset
        List<TokenRangeReplicas> range5 = TokenRangeReplicas.generateTokenRangeReplicas(
        BigInteger.valueOf(1000), Partitioner.Murmur3.maxToken, Partitioner.Murmur3, new HashSet<>());
        List<TokenRangeReplicas> range6 = TokenRangeReplicas.generateTokenRangeReplicas(
        BigInteger.valueOf(200000), BigInteger.valueOf(300000), Partitioner.Murmur3, new HashSet<>());

        assertThat(intersectsWithRanges(range6.get(0), range5)).isTrue();
    }

    private boolean hasOverlaps(List<TokenRangeReplicas> rangeList)
    {
        Collections.sort(rangeList);
        for (int c = 0, i = 1; i < rangeList.size(); i++)
        {
            if (rangeList.get(c++).end().compareTo(rangeList.get(i).start()) > 0) return true;
        }
        return false;
    }

    private boolean checkContains(List<TokenRangeReplicas> resultList, TokenRangeReplicas expected)
    {
        return resultList.stream()
                         .map(TokenRangeReplicas::toString)
                         .anyMatch(r -> r.equals(expected.toString()));
    }

    private boolean isPartOfRanges(TokenRangeReplicas range, List<TokenRangeReplicas> rangeList)
    {
        return rangeList.stream().anyMatch(r -> r.contains(range));
    }

    private boolean intersectsWithRanges(TokenRangeReplicas range, List<TokenRangeReplicas> rangeList)
    {
        return rangeList.stream().anyMatch(r -> r.intersects(range));
    }

    private List<TokenRangeReplicas> createSimpleTokenRangeReplicaList()
    {
        List<TokenRangeReplicas> simpleList = Arrays.asList(
        TokenRangeReplicas.generateTokenRangeReplicas(Partitioner.Random.minToken,
                                                      new BigInteger("10"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h1", "h2", "h3"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("20"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h4", "h5"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("20"),
                                                      Partitioner.Random.minToken,
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h6", "h7"))).get(0)
        );
        assertThat(hasOverlaps(simpleList)).isFalse();
        return simpleList;
    }

    // 2. Simple single overlap (consuming) => superset + no changes to others [Merge]
    private List<TokenRangeReplicas> createOverlappingTokenRangeReplicaList()
    {
        List<TokenRangeReplicas> rangeWithOverlaps = Arrays.asList(
        TokenRangeReplicas.generateTokenRangeReplicas(Partitioner.Random.minToken,
                                                      new BigInteger("10"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h1", "h2", "h3"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("40"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h4", "h5"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("20"),
                                                      new BigInteger("30"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h6", "h7"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("40"),
                                                      Partitioner.Random.maxToken,
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9"))).get(0)
        );
        assertThat(hasOverlaps(rangeWithOverlaps)).isTrue();
        return rangeWithOverlaps;
    }

    // 3. Single overlap - cutting [Merge + Split]
    private List<TokenRangeReplicas> createPartialOverlappingTokenRangeReplicaList()
    {
        List<TokenRangeReplicas> rangeWithOverlaps = Arrays.asList(
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("-1"),
                                                      new BigInteger("10"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h1", "h2", "h3"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("20"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h4", "h5"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("15"),
                                                      new BigInteger("30"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h6", "h7"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("30"),
                                                      Partitioner.Random.maxToken,
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9"))).get(0)
        );
        assertThat(hasOverlaps(rangeWithOverlaps)).isTrue();
        return rangeWithOverlaps;
    }

    // 4. Multi-overlaps
    private List<TokenRangeReplicas> createMultipleOverlappingTokenRangeReplicaList()
    {
        List<TokenRangeReplicas> rangeWithOverlaps = Arrays.asList(
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("-1"),
                                                      new BigInteger("15"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h1", "h2", "h3"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("25"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h4", "h5"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("15"),
                                                      new BigInteger("30"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h6", "h7"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("30"),
                                                      Partitioner.Random.maxToken,
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9"))).get(0)
        );
        assertThat(hasOverlaps(rangeWithOverlaps)).isTrue();
        return rangeWithOverlaps;
        
    }

    // 5. Overlaps w/ wrap-around
    private List<TokenRangeReplicas> createUnwrappedMultipleOverlappingTokenRangeReplicaList()
    {

        List<TokenRangeReplicas> createdList = new ArrayList<>();
        createdList.addAll(
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("-1"),
                                                      new BigInteger("15"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h1", "h2", "h3"))));

        createdList.addAll(TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("15"),
                                                                         new BigInteger("20"),
                                                                         Partitioner.Random,
                                                                         new HashSet<>(Arrays.asList("h4", "h5"))));

        createdList.addAll(TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("20"),
                                                                         new BigInteger("35"),
                                                                         Partitioner.Random,
                                                                         new HashSet<>(Arrays.asList("h6", "h7"))));

        List<TokenRangeReplicas> wrappedList =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("30"),
                                                      new BigInteger("10"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9")));
        assertThat(wrappedList).hasSize(2);


        createdList.addAll(wrappedList);
        assertThat(hasWrappedRange(createdList)).isFalse();
        assertThat(hasOverlaps(createdList)).isTrue();
        return createdList;
    }

    private boolean hasWrappedRange(List<TokenRangeReplicas> createdList)
    {
        return createdList.stream().anyMatch(r -> r.start().compareTo(r.end()) > 0);
    }

    private List<TokenRangeReplicas> createIntersectingTokenRangeReplicaList()
    {
        List<TokenRangeReplicas> rangeWithOverlaps = Arrays.asList(
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("40"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h1", "h2", "h3"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("20"),
                                                      new BigInteger("60"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h4", "h5"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("30"),
                                                      new BigInteger("80"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h6", "h7"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("40"),
                                                      Partitioner.Random.maxToken,
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9"))).get(0)
        );
        assertThat(hasOverlaps(rangeWithOverlaps)).isTrue();
        return rangeWithOverlaps;
    }

    private List<TokenRangeReplicas> createIntersectingTokenRangeReplicaList2()
    {
        List<TokenRangeReplicas> rangeWithOverlaps = Arrays.asList(
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("40"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h1", "h2", "h3"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("20"),
                                                      new BigInteger("30"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h4", "h5"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("30"),
                                                      new BigInteger("80"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h6", "h7"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("40"),
                                                      Partitioner.Random.maxToken,
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9"))).get(0)
        );
        assertThat(hasOverlaps(rangeWithOverlaps)).isTrue();
        return rangeWithOverlaps;

    }

    private List<TokenRangeReplicas> createIntersectingTokenRangeReplicaList3()
    {
        List<TokenRangeReplicas> rangeWithOverlaps = Arrays.asList(
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("10"),
                                                      new BigInteger("40"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h1", "h2", "h3"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("15"),
                                                      new BigInteger("35"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h4", "h5"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("20"),
                                                      new BigInteger("30"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h6", "h7"))).get(0),
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("30"),
                                                      Partitioner.Random.maxToken,
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9"))).get(0)
        );
        assertThat(hasOverlaps(rangeWithOverlaps)).isTrue();
        return rangeWithOverlaps;

    }


    private List<TokenRangeReplicas> createWrappedOverlappingTokenRangeReplicaList()
    {
        List<TokenRangeReplicas> createdList = new ArrayList<>();

        createdList.addAll(TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("-1"),
                                                                         new BigInteger("20"),
                                                                         Partitioner.Random,
                                                                         new HashSet<>(Arrays.asList("h4", "h5"))));
        createdList.addAll(TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("20"),
                                                                         new BigInteger("35"),
                                                                         Partitioner.Random,
                                                                         new HashSet<>(Arrays.asList("h6", "h7"))));
        createdList.addAll(
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("40"),
                                                      new BigInteger("-1"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h1", "h2", "h3"))));


        List<TokenRangeReplicas> wrappedRange =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("35"),
                                                      new BigInteger("10"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9")));
        createdList.addAll(wrappedRange);
        assertThat(wrappedRange).hasSize(2);
        // We should not have wrapped ranges as generateTokenRangeReplicas unwraps them, and we validate this below
        assertThat(hasWrappedRange(createdList)).isFalse();
        assertThat(hasOverlaps(createdList)).isTrue();
        return createdList;
    }

    private List<TokenRangeReplicas> createWrappedOvlNonMatchingMinTokenList()
    {
        List<TokenRangeReplicas> createdList = new ArrayList<>();

        createdList.addAll(TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("5"),
                                                                         new BigInteger("20"),
                                                                         Partitioner.Random,
                                                                         new HashSet<>(Arrays.asList("h4", "h5"))));
        createdList.addAll(TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("20"),
                                                                         new BigInteger("35"),
                                                                         Partitioner.Random,
                                                                         new HashSet<>(Arrays.asList("h6", "h7"))));

        List<TokenRangeReplicas> wrappedRange1 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("40"),
                                                      new BigInteger("5"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h1", "h2", "h3")));

        List<TokenRangeReplicas> wrappedRange2 =
        TokenRangeReplicas.generateTokenRangeReplicas(new BigInteger("35"),
                                                      new BigInteger("10"),
                                                      Partitioner.Random,
                                                      new HashSet<>(Arrays.asList("h9")));
        createdList.addAll(wrappedRange1);
        assertThat(wrappedRange1).hasSize(2);
        createdList.addAll(wrappedRange2);
        assertThat(wrappedRange2).hasSize(2);
        // We should not have wrapped ranges as generateTokenRangeReplicas unwraps them, and we validate this below
        assertThat(hasWrappedRange(createdList)).isFalse();
        assertThat(hasOverlaps(createdList)).isTrue();

        return createdList;
    }
}
