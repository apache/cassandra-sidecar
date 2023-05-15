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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for TokenRangeReplicas
 */
class TokenRangeReplicasTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TokenRangeReplicasTest.class);

    private boolean hasOverlaps(List<TokenRangeReplicas> rangeList)
    {
        Collections.sort(rangeList);
        for (int c = 0, i = 1; i < rangeList.size(); i++)
        {
            TokenRangeReplicas a = rangeList.get(c++);
            TokenRangeReplicas b = rangeList.get(i);
            if (a.intersects(b))
            {
                return true;
            }
        }
        return false;
    }

    private boolean checkContains(List<TokenRangeReplicas> resultList, TokenRangeReplicas expected)
    {
        return resultList.stream()
                         .map(TokenRangeReplicas::toString)
                         .anyMatch(r -> r.equals(expected.toString()));
    }

    // non-overlapping ranges
    @Test
    void simpleTest()
    {
        List<TokenRangeReplicas> simpleList = createSimpleTokenRangeReplicaList();
        LOGGER.info("Input={}", simpleList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(simpleList);
        LOGGER.info("Result={}", rangeList);
        assertThat(hasOverlaps(rangeList)).isFalse();
    }

    @Test
    void subRangeTest()
    {
        List<TokenRangeReplicas> subRangeList = createOverlappingTokenRangeReplicaList();
        LOGGER.info("Input={}", subRangeList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(subRangeList);
        LOGGER.info("Result={}", rangeList);

        assertThat(hasOverlaps(rangeList)).isFalse();
        // Validate that we have 1 additional list as a result of the splits
        assertThat(rangeList.size() == subRangeList.size() + 1).isTrue();

        // Validate that there is a merged range with 20-30 with hosts h4-h7
        TokenRangeReplicas expectedExists = new TokenRangeReplicas(new BigInteger("20"), new BigInteger("30"),
                Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5", "h6", "h7")));
        // Validate absence of larger list
        TokenRangeReplicas expectedNotExists = new TokenRangeReplicas(new BigInteger("10"),
                new BigInteger("40"), Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5")));

        assertThat(checkContains(rangeList, expectedExists)).isTrue();
        assertThat(checkContains(rangeList, expectedNotExists)).isFalse();
    }

    // Validate merge-split resulting from 2 ranges overlapping
    @Test
    void partialOverlapTest()
    {
        List<TokenRangeReplicas> partialOverlapList = createPartialOverlappingTokenRangeReplicaList();
        LOGGER.info("Input={}", partialOverlapList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(partialOverlapList);
        LOGGER.info("Result={}", rangeList);
        assertThat(hasOverlaps(rangeList)).isFalse();
        // Validate that we have 1 additional list as a result of the splits
        assertThat(rangeList.size() == partialOverlapList.size() + 1).isTrue();

        TokenRangeReplicas expectedExists = new TokenRangeReplicas(new BigInteger("10"), new BigInteger("15"),
                Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5")));
        TokenRangeReplicas expectedExists2 = new TokenRangeReplicas(new BigInteger("15"), new BigInteger("20"),
                Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5", "h6", "h7")));
        TokenRangeReplicas expectedExists3 = new TokenRangeReplicas(new BigInteger("20"), new BigInteger("30"),
                Partitioner.Random, new HashSet<>(Arrays.asList("h6", "h7")));

        TokenRangeReplicas expectedNotExists = new TokenRangeReplicas(new BigInteger("10"),
                new BigInteger("20"), Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5")));
        TokenRangeReplicas expectedNotExists2 = new TokenRangeReplicas(new BigInteger("15"),
                new BigInteger("30"), Partitioner.Random, new HashSet<>(Arrays.asList("h6", "h7")));

        assertThat(checkContains(rangeList, expectedExists)).isTrue();
        assertThat(checkContains(rangeList, expectedExists2)).isTrue();
        assertThat(checkContains(rangeList, expectedExists3)).isTrue();
        assertThat(checkContains(rangeList, expectedNotExists)).isFalse();
        assertThat(checkContains(rangeList, expectedNotExists2)).isFalse();
    }

    // Validate merge-split resulting from 3 consecutive ranges overlapping
    @Test
    void multiOverlapTest()
    {
        List<TokenRangeReplicas> multiOverlapList = createMultipleOverlappingTokenRangeReplicaList();
        LOGGER.info("Input={}", multiOverlapList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(multiOverlapList);
        LOGGER.info("Result={}", rangeList);
        assertThat(hasOverlaps(rangeList)).isFalse();
        // Validate that we have 1 additional list as a result of the splits
        assertThat(rangeList.size() == multiOverlapList.size() + 1).isTrue();

        TokenRangeReplicas expectedExists = new TokenRangeReplicas(new BigInteger("10"), new BigInteger("15"),
               Partitioner.Random, new HashSet<>(Arrays.asList("h1", "h2", "h3", "h4", "h5")));
        TokenRangeReplicas expectedExists2 = new TokenRangeReplicas(new BigInteger("15"), new BigInteger("25"),
               Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5", "h6", "h7")));
        TokenRangeReplicas expectedNotExists = new TokenRangeReplicas(new BigInteger("10"),
               new BigInteger("25"), Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5")));

        assertThat(checkContains(rangeList, expectedExists)).isTrue();
        assertThat(checkContains(rangeList, expectedExists2)).isTrue();
        assertThat(checkContains(rangeList, expectedNotExists)).isFalse();
    }

    // Validate merge-split from wrapped overlapping ranges
    @Test
    void wrappedMultiOverlapTest()
    {
        List<TokenRangeReplicas> wrappedOverlapList = createWrappedMultipleOverlappingTokenRangeReplicaList();
        LOGGER.info("Input={}", wrappedOverlapList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(wrappedOverlapList);
        LOGGER.info("Result={}", rangeList);
        assertThat(hasOverlaps(rangeList)).isFalse();

        // Validate that we have 2 additional ranges as a result of the splits - from unwrapping and split
        assertThat(rangeList.size() == wrappedOverlapList.size() + 2).isTrue();

        TokenRangeReplicas expectedExists = new TokenRangeReplicas(Partitioner.Random.minToken,
                new BigInteger("10"), Partitioner.Random, new HashSet<>(Arrays.asList("h9", "h1", "h2", "h3")));
        TokenRangeReplicas expectedExists2 = new TokenRangeReplicas(new BigInteger("30"),
                new BigInteger("35"), Partitioner.Random, new HashSet<>(Arrays.asList("h9", "h6", "h7")));
        TokenRangeReplicas expectedExists3 = new TokenRangeReplicas(new BigInteger("35"),
                Partitioner.Random.maxToken, Partitioner.Random, new HashSet<>(Collections.singletonList("h9")));
        TokenRangeReplicas expectedNotExists = new TokenRangeReplicas(new BigInteger("30"),
                new BigInteger("10"), Partitioner.Random, new HashSet<>(Collections.singletonList("h9")));
        TokenRangeReplicas expectedNotExists2 = new TokenRangeReplicas(new BigInteger("35"),
                Partitioner.Random.minToken, Partitioner.Random, new HashSet<>(Collections.singletonList("h9")));

        assertThat(checkContains(rangeList, expectedExists)).isTrue();
        assertThat(checkContains(rangeList, expectedExists2)).isTrue();
        assertThat(checkContains(rangeList, expectedExists3)).isTrue();
        assertThat(checkContains(rangeList, expectedNotExists)).isFalse();
        assertThat(checkContains(rangeList, expectedNotExists2)).isFalse();
    }

    @Test
    void wrappedOverlapTest()
    {
        List<TokenRangeReplicas> wrappedOverlapList = createWrappedOverlappingTokenRangeReplicaList();
        LOGGER.info("Input={}", wrappedOverlapList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(wrappedOverlapList);
        LOGGER.info("Result={}", rangeList);
        assertThat(hasOverlaps(rangeList)).isFalse();

        assertThat(rangeList.size() == wrappedOverlapList.size() + 1).isTrue();

        TokenRangeReplicas expectedExists = new TokenRangeReplicas(new BigInteger("40"),
                Partitioner.Random.maxToken, Partitioner.Random, new HashSet<>(Arrays.asList("h9", "h1", "h2", "h3")));
        TokenRangeReplicas expectedExists2 = new TokenRangeReplicas(new BigInteger("35"), new BigInteger("40"),
                Partitioner.Random, new HashSet<>(Collections.singletonList("h9")));
        TokenRangeReplicas expectedExists3 = new TokenRangeReplicas(Partitioner.Random.minToken,
                new BigInteger("10"), Partitioner.Random, new HashSet<>(Arrays.asList("h9", "h4", "h5")));
        TokenRangeReplicas expectedNotExists = new TokenRangeReplicas(Partitioner.Random.minToken,
                new BigInteger("20"), Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5")));
        TokenRangeReplicas expectedNotExists2 = new TokenRangeReplicas(new BigInteger("40"),
                Partitioner.Random.minToken, Partitioner.Random, new HashSet<>(Arrays.asList("h9", "h1", "h2", "h3")));

        assertThat(checkContains(rangeList, expectedExists)).isTrue();
        assertThat(checkContains(rangeList, expectedExists2)).isTrue();
        assertThat(checkContains(rangeList, expectedExists3)).isTrue();
        assertThat(checkContains(rangeList, expectedNotExists)).isFalse();
        assertThat(checkContains(rangeList, expectedNotExists2)).isFalse();
    }

    // Validate case when the partitioner min token does not match the least token value in the ring
    @Test
    void wrappedOverlapNonMatchingMinTokenTest()
    {
        List<TokenRangeReplicas> wrappedOverlapList = createWrappedOvlNonMatchingMinTokenList();
        LOGGER.info("Input={}", wrappedOverlapList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(wrappedOverlapList);
        LOGGER.info("Result={}", rangeList);
        assertThat(hasOverlaps(rangeList)).isFalse();

        // Additional split resulting from mismatching min tokens
        assertThat(rangeList.size()).isEqualTo(wrappedOverlapList.size() + 2);

        TokenRangeReplicas expectedExists = new TokenRangeReplicas(new BigInteger("40"),
                Partitioner.Random.maxToken, Partitioner.Random, new HashSet<>(Arrays.asList("h9", "h1", "h2", "h3")));
        // New Token range resulting from non-matching min token
        TokenRangeReplicas expectedExistsNew = new TokenRangeReplicas(Partitioner.Random.minToken,
                new BigInteger("5"), Partitioner.Random, new HashSet<>(Arrays.asList("h9", "h1", "h2", "h3")));
        TokenRangeReplicas expectedExists2 = new TokenRangeReplicas(new BigInteger("35"), new BigInteger("40"),
                Partitioner.Random, new HashSet<>(Collections.singletonList("h9")));
        // Other split resulting from new range
        TokenRangeReplicas expectedExists3 = new TokenRangeReplicas(new BigInteger("5"), new BigInteger("10"),
                Partitioner.Random, new HashSet<>(Arrays.asList("h9", "h4", "h5")));
        TokenRangeReplicas expectedNotExists = new TokenRangeReplicas(Partitioner.Random.minToken,
                new BigInteger("20"), Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5")));
        TokenRangeReplicas expectedNotExists2 = new TokenRangeReplicas(new BigInteger("40"),
                Partitioner.Random.minToken, Partitioner.Random, new HashSet<>(Arrays.asList("h9", "h1", "h2", "h3")));

        assertThat(checkContains(rangeList, expectedExists)).isTrue();
        assertThat(checkContains(rangeList, expectedExistsNew)).isTrue();
        assertThat(checkContains(rangeList, expectedExists2)).isTrue();
        assertThat(checkContains(rangeList, expectedExists3)).isTrue();
        assertThat(checkContains(rangeList, expectedNotExists)).isFalse();
        assertThat(checkContains(rangeList, expectedNotExists2)).isFalse();
    }

    @Test
    void wrappedActualOverlapTest()
    {
        List<TokenRangeReplicas> wrappedOverlapList = Arrays.asList(
        new TokenRangeReplicas(
        new BigInteger("3074457345618258602"), Partitioner.Murmur3.minToken,
        Partitioner.Murmur3, new HashSet<>(Arrays.asList("h1", "h2", "h3"))),
        new TokenRangeReplicas(
        Partitioner.Murmur3.minToken, new BigInteger("-3074457345618258603"),
        Partitioner.Murmur3, new HashSet<>(Arrays.asList("h2", "h3", "h1"))),
        new TokenRangeReplicas(
        new BigInteger("-3074457345618258603"), new BigInteger("3074457345618258602"),
        Partitioner.Murmur3, new HashSet<>(Arrays.asList("h3", "h1", "h2"))),
        new TokenRangeReplicas(
        new BigInteger("3074457345618258602"), new BigInteger("6148914691236517204"),
        Partitioner.Murmur3, new HashSet<>(Collections.singletonList("h9"))),
        new TokenRangeReplicas(
        new BigInteger("-3074457345618258603"), new BigInteger("3074457345618258602"),
        Partitioner.Murmur3, new HashSet<>(Collections.singletonList("h9"))),
        new TokenRangeReplicas(
        Partitioner.Murmur3.minToken, new BigInteger("-3074457345618258603"),
        Partitioner.Murmur3, new HashSet<>(Collections.singletonList("h9")))
        );
        LOGGER.info("Input:" + wrappedOverlapList);
        List<TokenRangeReplicas> rangeList = TokenRangeReplicas.normalize(wrappedOverlapList);
        LOGGER.info("Result:" + rangeList);
        assertThat(hasOverlaps(rangeList)).isFalse();

        assertThat(rangeList.size() == 4).isTrue();

        TokenRangeReplicas expectedExists = new TokenRangeReplicas(
        Partitioner.Murmur3.minToken, new BigInteger("-3074457345618258603"),
        Partitioner.Murmur3, new HashSet<>(Arrays.asList("h9", "h1", "h2", "h3")));
        TokenRangeReplicas expectedExists2 = new TokenRangeReplicas(
        new BigInteger("-3074457345618258603"), new BigInteger("3074457345618258602"),
        Partitioner.Murmur3, new HashSet<>(Arrays.asList("h9", "h1", "h2", "h3")));
        TokenRangeReplicas expectedExists3 = new TokenRangeReplicas(
        new BigInteger("3074457345618258602"), new BigInteger("6148914691236517204"),
        Partitioner.Murmur3, new HashSet<>(Arrays.asList("h9", "h1", "h2", "h3")));
        TokenRangeReplicas expectedExists4 = new TokenRangeReplicas(
        new BigInteger("6148914691236517204"), Partitioner.Murmur3.maxToken,
        Partitioner.Murmur3, new HashSet<>(Arrays.asList("h1", "h2", "h3")));
        TokenRangeReplicas expectedNotExists = new TokenRangeReplicas(
        new BigInteger("3074457345618258602"), Partitioner.Murmur3.minToken,
        Partitioner.Murmur3, new HashSet<>(Arrays.asList("h1", "h2", "h3")));

        assertThat(checkContains(rangeList, expectedExists)).isTrue();
        assertThat(checkContains(rangeList, expectedExists2)).isTrue();
        assertThat(checkContains(rangeList, expectedExists3)).isTrue();
        assertThat(checkContains(rangeList, expectedExists4)).isTrue();
        assertThat(checkContains(rangeList, expectedNotExists)).isFalse();
    }

    @Test
    void testIntersects()
    {
        TokenRangeReplicas range1 = createMurmur3(1, 10);
        TokenRangeReplicas range2 = createMurmur3(9, 12);
        assertThat(range1.intersects(range2)).isTrue();
        assertThat(range2.intersects(range1)).isTrue();

        TokenRangeReplicas range3 = createMurmur3(1, 10);
        TokenRangeReplicas range4 = createMurmur3(11, 20);
        assertThat(range3.intersects(range4)).isFalse();
        assertThat(range4.intersects(range3)).isFalse();

        // same range
        TokenRangeReplicas range11 = createMurmur3(10, 20);
        TokenRangeReplicas range12 = createMurmur3(10, 20);
        assertThat(range11.intersects(range12)).isTrue();
        assertThat(range12.intersects(range11)).isTrue();

        // range13 is full ring
        TokenRangeReplicas range13 = createMurmur3(10, 10);
        TokenRangeReplicas range14 = createMurmur3(50, 60);
        assertThat(range13.intersects(range14)).isTrue();
        assertThat(range14.intersects(range13)).isTrue();

        // wrap around case - non-overlapping
        TokenRangeReplicas range5 = createMurmur3(100, 10);
        TokenRangeReplicas range6 = createMurmur3(20, 30);
        assertThat(range5.intersects(range6)).isFalse();
        assertThat(range6.intersects(range5)).isFalse();

        // wrap around case - overlapping
        TokenRangeReplicas range7 = createMurmur3(100, 10);
        TokenRangeReplicas range8 = createMurmur3(5, 10);
        assertThat(range7.intersects(range8)).isTrue();
        assertThat(range8.intersects(range7)).isTrue();

        // both wrap around
        TokenRangeReplicas range9 = createMurmur3(100, 10);
        TokenRangeReplicas range10 = createMurmur3(200, 1);
        assertThat(range9.intersects(range10)).isTrue();
        assertThat(range10.intersects(range9)).isTrue();

        // complement
        TokenRangeReplicas range15 = createMurmur3(10, 2);
        TokenRangeReplicas range16 = createMurmur3(range15.end(), range15.start());
        assertThat(range15.intersects(range16)).isFalse();
        assertThat(range16.intersects(range15)).isFalse();
    }

    @Test
    void testContains()
    {
        // wraps around
        TokenRangeReplicas range1 = createMurmur3(100, -100);
        TokenRangeReplicas range2 = createMurmur3(BigInteger.valueOf(100), Partitioner.Murmur3.maxToken);
        assertThat(range1.contains(range2)).isTrue();
        assertThat(range2.contains(range1)).isFalse();

        // full ring
        TokenRangeReplicas range3 = createMurmur3(100, 100);
        assertThat(range3.contains(range1)).isTrue();
        assertThat(range3.contains(range2)).isTrue();
        assertThat(range2.contains(range1)).isFalse();
        assertThat(range2.contains(range3)).isFalse();
    }

    @Test
    void testIsLarger()
    {
        TokenRangeReplicas range1 = createMurmur3(10, 20);
        TokenRangeReplicas range2 = createMurmur3(10, 30);
        assertThat(range1.isLarger(range1)).isFalse();
        assertThat(range2.isLarger(range1)).isTrue();
        assertThat(range1.isLarger(range2)).isFalse();

        TokenRangeReplicas range3 = createRandom(-1, 10);
        TokenRangeReplicas range4 = createRandom(10, 20);
        assertThat(range3.isLarger(range4)).isTrue();
        assertThat(range4.isLarger(range3)).isFalse();

        TokenRangeReplicas range5 = createMurmur3(BigInteger.valueOf(100), Partitioner.Murmur3.maxToken);
        TokenRangeReplicas range6 = createMurmur3(BigInteger.valueOf(200), Partitioner.Murmur3.maxToken);
        assertThat(range5.isLarger(range6)).isTrue();
        assertThat(range1.isLarger(range5)).isFalse();

        TokenRangeReplicas range7 = createMurmur3(300, 1000);
        assertThat(range5.isLarger(range7)).isTrue();
        assertThat(range6.isLarger(range7)).isTrue();
    }

    private List<TokenRangeReplicas> createSimpleTokenRangeReplicaList()
    {
        return Arrays.asList(
        new TokenRangeReplicas(Partitioner.Random.minToken, new BigInteger("10"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h1", "h2", "h3"))),
        new TokenRangeReplicas(new BigInteger("10"), new BigInteger("20"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5"))),
        new TokenRangeReplicas(new BigInteger("20"), Partitioner.Random.minToken,
                               Partitioner.Random, new HashSet<>(Arrays.asList("h6", "h7")))
        );
    }

    // 2. Simple single overlap (consuming) => superset + no changes to others [Merge]
    private List<TokenRangeReplicas> createOverlappingTokenRangeReplicaList()
    {
        return Arrays.asList(
        new TokenRangeReplicas(Partitioner.Random.minToken, new BigInteger("10"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h1", "h2", "h3"))),
        new TokenRangeReplicas(new BigInteger("10"), new BigInteger("40"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5"))),
        new TokenRangeReplicas(new BigInteger("20"), new BigInteger("30"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h6", "h7"))),
        new TokenRangeReplicas(new BigInteger("40"), Partitioner.Random.minToken,
                               Partitioner.Random, new HashSet<>(Collections.singletonList("h9")))
        );
    }

    // 3. Single overlap - cutting [Merge + Split]
    private List<TokenRangeReplicas> createPartialOverlappingTokenRangeReplicaList()
    {
        return Arrays.asList(
        new TokenRangeReplicas(new BigInteger("-1"), new BigInteger("10"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h1", "h2", "h3"))),
        new TokenRangeReplicas(new BigInteger("10"), new BigInteger("20"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5"))),
        new TokenRangeReplicas(new BigInteger("15"), new BigInteger("30"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h6", "h7"))),
        new TokenRangeReplicas(new BigInteger("30"), new BigInteger("-1"),
                               Partitioner.Random, new HashSet<>(Collections.singletonList("h9")))
        );
    }

    // 4. Multi-overlaps
    private List<TokenRangeReplicas> createMultipleOverlappingTokenRangeReplicaList()
    {
        return Arrays.asList(
        new TokenRangeReplicas(new BigInteger("-1"), new BigInteger("15"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h1", "h2", "h3"))),
        new TokenRangeReplicas(new BigInteger("10"), new BigInteger("25"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5"))),
        new TokenRangeReplicas(new BigInteger("15"), new BigInteger("30"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h6", "h7"))),
        new TokenRangeReplicas(new BigInteger("30"), new BigInteger("-1"),
                               Partitioner.Random, new HashSet<>(Collections.singletonList("h9")))
        );
    }

    // 5. Overlaps w/ wrap-around, etc.
    private List<TokenRangeReplicas> createWrappedMultipleOverlappingTokenRangeReplicaList()
    {
        return Arrays.asList(
        new TokenRangeReplicas(new BigInteger("-1"), new BigInteger("15"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h1", "h2", "h3"))),
        new TokenRangeReplicas(new BigInteger("15"), new BigInteger("20"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5"))),
        new TokenRangeReplicas(new BigInteger("20"), new BigInteger("35"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h6", "h7"))),
        new TokenRangeReplicas(new BigInteger("30"), new BigInteger("10"),
                               Partitioner.Random, new HashSet<>(Collections.singletonList("h9")))
        );
    }

    private List<TokenRangeReplicas> createWrappedOverlappingTokenRangeReplicaList()
    {
        return Arrays.asList(
        new TokenRangeReplicas(new BigInteger("40"), new BigInteger("-1"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h1", "h2", "h3"))),
        new TokenRangeReplicas(new BigInteger("-1"), new BigInteger("20"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5"))),
        new TokenRangeReplicas(new BigInteger("20"), new BigInteger("35"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h6", "h7"))),
        new TokenRangeReplicas(new BigInteger("35"), new BigInteger("10"),
                               Partitioner.Random, new HashSet<>(Collections.singletonList("h9")))
        );
    }

    private List<TokenRangeReplicas> createWrappedOvlNonMatchingMinTokenList()
    {
        return Arrays.asList(
        new TokenRangeReplicas(new BigInteger("40"), new BigInteger("5"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h1", "h2", "h3"))),
        new TokenRangeReplicas(new BigInteger("5"), new BigInteger("20"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h4", "h5"))),
        new TokenRangeReplicas(new BigInteger("20"), new BigInteger("35"),
                               Partitioner.Random, new HashSet<>(Arrays.asList("h6", "h7"))),
        new TokenRangeReplicas(new BigInteger("35"), new BigInteger("10"),
                               Partitioner.Random, new HashSet<>(Collections.singletonList("h9")))
        );
    }

    private static TokenRangeReplicas createMurmur3(int start, int end)
    {
        return create(BigInteger.valueOf(start), BigInteger.valueOf(end), Partitioner.Murmur3);
    }

    private static TokenRangeReplicas createMurmur3(BigInteger start, BigInteger end)
    {
        return create(start, end, Partitioner.Murmur3);
    }

    private static TokenRangeReplicas createRandom(int start, int end)
    {
        return create(BigInteger.valueOf(start), BigInteger.valueOf(end), Partitioner.Random);
    }

    private static TokenRangeReplicas create(BigInteger start, BigInteger end, Partitioner partitioner)
    {
        return new TokenRangeReplicas(start, end, partitioner, Collections.emptySet());
    }
}
