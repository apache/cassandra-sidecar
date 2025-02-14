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

package org.apache.cassandra.sidecar.common.server.cluster.locator;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import com.datastax.driver.core.DataType;

/**
 * Range: (start, end] - start exclusive and end inclusive
 */
public class TokenRange
{
    public final Range<Token> range;

    /**
     * Unwrap the java driver's token range if necessary and convert the unwrapped ranges list.
     * Only the token ranges from Murmur3Partitioner and RandomPartitioner are supported.
     *
     * @param dsTokenRange TokenRange implementation in Cassandra java driver
     * @return list of token ranges. If the input token range wraps around, the size of the list is 2;
     * otherwise, the list has only one range
     */
    public static List<TokenRange> from(com.datastax.driver.core.TokenRange dsTokenRange)
    {
        DataType tokenDataType = dsTokenRange.getStart().getType();
        if (tokenDataType == DataType.varint()) // BigInteger - RandomPartitioner
        {
            return dsTokenRange.unwrap()
                               .stream()
                               .map(range -> {
                                   BigInteger start = (BigInteger) range.getStart().getValue();
                                   BigInteger end = (BigInteger) range.getEnd().getValue();
                                   if (end.compareTo(Partitioners.RANDOM.minimumToken().toBigInteger()) == 0)
                                   {
                                       end = Partitioners.RANDOM.maximumToken().toBigInteger();
                                   }
                                   return new TokenRange(start, end);
                               })
                               .collect(Collectors.toList());
        }
        else if (tokenDataType == DataType.bigint()) // Long - Murmur3Partitioner
        {
            return dsTokenRange.unwrap()
                               .stream()
                               .map(range -> {
                                   BigInteger start = BigInteger.valueOf((Long) range.getStart().getValue());
                                   BigInteger end = BigInteger.valueOf((Long) range.getEnd().getValue());
                                   if (end.compareTo(Partitioners.MURMUR3.minimumToken().toBigInteger()) == 0)
                                   {
                                       end = Partitioners.MURMUR3.maximumToken().toBigInteger();
                                   }
                                   return new TokenRange(start, end);
                               })
                               .collect(Collectors.toList());
        }
        else
        {
            throw new IllegalArgumentException(
            "Unsupported token type: " + tokenDataType +
            ". Only tokens of Murmur3Partitioner and RandomPartitioner are supported.");
        }
    }

    /**
     * Diff the two set of {@link TokenRange}s. The connected token ranges in each set are merged before diffing.
     * The result {@link SymmetricDiffResult#onlyInLeft} contains the token ranges that are only in the input {@code left} token range set, and
     * the result {@link SymmetricDiffResult#onlyInRight} contains the token ranges that are only in the input {@code right} token range set.
     * @param left token range set
     * @param right token range set
     * @return {@link SymmetricDiffResult} where its left contains the token ranges that are only in the input {@code left} token range set, and
     *         its right contains the token ranges that are only in the input {@code right} token range set.
     */
    @SuppressWarnings("UnstableApiUsage")
    public static SymmetricDiffResult symmetricDiff(Set<TokenRange> left, Set<TokenRange> right)
    {
        RangeSet<Token> mergedLeft = TreeRangeSet.create();
        RangeSet<Token> mergedRight = TreeRangeSet.create();
        left.forEach(r -> mergedLeft.add(r.range));
        right.forEach(r -> mergedRight.add(r.range));
        RangeSet<Token> resultLeft = TreeRangeSet.create(mergedLeft);
        resultLeft.removeAll(mergedRight);
        RangeSet<Token> resultRight = TreeRangeSet.create(mergedRight);
        resultRight.removeAll(mergedLeft);
        return new SymmetricDiffResult(resultLeft.asRanges(), resultRight.asRanges());
    }

    public TokenRange(long start, long end)
    {
        this(BigInteger.valueOf(start), BigInteger.valueOf(end));
    }

    public TokenRange(BigInteger start, BigInteger end)
    {
        this(Token.from(start), Token.from(end));
    }

    public TokenRange(Token start, Token end)
    {
        this.range = Range.openClosed(start, end);
    }

    /**
     * @return start token. It is not enclosed in the range.
     */
    public Token start()
    {
        return range.lowerEndpoint();
    }

    /**
     * @return start token as {@link BigInteger}. It is not enclosed in the range.
     */
    public BigInteger startAsBigInt()
    {
        return range.lowerEndpoint().toBigInteger();
    }

    /**
     * @return end token. It is the last token enclosed in the range.
     */
    public Token end()
    {
        return range.upperEndpoint();
    }

    /**
     * @return end token as {@link BigInteger}. It is the last token enclosed in the range.
     */
    public BigInteger endAsBigInt()
    {
        return range.upperEndpoint().toBigInteger();
    }

    /**
     * Test if this range encloses the other range.
     * It simply delegates to {@link Range#encloses(Range)}
     */
    public boolean encloses(TokenRange other)
    {
        return this.range.encloses(other.range);
    }

    /**
     * Two ranges are intersecting when their intersection is non-empty. For example,
     * <p>Ranges {@code (0, 3]} and {@code (1, 4]} are overlapping. The intersection is {@code (1, 3]}
     * <p>Ranges {@code (0, 3]} and {@code (5, 7]} are not overlapping, as there is no intersection
     * <p>Ranges {@code (0, 3]} and {@code (3, 5]} are not overlapping, as the intersection {@code (3, 3]} is empty
     *
     * <p>Note that the semantics is different from {@link Range#isConnected(Range)}
     *
     * @return true if this range intersects with the other range; otherwise, false
     */
    public boolean intersects(TokenRange other)
    {
        return this.range.lowerEndpoint().compareTo(other.range.upperEndpoint()) < 0
               && other.range.lowerEndpoint().compareTo(this.range.upperEndpoint()) < 0;
    }

    /**
     * Two ranges connect with each other when 1) they overlap or 2) their ends are connected.
     * <p>For 1), refer to {@link #intersects(TokenRange)}
     * <p>For 2), see the following examples. The ranges {@code (0, 3]} and {@code (3, 5]} are connected.
     * The ranges {@code (0, 3]} and {@code (4, 6]} are not connected.
     *
     * <p> Note that it is implemented using {@link Range#isConnected(Range)}
     *
     * @param other the other range to check
     * @return true if this range connects with the other range; otherwise, false
     */
    public boolean connectsWith(TokenRange other)
    {
        return this.range.isConnected(other.range);
    }

    /**
     * @return the range that is enclosed by both this and the other ranges.
     */
    public TokenRange intersection(TokenRange other)
    {
        Range<Token> overlap = this.range.intersection(other.range);
        return new TokenRange(overlap.lowerEndpoint(), overlap.upperEndpoint());
    }

    /**
     * Determine whether all tokens in this range are larger than the ones in the other token range
     * @param other token range
     * @return true if the start token of this range is larger or equals to the other range's end token; otherwise, false
     */
    public boolean largerThan(TokenRange other)
    {
        return this.start().compareTo(other.end()) >= 0;
    }

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

        TokenRange that = (TokenRange) o;
        return Objects.equals(range, that.range);
    }

    @Override
    public int hashCode()
    {
        return range.hashCode();
    }

    @Override
    public String toString()
    {
        return "TokenRange(" +
               range.lowerEndpoint().toBigInteger() + ", " +
               range.upperEndpoint().toBigInteger() + ']';
    }

    /**
     * Pair of {@link TokenRange} sets that represent the result of symmetric diff
     */
    public static class SymmetricDiffResult
    {
        public final Set<TokenRange> onlyInLeft;
        public final Set<TokenRange> onlyInRight;

        private SymmetricDiffResult(Set<Range<Token>> onlyInLeft, Set<Range<Token>> onlyInRight)
        {
            this.onlyInLeft = toUnmodifiableSet(onlyInLeft);
            this.onlyInRight = toUnmodifiableSet(onlyInRight);
        }

        private static Set<TokenRange> toUnmodifiableSet(Set<Range<Token>> ranges)
        {
            return ranges.stream()
                         .map(r -> new TokenRange(r.lowerEndpoint(), r.upperEndpoint()))
                         .collect(Collectors.toUnmodifiableSet());
        }
    }
}
