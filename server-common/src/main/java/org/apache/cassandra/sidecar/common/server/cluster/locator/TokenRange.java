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
import java.util.stream.Collectors;

import com.google.common.collect.Range;

import com.datastax.driver.core.DataType;
import org.jetbrains.annotations.Nullable;

/**
 * Range: (start, end] - start exclusive and end inclusive
 */
public class TokenRange
{
    private final Range<Token> range;
    private volatile Token firstToken = null;

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
     * @return end token. It is the last token enclosed in the range.
     */
    public Token end()
    {
        return range.upperEndpoint();
    }

    /**
     * @return the first token enclosed in the range. It returns null if the range is empty, e.g. (v, v]
     */
    @Nullable
    public Token firstToken()
    {
        if (range.isEmpty())
        {
            return null;
        }

        // it is ok to race
        if (firstToken == null)
        {
            firstToken = range.lowerEndpoint().increment();
        }
        return firstToken;
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
     * Two ranges are overlapping when their intersection is non-empty. For example,
     *
     * Ranges (0, 3] and (1, 4] are overlapping. The intersection is (1, 3]
     * Ranges (0, 3] and (5, 7] are not overlapping, as there is no intersection
     * Ranges (0, 3] and (3, 5] are not overlapping, as the intersection (3, 3] is empty
     *
     * Note that the semantics is different from {@link Range#isConnected(Range)}
     *
     * @return true if this range overlaps with the other range; otherwise, false
     */
    public boolean overlaps(TokenRange other)
    {
        return this.range.lowerEndpoint().compareTo(other.range.upperEndpoint()) < 0
               && other.range.lowerEndpoint().compareTo(this.range.upperEndpoint()) < 0;
    }

    public TokenRange intersection(TokenRange overlapping)
    {
        Range<Token> overlap = this.range.intersection(overlapping.range);
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
}
