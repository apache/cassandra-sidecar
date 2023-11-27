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

package org.apache.cassandra.sidecar.locator;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.datastax.driver.core.DataType;

/**
 * Range: (start, end] - start exclusive and end inclusive
 */
public class TokenRange
{
    public final BigInteger start;
    public final BigInteger end;

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
                               .map(range -> new TokenRange((BigInteger) range.getStart().getValue(),
                                                            (BigInteger) range.getEnd().getValue()))
                               .collect(Collectors.toList());
        }
        else if (tokenDataType == DataType.bigint()) // Long - Murmur3Partitioner
        {
            return dsTokenRange.unwrap()
                               .stream()
                               .map(range -> new TokenRange((Long) range.getStart().getValue(),
                                                            (Long) range.getEnd().getValue()))
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
        this.start = start;
        this.end = end;
    }

    public BigInteger start()
    {
        return this.start;
    }

    public BigInteger end()
    {
        return this.end;
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
        return Objects.equals(start, that.start) && Objects.equals(end, that.end);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(start, end);
    }
}
