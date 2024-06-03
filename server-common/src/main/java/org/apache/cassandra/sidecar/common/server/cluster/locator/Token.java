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
import java.util.Comparator;
import java.util.Objects;

import org.jetbrains.annotations.NotNull;

/**
 * Token, i.e. hashed partition key, in Cassandra
 */
public final class Token implements Comparable<Token>
{
    private static final Comparator<Token> TOKEN_COMPARATOR = Comparator.comparing(Token::toBigInteger);

    private final BigInteger value;
    private final int hashCode;

    public static Token from(BigInteger value)
    {
        return new Token(value);
    }

    /**
     * Create token from its string literal
     * @param valueStr token value
     * @throws NumberFormatException {@code valueStr} is not a valid representation
     *         of a BigInteger.
     * @return token
     */
    public static Token from(String valueStr)
    {
        return new Token(new BigInteger(valueStr));
    }

    public static Token from(long value)
    {
        return new Token(BigInteger.valueOf(value));
    }

    private Token(BigInteger value)
    {
        this.value = value;
        this.hashCode = value.hashCode();
    }

    public BigInteger toBigInteger()
    {
        return value;
    }

    public Token increment()
    {
        return new Token(value.add(BigInteger.ONE));
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
        Token token = (Token) o;
        return Objects.equals(value, token.value);
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }

    @Override
    public int compareTo(@NotNull Token other)
    {
        return Objects.compare(this, Objects.requireNonNull(other), TOKEN_COMPARATOR);
    }

    @Override
    public String toString()
    {
        return "Token(" + value + ')';
    }
}
