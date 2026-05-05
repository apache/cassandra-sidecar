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

package org.apache.cassandra.sidecar.common.server.utils;

import java.math.BigInteger;

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.RandomToken;

/**
 * Token utility functions.
 */
public class TokenUtils
{
    public static BigInteger tokenToBigInteger(Token token)
    {
        if (token instanceof RandomToken) // BigInteger - RandomPartitioner
        {
            RandomToken t = (RandomToken) token;
            return t.getValue();
        }
        else if (token instanceof Murmur3Token) // Long - Murmur3Partitioner
        {
            Murmur3Token t = (Murmur3Token) token;
            return BigInteger.valueOf(t.getValue());
        }
        throw new IllegalArgumentException("Unsupported token type: " + token.getClass().getName() +
                                           ". Only tokens of Murmur3Partitioner and RandomPartitioner are supported.");
    }
}
