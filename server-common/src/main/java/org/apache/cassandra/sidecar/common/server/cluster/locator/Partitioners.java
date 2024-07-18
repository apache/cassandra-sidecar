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

/**
 * A collection of supported {@link Partitioner}
 */
public class Partitioners
{
    private static final String FULL_PACKAGE_NAME = "org.apache.cassandra.dht.";
    private static final String RANDOM_PARTITIONER_NAME = RandomPartitioner.class.getSimpleName().toLowerCase();
    private static final String MURMUR3_PARTITIONER_NAME = Murmur3Partitioner.class.getSimpleName().toLowerCase();

    private Partitioners()
    {
        throw new UnsupportedOperationException();
    }

    public static final Partitioner RANDOM = RandomPartitioner.INSTANCE;
    public static final Partitioner MURMUR3 = Murmur3Partitioner.INSTANCE;

    /**
     * Return a {@link Partitioner} instance based on the class name
     * @param className class name of the partitioner
     * @return partitioner instance
     * @throws IllegalArgumentException when the partitioner is unsupported
     */
    public static Partitioner from(String className)
    {
        String simpleClassName = className.toLowerCase();
        if (simpleClassName.startsWith(FULL_PACKAGE_NAME))
        {
            simpleClassName = simpleClassName.substring(FULL_PACKAGE_NAME.length());
        }

        if (simpleClassName.equals(RANDOM_PARTITIONER_NAME))
        {
            return RandomPartitioner.INSTANCE;
        }
        else if (simpleClassName.equals(MURMUR3_PARTITIONER_NAME))
        {
            return Murmur3Partitioner.INSTANCE;
        }
        else
        {
            throw new IllegalArgumentException("Unsupported partitioner: " + simpleClassName);
        }
    }

    /**
     * Simplified RandomPartitioner
     */
    private static class RandomPartitioner implements Partitioner
    {
        private static final RandomPartitioner INSTANCE = new RandomPartitioner();

        private static final Token MIN = Token.from(BigInteger.valueOf(-1));
        private static final Token MAX = Token.from(BigInteger.valueOf(2).pow(127));

        @Override
        public Token minimumToken()
        {
            return MIN;
        }

        @Override
        public Token maximumToken()
        {
            return MAX;
        }
    }

    /**
     * Simplified Murmur3Partitioner
     */
    private static class Murmur3Partitioner implements Partitioner
    {
        private static final Murmur3Partitioner INSTANCE = new Murmur3Partitioner();

        private static final Token MIN = Token.from(BigInteger.valueOf(Long.MIN_VALUE));
        private static final Token MAX = Token.from(BigInteger.valueOf(Long.MAX_VALUE));

        @Override
        public Token minimumToken()
        {
            return MIN;
        }

        @Override
        public Token maximumToken()
        {
            return MAX;
        }
    }
}
