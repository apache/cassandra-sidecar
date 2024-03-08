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

package org.apache.cassandra.sidecar.utils;

import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * Provides the XXHash32 implementation from LZ4
 */
public class Lz4XXHash32Provider implements HasherProvider
{
    @Override
    public Hasher get(int seed)
    {
        return new Lz4XXHash32(seed);
    }

    /**
     * XXHash32 implementation from LZ4
     */
    public static class Lz4XXHash32 implements Hasher
    {
        private final StreamingXXHash32 hasher;

        Lz4XXHash32(int seed)
        {
            // might have shared hashers with ThreadLocal
            XXHashFactory factory = XXHashFactory.safeInstance();
            this.hasher = factory.newStreamingHash32(seed);
        }

        @Override
        public void update(byte[] buf, int off, int len)
        {
            hasher.update(buf, off, len);
        }

        @Override
        public String checksum()
        {
            return Long.toHexString(hasher.getValue());
        }

        @Override
        public void close()
        {
            hasher.close();
        }
    }
}
