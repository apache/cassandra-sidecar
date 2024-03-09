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

import org.apache.commons.codec.digest.XXHash32;

/**
 * Provides the XXHash32 implementation from commons-codec
 */
public class XXHash32Provider implements DigestAlgorithmProvider
{
    @Override
    public DigestAlgorithm get(int seed)
    {
        return new Lz4XXHash32(seed);
    }

    /**
     * XXHash32 implementation from LZ4
     */
    public static class Lz4XXHash32 implements DigestAlgorithm
    {
        private final XXHash32 xxHash32;

        Lz4XXHash32(int seed)
        {
            this.xxHash32 = new XXHash32(seed);
        }

        @Override
        public void update(byte[] buf, int off, int len)
        {
            xxHash32.update(buf, off, len);
        }

        @Override
        public String digest()
        {
            return Long.toHexString(xxHash32.getValue());
        }

        @Override
        public void close()
        {
            // XXHash32 does not require to be closed
        }
    }
}
