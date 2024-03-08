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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Provides the MD5 implementation from JDK
 */
public class JdkMd5DigestProvider implements HasherProvider
{
    @Override
    public Hasher get(int seed)
    {
        return new JdkMD5Digest();
    }

    /**
     * MD5 implementation from JDK
     */
    public static class JdkMD5Digest implements Hasher
    {
        private final MessageDigest md5;

        public JdkMD5Digest()
        {
            try
            {
                this.md5 = MessageDigest.getInstance("MD5");
            }
            catch (NoSuchAlgorithmException ex)
            {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void update(byte[] buf, int off, int len)
        {
            md5.update(buf, off, len);
        }

        @Override
        public String checksum()
        {
            return Base64.getEncoder().encodeToString(md5.digest());
        }

        @Override
        public void close()
        {
            // MessageDigest does not require to be closed
        }
    }

}
