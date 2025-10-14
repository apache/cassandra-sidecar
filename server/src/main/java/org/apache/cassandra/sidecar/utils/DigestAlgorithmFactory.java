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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.common.request.data.MD5Digest.MD5_ALGORITHM;
import static org.apache.cassandra.sidecar.common.request.data.XXHash32Digest.XXHASH_32_ALGORITHM;

/**
 * Factory for creating digest algorithm instances based on algorithm name and optional seed.
 * Supports MD5 and XXHash32 digest algorithms.
 */
public class DigestAlgorithmFactory
{
    private final DigestAlgorithmProvider md5DigestAlgorithmProvider;
    private final DigestAlgorithmProvider xxHash32DigestAlgorithmProvider;

    @Inject
    public DigestAlgorithmFactory(@Named("md5") DigestAlgorithmProvider md5DigestAlgorithmProvider,
                                  @Named("xxhash32") DigestAlgorithmProvider xxHash32DigestAlgorithmProvider)
    {
        this.md5DigestAlgorithmProvider = md5DigestAlgorithmProvider;
        this.xxHash32DigestAlgorithmProvider = xxHash32DigestAlgorithmProvider;
    }

    /**
     * Validates whether the given digest algorithm name is supported.
     * This method performs validation without creating a DigestAlgorithm instance.
     *
     * @param algorithmName the digest algorithm name to validate
     * @throws IllegalArgumentException if the algorithm name is null, empty, or unsupported
     */
    public static void validateAlgorithmName(String algorithmName)
    {
        if (null == algorithmName || algorithmName.isBlank())
        {
            throw new IllegalArgumentException("Digest algorithm name cannot be null or empty");
        }
        if (!algorithmName.equalsIgnoreCase(MD5_ALGORITHM) && !algorithmName.equalsIgnoreCase(XXHASH_32_ALGORITHM))
        {
            throw new IllegalArgumentException("Unsupported digest algorithm " + algorithmName);
        }
    }

    /**
     * Creates a digest algorithm instance based on the specified algorithm name and optional seed.
     *
     * @param algorithmName the digest algorithm name (MD5 or XXHash32, case-insensitive)
     * @param seed          optional seed value for the digest algorithm (maybe null)
     * @return a DigestAlgorithm instance
     * @throws IllegalArgumentException if algorithmName is null, empty, or unsupported
     */
    public DigestAlgorithm getDigestAlgorithm(String algorithmName, @Nullable Integer seed)
    {
        if (null == algorithmName || algorithmName.isBlank())
        {
            throw new IllegalArgumentException("Digest algorithm name cannot be null or empty");
        }
        if (algorithmName.equalsIgnoreCase(MD5_ALGORITHM))
        {
            return seed == null
                   ? md5DigestAlgorithmProvider.get()
                   : md5DigestAlgorithmProvider.get(seed);
        }
        else if (algorithmName.equalsIgnoreCase(XXHASH_32_ALGORITHM))
        {
            return seed == null
                   ? xxHash32DigestAlgorithmProvider.get()
                   : xxHash32DigestAlgorithmProvider.get(seed);
        }
        else
        {
            throw new IllegalArgumentException("Unsupported digest algorithm " + algorithmName);
        }
    }
}
