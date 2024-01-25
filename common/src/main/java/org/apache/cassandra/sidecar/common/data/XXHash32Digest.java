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

package org.apache.cassandra.sidecar.common.data;

import java.util.Map;
import java.util.Objects;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implements the XXHash32 Digest
 */
public class XXHash32Digest implements Digest
{
    private final @NotNull String value;
    private final @Nullable String seedHex;

    /**
     * Constructs a new XXHashDigest with the provided XXHash {@code value}
     *
     * @param value the xxhash value
     */
    public XXHash32Digest(String value)
    {
        this(value, null);
    }

    /**
     * Constructs a new XXHashDigest with the provided XXHash {@code value} and the seed value represented as
     * a hexadecimal string
     *
     * @param value   the xxhash value
     * @param seedHex the value of the seed represented as a hexadecimal value
     */
    public XXHash32Digest(@NotNull String value, @Nullable String seedHex)
    {
        this.value = Objects.requireNonNull(value, "value is required");
        this.seedHex = seedHex;
    }

    /**
     * Adds XXHash headers to the existing {@code headers}
     *
     * @param headers the input argument
     */
    @Override
    public void accept(Map<String, String> headers)
    {
        headers.put("content-xxhash32", value);
        if (seedHex != null)
        {
            headers.put("content-xxhash32-seed", seedHex);
        }
    }
}
