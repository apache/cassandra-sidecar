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

import io.vertx.core.MultiMap;
import io.vertx.core.file.FileSystem;
import org.apache.cassandra.sidecar.common.data.XXHash32Digest;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.common.http.SidecarHttpHeaderNames.CONTENT_XXHASH32;
import static org.apache.cassandra.sidecar.common.http.SidecarHttpHeaderNames.CONTENT_XXHASH32_SEED;

/**
 * Implementation of {@link DigestVerifier} to calculate the digest and match the calculated digest
 * with the expected digest.
 */
public class XXHash32DigestVerifier extends AsyncFileDigestVerifier<XXHash32Digest>
{
    protected XXHash32DigestVerifier(@NotNull FileSystem fs, @NotNull XXHash32Digest digest, @NotNull Hasher hasher)
    {
        super(fs, digest, hasher);
    }

    public static XXHash32DigestVerifier create(FileSystem fs, MultiMap headers, HasherProvider hasherProvider)
    {
        XXHash32Digest digest = new XXHash32Digest(headers.get(CONTENT_XXHASH32), headers.get(CONTENT_XXHASH32_SEED));
        Hasher hasher = hasherProvider.get(maybeGetSeedOrDefault(digest));
        return new XXHash32DigestVerifier(fs, digest, hasher);
    }

    protected static int maybeGetSeedOrDefault(XXHash32Digest digest)
    {
        String seedHex = digest.seedHex();
        if (seedHex != null)
        {
            return (int) Long.parseLong(seedHex, 16);
        }
        return 0;
    }
}
