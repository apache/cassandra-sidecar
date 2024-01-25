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

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Implementation of {@link ChecksumVerifier} to calculate the checksum and match the calculated checksum
 * with the expected checksum.
 */
public class XXHash32ChecksumVerifier extends AsyncFileChecksumVerifier
{
    public static final String CONTENT_XXHASH = "content-xxhash32";
    public static final String CONTENT_XXHASH_SEED = "content-xxhash32-seed";

    public XXHash32ChecksumVerifier(FileSystem fs)
    {
        super(fs);
    }

    @Override
    protected @Nullable String expectedChecksum(MultiMap options)
    {
        return options.get(CONTENT_XXHASH);
    }

    @Override
    @VisibleForTesting
    protected Future<String> calculateHash(AsyncFile file, MultiMap options)
    {
        Promise<String> result = Promise.promise();
        Future<String> future = result.future();

        // might have shared hashers with ThreadLocal
        XXHashFactory factory = XXHashFactory.safeInstance();

        int seed = maybeGetSeedOrDefault(options);
        StreamingXXHash32 hasher = factory.newStreamingHash32(seed);

        future.onComplete(ignored -> hasher.close());

        readFile(file, result, buf -> {
                     byte[] bytes = buf.getBytes();
                     hasher.update(bytes, 0, bytes.length);
                 },
                 _v -> result.complete(Long.toHexString(hasher.getValue())));

        return future;
    }

    @Override
    protected String algorithm()
    {
        return "XXHash32";
    }

    protected int maybeGetSeedOrDefault(MultiMap options)
    {
        String seedHex = options.get(CONTENT_XXHASH_SEED);
        if (seedHex != null)
        {
            return (int) Long.parseLong(seedHex, 16);
        }
        return 0x9747b28c; // random seed for initializing
    }
}
