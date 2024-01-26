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
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import org.jetbrains.annotations.VisibleForTesting;

import static org.apache.cassandra.sidecar.common.data.XXHash32Digest.CONTENT_XXHASH32;

/**
 * A factory class that returns the {@link DigestVerifier} instance.
 */
@Singleton
public class DigestVerifierFactory
{
    @VisibleForTesting
    static final DigestVerifier FALLBACK_VERIFIER = Future::succeededFuture;
    private final FileSystem fs;


    /**
     * Constructs a new factory
     *
     * @param vertx the vertx instance
     */
    @Inject
    public DigestVerifierFactory(Vertx vertx)
    {
        this.fs = vertx.fileSystem();
    }

    /***
     * Returns the first match for a {@link DigestVerifier} from the registered list of verifiers. If none of the
     * verifiers matches, a no-op validator is returned.
     *
     * @param headers the request headers used to test whether a {@link DigestVerifier} can be used to verify the
     *                request
     * @return the first match for a {@link DigestVerifier} from the registered list of verifiers, or a no-op
     * verifier if none match
     */
    public DigestVerifier verifier(MultiMap headers)
    {
        if (headers.contains(CONTENT_XXHASH32))
        {
            return XXHash32DigestVerifier.create(fs, headers);
        }
        else if (headers.contains(HttpHeaderNames.CONTENT_MD5.toString()))
        {
            return MD5DigestVerifier.create(fs, headers);
        }
        // Fallback to no-op validator
        return FALLBACK_VERIFIER;
    }
}
