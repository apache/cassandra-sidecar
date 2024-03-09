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
import com.google.inject.name.Named;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import org.jetbrains.annotations.VisibleForTesting;

import static org.apache.cassandra.sidecar.common.http.SidecarHttpHeaderNames.CONTENT_XXHASH32;

/**
 * A factory class that returns the {@link DigestVerifier} instance.
 */
@Singleton
public class DigestVerifierFactory
{
    @VisibleForTesting
    static final DigestVerifier FALLBACK_VERIFIER = Future::succeededFuture;
    private final FileSystem fs;
    private final DigestAlgorithmProvider xxhash32;
    private final DigestAlgorithmProvider md5;

    /**
     * Constructs a new factory with a list of hash algorithms it supports
     *
     * @param vertx the vertx instance
     * @param xxhash32 xxhash32 hash algorithm it supports
     * @param md5 md5 hash algorithm it supports
     */
    @Inject
    public DigestVerifierFactory(Vertx vertx,
                                 @Named("xxhash32") DigestAlgorithmProvider xxhash32,
                                 @Named("md5") DigestAlgorithmProvider md5)
    {
        this.fs = vertx.fileSystem();
        this.xxhash32 = xxhash32;
        this.md5 = md5;
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
            return XXHash32DigestVerifier.create(fs, headers, xxhash32);
        }
        else if (headers.contains(HttpHeaderNames.CONTENT_MD5.toString()))
        {
            return MD5DigestVerifier.create(fs, headers, md5);
        }
        // Fallback to no-op validator
        return FALLBACK_VERIFIER;
    }
}
