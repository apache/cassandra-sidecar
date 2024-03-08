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

import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.MultiMap;
import io.vertx.core.file.FileSystem;
import org.apache.cassandra.sidecar.common.data.MD5Digest;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of {@link DigestVerifier}. Here we use MD5 implementation of {@link java.security.MessageDigest}
 * for calculating digest and match the calculated digest with expected digest obtained from request.
 */
public class MD5DigestVerifier extends AsyncFileDigestVerifier<MD5Digest>
{
    protected MD5DigestVerifier(@NotNull FileSystem fs, @NotNull MD5Digest digest, @NotNull Hasher hasher)
    {
        super(fs, digest, hasher);
    }

    public static DigestVerifier create(FileSystem fs, MultiMap headers, HasherProvider hasherProvider)
    {
        MD5Digest md5Digest = new MD5Digest(headers.get(HttpHeaderNames.CONTENT_MD5.toString()));
        return new MD5DigestVerifier(fs, md5Digest, hasherProvider.get());
    }
}
