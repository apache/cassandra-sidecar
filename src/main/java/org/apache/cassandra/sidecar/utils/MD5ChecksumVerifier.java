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

import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.FileSystem;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Implementation of {@link ChecksumVerifier}. Here we use MD5 implementation of {@link java.security.MessageDigest}
 * for calculating checksum. And match the calculated checksum with expected checksum obtained from request.
 */
public class MD5ChecksumVerifier extends AsyncFileChecksumVerifier
{
    public MD5ChecksumVerifier(FileSystem fs)
    {
        super(fs);
    }

    @Override
    protected @Nullable String expectedChecksum(MultiMap options)
    {
        return options.get(HttpHeaderNames.CONTENT_MD5.toString());
    }

    @Override
    @VisibleForTesting
    protected Future<String> calculateHash(AsyncFile file, MultiMap options)
    {
        MessageDigest digest;
        try
        {
            digest = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            return Future.failedFuture(e);
        }

        Promise<String> result = Promise.promise();

        readFile(file, result, buf -> digest.update(buf.getBytes()),
                 _v -> result.complete(Base64.getEncoder().encodeToString(digest.digest())));

        return result.future();
    }

    @Override
    protected String algorithm()
    {
        return "MD5";
    }
}
