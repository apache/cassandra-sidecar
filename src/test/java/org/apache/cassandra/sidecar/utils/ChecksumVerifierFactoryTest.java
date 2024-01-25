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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.impl.headers.HeadersMultiMap;

import static org.apache.cassandra.sidecar.utils.ChecksumVerifierFactory.FALLBACK_VERIFIER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * Unit tests for {@link ChecksumVerifierFactory}
 */
class ChecksumVerifierFactoryTest
{
    MultiMap options;
    FileSystem fs = Vertx.vertx().fileSystem();

    @BeforeEach
    void setup()
    {
        options = new HeadersMultiMap();
    }

    @Test
    void failsWhenListIsNull()
    {
        assertThatNullPointerException().isThrownBy(() -> new ChecksumVerifierFactory(null))
                                        .withMessage("list must be provided");
    }

    @Test
    void testEmptyFactoryReturnsFallbackVerifier()
    {
        options.set("content-md5", "md5-header")
               .set("content-xxhash32", "xxhash-header");
        ChecksumVerifier verifier = new ChecksumVerifierFactory(Collections.emptyList())
                                    .verifier(options);
        assertThat(verifier).as("should fallback to the fallback verifier when no verifiers are configured")
                            .isNotNull()
                            .isSameAs(FALLBACK_VERIFIER);
    }

    @Test
    void testMd5VerifierDoesNotSupportXXHash()
    {
        options.set("content-xxhash32", "xxhash-header");
        ChecksumVerifier verifier = new ChecksumVerifierFactory(Collections.singletonList(new MD5ChecksumVerifier(fs)))
                                    .verifier(options);

        assertThat(verifier).as("should fallback to the fallback verifier when the header is xxhash")
                            .isNotNull()
                            .isSameAs(FALLBACK_VERIFIER);
    }

    @Test
    void testMd5Verifier()
    {
        options.set("content-md5", "md5-header");
        ChecksumVerifier verifier = new ChecksumVerifierFactory(Collections.singletonList(new MD5ChecksumVerifier(fs)))
                                    .verifier(options);

        assertThat(verifier).as("MD5ChecksumVerifier can verify MD5 content headers")
                            .isNotNull()
                            .isInstanceOf(MD5ChecksumVerifier.class);
    }

    @Test
    void testXXHashVerifierDoesNotSupportMD5()
    {
        options.set("content-md5", "md5-header");
        List<ChecksumVerifier> list = Collections.singletonList(new XXHash32ChecksumVerifier(fs));
        ChecksumVerifier verifier = new ChecksumVerifierFactory(list)
                                    .verifier(options);

        assertThat(verifier).as("should fallback to the fallback verifier when the header is md5")
                            .isNotNull()
                            .isSameAs(FALLBACK_VERIFIER);
    }

    @Test
    void testXXHashVerifier()
    {
        options.set("content-xxhash32", "xxhash-header");
        List<ChecksumVerifier> list = Collections.singletonList(new XXHash32ChecksumVerifier(fs));
        ChecksumVerifier verifier = new ChecksumVerifierFactory(list)
                                    .verifier(options);

        assertThat(verifier).as("XXHashChecksumVerifier can verify XXHash content headers")
                            .isNotNull()
                            .isInstanceOf(XXHash32ChecksumVerifier.class);
    }

    @Test
    void testFirstVerifierInListTakesPrecedence()
    {
        options.set("content-md5", "md5-header")
               .set("content-xxhash32", "xxhash-header");
        List<ChecksumVerifier> list = Arrays.asList(new XXHash32ChecksumVerifier(fs),
                                                    new MD5ChecksumVerifier(fs));
        ChecksumVerifier verifier = new ChecksumVerifierFactory(list)
                                    .verifier(options);

        assertThat(verifier).as("XXHashChecksumVerifier is selected when both headers are present")
                            .isNotNull()
                            .isInstanceOf(XXHash32ChecksumVerifier.class);

        // Reverse the order of the list, and now the MD5 verifier should take precedence
        Collections.reverse(list);
        verifier = new ChecksumVerifierFactory(list).verifier(options);

        assertThat(verifier).as("MD5ChecksumVerifier now takes precedence, since it is added first to the list")
                            .isNotNull()
                            .isInstanceOf(MD5ChecksumVerifier.class);
    }
}
