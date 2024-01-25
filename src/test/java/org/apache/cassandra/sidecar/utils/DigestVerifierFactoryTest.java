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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.impl.headers.HeadersMultiMap;

import static org.apache.cassandra.sidecar.common.http.SidecarHttpHeaderNames.CONTENT_XXHASH32;
import static org.apache.cassandra.sidecar.utils.DigestVerifierFactory.FALLBACK_VERIFIER;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DigestVerifierFactory}
 */
class DigestVerifierFactoryTest
{
    MultiMap options;
    Vertx vertx = Vertx.vertx();

    @BeforeEach
    void setup()
    {
        options = new HeadersMultiMap();
    }


    @Test
    void testEmptyFactoryReturnsFallbackVerifier()
    {
        DigestVerifier verifier = new DigestVerifierFactory(vertx).verifier(options);
        assertThat(verifier).as("should fallback to the fallback verifier when no verifiers are configured")
                            .isNotNull()
                            .isSameAs(FALLBACK_VERIFIER);
    }

    @Test
    void testMd5Verifier()
    {
        options.set("content-md5", "md5-header");
        DigestVerifier verifier = new DigestVerifierFactory(vertx).verifier(options);

        assertThat(verifier).as("MD5DigestVerifier can verify MD5 content headers")
                            .isNotNull()
                            .isInstanceOf(MD5DigestVerifier.class);
    }

    @Test
    void testXXHashVerifier()
    {
        options.set(CONTENT_XXHASH32, "xxhash-header");
        DigestVerifier verifier = new DigestVerifierFactory(vertx).verifier(options);

        assertThat(verifier).as("XXHashDigestVerifier can verify XXHash content headers")
                            .isNotNull()
                            .isInstanceOf(XXHash32DigestVerifier.class);
    }

    @Test
    void testFirstVerifierTakesPrecedence()
    {
        options.set("content-md5", "md5-header")
               .set(CONTENT_XXHASH32, "xxhash-header");
        DigestVerifier verifier = new DigestVerifierFactory(vertx).verifier(options);

        assertThat(verifier).as("XXHashDigestVerifier is selected when both headers are present")
                            .isNotNull()
                            .isInstanceOf(XXHash32DigestVerifier.class);
    }
}
