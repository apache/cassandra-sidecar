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

package org.apache.cassandra.sidecar.acl.authentication;

import java.util.Arrays;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.apache.cassandra.sidecar.acl.authentication.JwtParameterExtractor.SITE_SUFFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link JwtParameterExtractor}
 */
class JwtParameterExtractorTest
{
    @Test
    void testExtractingValidParameters()
    {
        JwtParameterExtractor parameterExtractor = new JwtParameterExtractor(Map.of("enabled", "true",
                                                                                    "site", "www.apache.org",
                                                                                    "client_id", "id",
                                                                                    "scopes_supported", "phone,email",
                                                                                    "config_discover_interval", "2m"));
        assertThat(parameterExtractor.enabled()).isTrue();
        assertThat(parameterExtractor.site()).isEqualTo("www.apache.org");
        assertThat(parameterExtractor.clientId()).isEqualTo("id");
        assertThat(parameterExtractor.scopes()).containsAll(Arrays.asList("email", "phone"));
        assertThat(parameterExtractor.configDiscoverInterval().toSeconds()).isEqualTo(120);
    }

    @Test
    void testSiteSuffixRemoved()
    {
        JwtParameterExtractor parameterExtractor = new JwtParameterExtractor(Map.of("site", "www.apache.org" + SITE_SUFFIX,
                                                                                    "client_id", "id"));
        assertThat(parameterExtractor.site()).as("Site suffix should be removed").isEqualTo("www.apache.org");
    }

    @Test
    void testCustomScopeSeparator()
    {
        JwtParameterExtractor parameterExtractor = new JwtParameterExtractor(Map.of("site", "www.apache.org",
                                                                                    "client_id", "id",
                                                                                    "scope_separator", " ",
                                                                                    "scopes_supported", "phone email"));
        assertThat(parameterExtractor.site()).isEqualTo("www.apache.org");
        assertThat(parameterExtractor.clientId()).isEqualTo("id");
        assertThat(parameterExtractor.scopes()).containsAll(Arrays.asList("email", "phone"));
    }

    @Test
    void testInvalidParameters()
    {
        assertThatThrownBy(() -> new JwtParameterExtractor(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("JWT parameters can not be null");

        assertThatThrownBy(() -> new JwtParameterExtractor(Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing site JWT parameter");

        assertThatThrownBy(() -> new JwtParameterExtractor(Map.of("site", "www.apache.org")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing client_id JWT parameter");
    }

    @Test
    void testInvalidEnabled()
    {
        JwtParameterExtractor parameterExtractor = new JwtParameterExtractor(Map.of("enabled", "random",
                                                                                    "site", "www.apache.org",
                                                                                    "client_id", "id"));
        assertThat(parameterExtractor.enabled()).isFalse();
        assertThat(parameterExtractor.site()).isEqualTo("www.apache.org");
        assertThat(parameterExtractor.clientId()).isEqualTo("id");
    }
}
