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

package org.apache.cassandra.sidecar.config.yaml;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link CacheConfigurationImpl}
 */
class CacheConfigurationImplTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testDefaultConstructor()
    {
        CacheConfigurationImpl config = new CacheConfigurationImpl();

        assertThat(config.expireAfterAccess()).isNull();
        assertThat(config.refreshAfterWrite()).isNull();
        assertThat(config.maximumSize()).isEqualTo(100);
        assertThat(config.enabled()).isTrue();
        assertThat(config.warmupRetries()).isEqualTo(5);
        assertThat(config.warmupRetryInterval()).isEqualTo(MillisecondBoundConfiguration.parse("1s"));
    }

    @Test
    void testConstructorWithParameters()
    {
        MillisecondBoundConfiguration expireAfterAccess = MillisecondBoundConfiguration.parse("30m");
        MillisecondBoundConfiguration refreshAfterWrite = MillisecondBoundConfiguration.parse("5m");
        MillisecondBoundConfiguration warmupRetryInterval = MillisecondBoundConfiguration.parse("2s");

        CacheConfigurationImpl config = CacheConfigurationImpl.builder()
                                                              .expireAfterAccess(expireAfterAccess)
                                                              .refreshAfterWrite(refreshAfterWrite)
                                                              .maximumSize(1000)
                                                              .enabled(false)
                                                              .warmupRetries(10)
                                                              .warmupRetryInterval(warmupRetryInterval)
                                                              .build();

        assertThat(config.expireAfterAccess()).isEqualTo(expireAfterAccess);
        assertThat(config.refreshAfterWrite()).isEqualTo(refreshAfterWrite);
        assertThat(config.maximumSize()).isEqualTo(1000);
        assertThat(config.enabled()).isFalse();
        assertThat(config.warmupRetries()).isEqualTo(10);
        assertThat(config.warmupRetryInterval()).isEqualTo(warmupRetryInterval);
    }

    @Test
    void testBuilderWithDefaults()
    {
        CacheConfigurationImpl config = CacheConfigurationImpl.builder()
                                                              .expireAfterAccess(MillisecondBoundConfiguration.parse("30m"))
                                                              .maximumSize(100)
                                                              .build();

        assertThat(config.expireAfterAccess()).isEqualTo(MillisecondBoundConfiguration.parse("30m"));
        assertThat(config.refreshAfterWrite()).isNull();
        assertThat(config.maximumSize()).isEqualTo(100);
        assertThat(config.enabled()).isTrue();
        assertThat(config.warmupRetries()).isEqualTo(5);
        assertThat(config.warmupRetryInterval()).isEqualTo(MillisecondBoundConfiguration.parse("1s"));
    }

    @Test
    void testSerializationWithOnlyRefreshAfterWrite() throws Exception
    {
        String jsonString = "{" +
                            "\"enabled\": \"true\"," +
                            "\"refresh_after_write\": \"5m\"," +
                            "\"maximum_size\": 1000," +
                            "\"warmup_retries\": 10," +
                            "\"warmup_retry_interval\": \"2s\"" +
                            "}";

        CacheConfigurationImpl config = MAPPER.readValue(jsonString, CacheConfigurationImpl.class);

        assertThat(config.enabled()).isTrue();
        assertThat(config.expireAfterAccess()).isNull();
        assertThat(config.refreshAfterWrite()).isEqualTo(MillisecondBoundConfiguration.parse("5m"));
        assertThat(config.maximumSize()).isEqualTo(1000);
        assertThat(config.warmupRetries()).isEqualTo(10);
        assertThat(config.warmupRetryInterval()).isEqualTo(MillisecondBoundConfiguration.parse("2s"));
    }
}
