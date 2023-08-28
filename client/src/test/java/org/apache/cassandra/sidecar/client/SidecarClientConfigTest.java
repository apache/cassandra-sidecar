/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SidecarClientConfig}
 */
class SidecarClientConfigTest
{
    @Test
    void testDefaults()
    {
        SidecarClientConfig config = new SidecarClientConfig.Builder().build();
        assertThat(config.maxRetries()).isEqualTo(3);
        assertThat(config.retryDelayMillis()).isEqualTo(500L);
        assertThat(config.maxRetryDelayMillis()).isEqualTo(60_000L);
    }

    @Test
    void testMaxRetries()
    {
        SidecarClientConfig config = new SidecarClientConfig.Builder().maxRetries(10).build();
        assertThat(config.maxRetries()).isEqualTo(10);
    }

    @Test
    void testRetryDelayMillis()
    {
        SidecarClientConfig config = new SidecarClientConfig.Builder().retryDelayMillis(100).build();
        assertThat(config.retryDelayMillis()).isEqualTo(100L);
    }

    @Test
    void testMaxRetryDelayMillis()
    {
        SidecarClientConfig config = new SidecarClientConfig.Builder().maxRetryDelayMillis(5_100).build();
        assertThat(config.maxRetryDelayMillis()).isEqualTo(5_100L);
    }

    @Test
    void testAllOptions()
    {
        SidecarClientConfig config = new SidecarClientConfig.Builder().maxRetries(10)
                                                                      .retryDelayMillis(100)
                                                                      .maxRetryDelayMillis(5_100)
                                                                      .build();
        assertThat(config.maxRetries()).isEqualTo(10);
        assertThat(config.retryDelayMillis()).isEqualTo(100L);
        assertThat(config.maxRetryDelayMillis()).isEqualTo(5_100L);
    }
}
