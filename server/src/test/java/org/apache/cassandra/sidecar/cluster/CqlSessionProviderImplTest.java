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

package org.apache.cassandra.sidecar.cluster;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link CQLSessionProviderImpl}
 */
class CqlSessionProviderImplTest
{
    @Test
    void testTruststoreRequiredForSSLConnection()
    {
        SslConfiguration withoutTruststore = SslConfigurationImpl.builder()
                                                                 .enabled(true)
                                                                 .keystore(new KeyStoreConfigurationImpl("/path", "password", "type"))
                                                                 .build();
        assertThatThrownBy(() -> new CQLSessionProviderImpl(Collections.emptyList(),
                                                            Collections.emptyList(),
                                                            0,
                                                            "dc",
                                                            0,
                                                            null,
                                                            null,
                                                            withoutTruststore,
                                                            null))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("SSL configured for Cassandra connection, but truststore is missing");
    }
}
