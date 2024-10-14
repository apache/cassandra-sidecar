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

package org.apache.cassandra.sidecar.adapters.base.db;

import java.net.InetAddress;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for ConnectedClientStats transformed from the cassandra driver results
 */
public class ConnectedClientStatsTest
{
    private static final String TEST_SPIFFE_IDENTITY = "spiffe://test.cassandra.apache.org/unitTest/mtls";
    Row mockRow = mock(Row.class);

    @Test
    public void connectedClientStatsTest()
    {
        setupMockData(mockRow, false);
        ConnectedClientStats stats = new ConnectedClientStats(mockRow);
        assertThat(stats).isNotNull();
        assertThat(stats.authenticationMetadata).isNotNull();
        assertThat(stats.authenticationMetadata.keySet()).contains("identity");
        assertThat(stats.authenticationMetadata.get("identity")).isEqualTo(TEST_SPIFFE_IDENTITY);
        assertThat(stats.clientOptions).isNotNull();
        assertThat(stats.clientOptions.get("CQL_VERSION")).isEqualTo("3.4.6");
    }

    @Test
    public void connectedClientStatsMissingFieldsTest()
    {
        setupMockData(mockRow, true);
        ConnectedClientStats stats = new ConnectedClientStats(mockRow);
        assertThat(stats).isNotNull();
        assertThat(stats.keyspaceName).isNull();
        assertThat(stats.authenticationMode).isNull();
        assertThat(stats.authenticationMetadata).isNull();
        assertThat(stats.clientOptions).isNull();
    }

    private void setupMockData(Row mockRow, boolean isMissingFields)
    {
        ColumnDefinitions mockColumnDefinitions = mock(ColumnDefinitions.class);
        when(mockRow.getColumnDefinitions()).thenReturn(mockColumnDefinitions);
        when(mockRow.getColumnDefinitions().contains(anyString())).thenAnswer(i -> {
            String input = i.getArgument(0, String.class);
            return !("keyspace_name".equals(input)
                     || "authentication_mode".equals(input)
                     || "authentication_metadata".equals(input)
                     || "client_options".equals(input));
        });

        String clientOptionsStr = "{ \"CQL_VERSION\": \"3.4.6\", \"DRIVER_NAME\": \"DataStax Python Driver\", \"DRIVER_VERSION\": \"3.25.0\" }";
        String authMetadataStr = "{ \"identity\": \"" + TEST_SPIFFE_IDENTITY + "\" }";

        when(mockRow.getInet("address")).thenReturn(InetAddress.getLoopbackAddress());
        when(mockRow.getInt("port")).thenReturn(0);
        when(mockRow.getString("hostname")).thenReturn("localhost");
        when(mockRow.getString("username")).thenReturn("u1");
        when(mockRow.getString("connection_stage")).thenReturn("test");
        when(mockRow.getInt("protocol_version")).thenReturn(5);
        when(mockRow.getString("driver_name")).thenReturn("TestDriver");
        when(mockRow.getString("driver_version")).thenReturn("TestVersion");
        when(mockRow.getBool("ssl_enabled")).thenReturn(false);
        when(mockRow.getString("ssl_protocol")).thenReturn("");
        when(mockRow.getString("ssl_cipher_suite")).thenReturn("");
        when(mockRow.getLong("request_count")).thenReturn(10L);
        String ks = isMissingFields ? null : "test";
        String authMode = isMissingFields ? null : "password";
        String authMetadata = isMissingFields ? null : authMetadataStr;
        String clientOptions = isMissingFields ? null : clientOptionsStr;
        when(mockRow.getString("keyspace_name")).thenReturn(ks);
        when(mockRow.getString("authentication_mode")).thenReturn(authMode);
        when(mockRow.getString("authentication_metadata")).thenReturn(authMetadata);
        when(mockRow.getString("client_options")).thenReturn(clientOptions);
    }
}
