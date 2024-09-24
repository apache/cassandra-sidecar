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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.server.data.DataObjectMappingException;
import org.jetbrains.annotations.NotNull;

/**
 * Representation of the ClientStats connection metadata
 */
public class ClientStats
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public final String address;
    public final int port;
    public final String hostname;
    public final String username;
    public final String connectionStage;
    public final String protocolVersion;
    public final Map<String, String> clientOptions;
    public final String driverName;
    public final String driverVersion;
    public final boolean sslEnabled;
    public final String sslProtocol;
    public final String sslCipherSuite;
    public final String keyspaceName;
    public final int requestCount;
    public final String authenticationMode;
    public final Map<String, String> authMetadata;

    public static ClientStats.Builder builder()
    {
        return new ClientStats.Builder();
    }

    public static ClientStats from(@NotNull Row row) throws DataObjectMappingException
    {
        ClientStats.Builder builder = new ClientStats.Builder();
        builder.address(row.getInet("address").getHostAddress())
               .port(row.getInt("port"))
               .hostname(row.getString("hostname"))
               .username(row.getString("username"))
               .connectionStage(row.getString("connection_stage"))
               .protocolVersion(Integer.toString(row.getInt("protocol_version")))
               .driverName(row.getString("driver_name"))
               .driverVersion(row.getString("driver_version"))
               .sslEnabled(row.getBool("ssl_enabled"))
               .sslProtocol(row.getString("ssl_protocol"))
               .sslCipherSuite(row.getString("ssl_cipher_suite"));

        return builder.build();
    }

    private static Map<String, String> deserializeMap(ByteBuffer bytes, String fieldNameHint)
    {
        return bytes == null
               ? null
               : deserializeToMap(bytes,
                                  new TypeReference<Map<String, String>>()
                                  {
                                  },
                                  fieldNameHint);
    }

    private static <T> Map<String, String> deserializeToMap(ByteBuffer byteBuffer, TypeReference<Map<String, String>> type, String fieldNameHint)
    {
        try
        {
            return MAPPER.readValue(Bytes.getArray(byteBuffer), type);
        }
        catch (IOException e)
        {
            throw new DataObjectMappingException("Failed to deserialize " + fieldNameHint, e);
        }
    }

    private ClientStats(Builder builder)
    {
        this.address = builder.address;
        this.port = builder.port;
        this.hostname = builder.hostname;
        this.username = builder.username;
        this.connectionStage = builder.connectionStage;
        this.protocolVersion = builder.protocolVersion;
        this.clientOptions = builder.clientOptions;
        this.driverName = builder.driverName;
        this.driverVersion = builder.driverVersion;
        this.sslEnabled = builder.sslEnabled;
        this.sslProtocol = builder.sslProtocol;
        this.sslCipherSuite = builder.sslCipherSuite;
        this.keyspaceName = builder.keyspaceName;
        this.requestCount = builder.requestCount;
        this.authMetadata = builder.authMetadata;
        this.authenticationMode = builder.authenticationMode;
    }

    /**
     * Builder for {@link ClientStats}
     */
    public static class Builder implements DataObjectBuilder<ClientStats.Builder, ClientStats>
    {
        private String address;
        private int port;
        private String hostname;
        private String username;
        private String connectionStage;
        private String protocolVersion;
        private Map<String, String> clientOptions;
        private String driverName;
        private String driverVersion;
        private boolean sslEnabled;
        private String sslProtocol;
        private String sslCipherSuite;
        private String keyspaceName;
        private int requestCount;
        private Map<String, String> authMetadata;
        private String authenticationMode;

        private Builder()
        {
        }

        private Builder(ClientStats clientStats)
        {
            this.address = clientStats.address;
            this.port = clientStats.port;
            this.hostname = clientStats.hostname;
            this.username = clientStats.username;
            this.connectionStage = clientStats.connectionStage;
            this.protocolVersion = clientStats.protocolVersion;
            this.clientOptions = clientStats.clientOptions;
            this.driverName = clientStats.driverName;
            this.driverVersion = clientStats.driverVersion;
            this.sslEnabled = clientStats.sslEnabled;
            this.sslProtocol = clientStats.sslProtocol;
            this.sslCipherSuite = clientStats.sslCipherSuite;
            this.keyspaceName = clientStats.keyspaceName;
            this.requestCount = clientStats.requestCount;
            this.authMetadata = clientStats.authMetadata;
            this.authenticationMode = clientStats.authenticationMode;
        }

        public Builder address(String address)
        {
            return update(b -> b.address = address);
        }

        public Builder port(int port)
        {
            return update(b -> b.port = port);
        }

        public Builder hostname(String hostname)
        {
            return update(b -> b.hostname = hostname);
        }

        public Builder username(String username)
        {
            return update(b -> b.username = username);
        }

        public Builder connectionStage(String connectionStage)
        {
            return update(b -> b.connectionStage = connectionStage);
        }

        public Builder protocolVersion(String protocolVersion)
        {
            return update(b -> b.protocolVersion = protocolVersion);
        }

        public Builder clientOptions(Map<String, String> clientOptions)
        {
            return update(b -> b.clientOptions = clientOptions);
        }

        public Builder driverName(String driverName)
        {
            return update(b -> b.driverName = driverName);
        }

        public Builder driverVersion(String driverVersion)
        {
            return update(b -> b.driverVersion = driverVersion);
        }

        public Builder sslEnabled(boolean sslEnabled)
        {
            return update(b -> b.sslEnabled = sslEnabled);
        }

        public Builder sslProtocol(String sslProtocol)
        {
            return update(b -> b.sslProtocol = sslProtocol);
        }

        public Builder sslCipherSuite(String sslCipherSuite)
        {
            return update(b -> b.sslCipherSuite = sslCipherSuite);
        }

        public Builder keyspaceName(String keyspaceName)
        {
            return update(b -> b.keyspaceName = keyspaceName);
        }

        public Builder requestCount(int requestCount)
        {
            return update(b -> b.requestCount = requestCount);
        }

        public Builder authMetadata(Map<String, String> authMetadata)
        {
            return update(b -> b.authMetadata = authMetadata);
        }

        public Builder authenticationMode(String authenticationMode)
        {
            return update(b -> b.authenticationMode = authenticationMode);
        }

        public Builder self()
        {
            return this;
        }

        public ClientStats build()
        {
            return new ClientStats(this);
        }
    }
}
