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

import com.datastax.driver.core.Row;
import org.apache.cassandra.sidecar.db.DataObjectMappingException;
import org.jetbrains.annotations.NotNull;

/**
 * Representation of the connected clients metadata
 */
public class ConnectedClientStats
{

    public final String address;
    public final int port;
    public final String hostname;
    public final String username;
    public final String connectionStage;
    public final String protocolVersion;
    public final String driverName;
    public final String driverVersion;
    public final boolean sslEnabled;
    public final String sslProtocol;
    public final String sslCipherSuite;
    public final long requestCount;
//    public final Map<String, String> clientOptions;
//    public final String keyspaceName;
//    public final String authenticationMode;
//    public final Map<String, String> authMetadata;

    public static ConnectedClientStats from(@NotNull Row row) throws DataObjectMappingException
    {
        return new ConnectedClientStats(row);
    }

    public ConnectedClientStats(@NotNull Row row)
    {
        this.address = row.getInet("address").getHostAddress();
        this.port = row.getInt("port");
        this.hostname = row.getString("hostname");
        this.username = row.getString("username");
        this.connectionStage = row.getString("connection_stage");
        this.protocolVersion = Integer.toString(row.getInt("protocol_version"));
        this.driverName = row.getString("driver_name");
        this.driverVersion = row.getString("driver_version");
        this.sslEnabled = row.getBool("ssl_enabled");
        this.sslProtocol = row.getString("ssl_protocol");
        this.sslCipherSuite = row.getString("ssl_cipher_suite");
        this.requestCount = row.getLong("request_count");
    }
}
