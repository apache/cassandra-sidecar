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

package org.apache.cassandra.sidecar.common.response.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

/**
 * Class representing an entry of the Client Connection Stats
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ClientConnectionEntry
{
    @JsonProperty("address")
    public String address()
    {
        return address;
    }

    @JsonProperty("port")
    public int port()
    {
        return port;
    }

    @JsonProperty("sslEnabled")
    public Boolean sslEnabled()
    {
        return sslEnabled;
    }

    @JsonProperty("sslCipherSuite")
    public String sslCipherSuite()
    {
        return sslCipherSuite;
    }

    @JsonProperty("sslProtocol")
    public String sslProtocol()
    {
        return sslProtocol;
    }

    @JsonProperty("protocolVersion")
    public String protocolVersion()
    {
        return protocolVersion;
    }

    @JsonProperty("username")
    public String username()
    {
        return username;
    }

    @JsonProperty("requestCount")
    public long requestCount()
    {
        return requestCount;
    }

    @JsonProperty("driverName")
    public String driverName()
    {
        return driverName;
    }

    @JsonProperty("driverVersion")
    public String driverVersion()
    {
        return driverVersion;
    }

    private final String address;
    private final int port;
    private final Boolean sslEnabled;
    private final String sslCipherSuite;
    private final String sslProtocol;
    private final String protocolVersion;
    private final String username;
    private final long requestCount;
    private final String driverName;
    private final String driverVersion;

    @JsonCreator
    public ClientConnectionEntry(@NotNull @JsonProperty("address") String address,
                                 @NotNull @JsonProperty("port") int port,
                                 @NotNull @JsonProperty("sslEnabled") boolean sslEnabled,
                                 @NotNull @JsonProperty("sslCipherSuite") String sslCipherSuite,
                                 @NotNull @JsonProperty("sslProtocol") String sslProtocol,
                                 @NotNull @JsonProperty("protocolVersion") String protocolVersion,
                                 @NotNull @JsonProperty("username") String username,
                                 @NotNull @JsonProperty("requestCount") long requestCount,
                                 @NotNull @JsonProperty("driverName") String driverName,
                                 @NotNull @JsonProperty("driverVersion") String driverVersion)
    {
        this.address = address;
        this.port = port;
        this.sslEnabled = sslEnabled;
        this.sslCipherSuite = sslCipherSuite;
        this.sslProtocol = sslProtocol;
        this.protocolVersion = protocolVersion;
        this.username = username;
        this.requestCount = requestCount;
        this.driverName = driverName;
        this.driverVersion = driverVersion;
    }
}
