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

package org.apache.cassandra.sidecar.common.server.utils;

import java.net.InetSocketAddress;

import com.google.common.net.HostAndPort;

/**
 * Common utilities for String manipulations
 */
public class StringUtils
{
    /**
     * The string representation of the address with the same signature as represented by
     * the Cassandra server. This is useful when we consume data representing an Inet address
     * and port from JMX that has been stringyfied, and we need to perform string matching
     * against those results from JMX.
     *
     * @param address the {@link InetSocketAddress address}
     * @return the string representation of the address with the same signature as represented by
     * the Cassandra server
     */
    @SuppressWarnings("UnstableApiUsage")
    public static String cassandraFormattedHostAndPort(InetSocketAddress address)
    {
        return HostAndPort.fromParts(address.getAddress().getHostAddress(), address.getPort()).toString();
    }

    /**
     * Similar to {@link #cassandraFormattedHostAndPort(InetSocketAddress)}, but use ip and port to
     * create the address string representing the Cassandra server.
     * @param ip ip address
     * @param port port
     * @return the string representation of the address with the same signature as represented by
     * the Cassandra server
     */
    @SuppressWarnings("UnstableApiUsage")
    public static String cassandraFormattedHostAndPort(String ip, int port)
    {
        return HostAndPort.fromParts(ip, port).toString();

    }
}
