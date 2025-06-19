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

package org.apache.cassandra.sidecar.common.server.dns;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Determines which DnsResolver to use. Currently supported implementations are
 * default and resolve_to_ip. The former will resolve hostname to address and
 * address to hostname whereas the latter will only resolve hostname to address.
 */
public enum DnsResolvers implements DnsResolver
{
    /**
     * Default implementation of the {@link DnsResolver} that uses the JDK's
     * underlying DNS resolution mechanism
     */
    @JsonProperty("default")
    DEFAULT
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public String reverseResolve(String address) throws UnknownHostException
        {
            return InetAddress.getByName(address).getHostName();
        }
    },

    /**
     * Implementation of the {@link DnsResolver} interface that always resolves
     * and reverse resolves to an IP address
     */
    @JsonProperty("resolve_to_ip")
    RESOLVE_TO_IP
    {
        /**
         * Returns the hostAddress for the provided address
         *
         * @param address IP address
         * @return IP address
         * @throws UnknownHostException when the host is not known
         */
        @Override
        public String reverseResolve(String address) throws UnknownHostException
        {
            return InetAddress.getByName(address).getHostAddress();
        }
    };

    /**
     * {@inheritDoc}
     */
    @Override
    public String resolve(String hostname) throws UnknownHostException
    {
           return InetAddress.getByName(hostname).getHostAddress();
    }
}
