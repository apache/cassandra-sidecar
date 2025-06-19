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

package org.apache.cassandra.sidecar.testing;

import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolvers;


/**
 * A {@link DnsResolver} instance used for tests that provides fast DNS resolution, to avoid blocking
 * DNS resolution at the JDK/OS-level.
 *
 * <p><b>NOTE:</b> The resolver assumes that the addresses are of the form 127.0.0.x, which is what is currently
 * configured for integration tests.
 */
public class LocalhostResolver implements DnsResolver
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalhostResolver.class);
    private static final Pattern HOSTNAME_PATTERN = Pattern.compile("^localhost(\\d+)?$");
    private final DnsResolver delegate;

    public LocalhostResolver()
    {
        this(DnsResolvers.DEFAULT);
    }

    LocalhostResolver(DnsResolver delegate)
    {
        this.delegate = delegate;
    }

    /**
     * Returns the resolved IP address from the hostname. If the {@code hostname} pattern is not matched,
     * delegate the resolution to the delegate resolver.
     *
     * <pre>
     * resolver.resolve("localhost") = "127.0.0.1"
     * resolver.resolve("localhost2") = "127.0.0.2"
     * resolver.resolve("localhost20") = "127.0.0.20"
     * resolver.resolve("127.0.0.5") = "127.0.0.5"
     * </pre>
     *
     * @param hostname the hostname to resolve
     * @return the resolved IP address
     */
    @Override
    public String resolve(String hostname) throws UnknownHostException
    {
        Matcher matcher = HOSTNAME_PATTERN.matcher(hostname);
        if (!matcher.matches())
        {
            LOGGER.warn("Invalid hostname found {}.", hostname);
            return delegate.resolve(hostname);
        }
        String group = matcher.group(1);
        return "127.0.0." + (group != null ? group : "1");
    }

    /**
     * Returns the resolved hostname from the given {@code address}. When an invalid IP address is provided,
     * delegates {@code address} resolution to the delegate.
     *
     * <pre>
     * resolver.reverseResolve("127.0.0.1") = "localhost"
     * resolver.reverseResolve("127.0.0.2") = "localhost2"
     * resolver.reverseResolve("127.0.0.20") = "localhost20"
     * resolver.reverseResolve("localhost5") = "localhost5"
     * </pre>
     *
     * @param address the IP address to perform the reverse resolution
     * @return the resolved hostname for the given {@code address}
     */
    @Override
    public String reverseResolve(String address) throws UnknownHostException
    {
        // IP addresses have the form 127.0.0.x
        int lastDotIndex = address.lastIndexOf('.');
        if (lastDotIndex < 0 || lastDotIndex + 1 == address.length())
        {
            LOGGER.warn("Invalid ip address found {}.", address);
            return delegate.reverseResolve(address);
        }
        String netNumber = address.substring(lastDotIndex + 1);
        return "1".equals(netNumber) ? "localhost" : "localhost" + netNumber;
    }
}
