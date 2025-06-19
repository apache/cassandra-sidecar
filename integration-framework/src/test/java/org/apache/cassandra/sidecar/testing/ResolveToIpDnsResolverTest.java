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

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolvers;

import static org.assertj.core.api.Assertions.assertThat;

class ResolveToIpDnsResolverTest
{
    @Test
    void testResolve() throws UnknownHostException
    {
        DnsResolver resolver = DnsResolvers.RESOLVE_TO_IP;
        assertThat(resolver.resolve("localhost")).isEqualTo("127.0.0.1");
        assertThat(resolver.resolve("localhost2")).isEqualTo("127.0.0.2");
    }

    @Test
    void testReverseResolve() throws UnknownHostException
    {
        DnsResolver resolver = DnsResolvers.RESOLVE_TO_IP;
        assertThat(resolver.reverseResolve("127.0.0.1")).isEqualTo("127.0.0.1");
        assertThat(resolver.reverseResolve("127.0.0.2")).isEqualTo("127.0.0.2");
    }
}
