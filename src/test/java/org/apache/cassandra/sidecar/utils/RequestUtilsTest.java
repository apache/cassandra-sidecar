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

package org.apache.cassandra.sidecar.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * RequestUtilsTest
 */
public class RequestUtilsTest
{
    @Test
    public void testAddressWithIPv4Host()
    {
        final String host = RequestUtils.extractHostAddressWithoutPort("127.0.0.1");
        assertEquals("127.0.0.1", host);
    }

    @Test
    public void testAddressIPv4HostAndPort()
    {
        final String host = RequestUtils.extractHostAddressWithoutPort("127.0.0.1:9043");
        assertEquals("127.0.0.1", host);
    }

    @Test
    public void testAddressWithIPv6Host()
    {
        final String host = RequestUtils.extractHostAddressWithoutPort("2001:db8:0:0:0:ff00:42:8329");
        assertEquals("2001:db8:0:0:0:ff00:42:8329", host);
    }

    @Test
    public void testAddressWithIPv6HostAndPort()
    {
        final String host = RequestUtils.extractHostAddressWithoutPort("[2001:db8:0:0:0:ff00:42:8329]:9043");
        assertEquals("2001:db8:0:0:0:ff00:42:8329", host);
    }

    @Test
    public void testAddressWithIPv6HostShortcut()
    {
        final String host = RequestUtils.extractHostAddressWithoutPort("::1");
        assertEquals("::1", host);
    }

    @Test
    public void testAddressWithIPv6HostShortcutWithPort()
    {
        final String host = RequestUtils.extractHostAddressWithoutPort("[::1]:9043");
        assertEquals("::1", host);
    }
}
