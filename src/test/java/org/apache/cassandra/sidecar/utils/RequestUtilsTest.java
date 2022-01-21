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
