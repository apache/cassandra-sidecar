package org.apache.cassandra.sidecar.utils;

/**
 * Utility class for Http request related operations.
 */
public class RequestUtils
{
    /**
     * Given a combined host address like 127.0.0.1:9042 or [2001:db8:0:0:0:ff00:42:8329]:9042, this method
     * removes port information and returns 27.0.0.1 or 2001:db8:0:0:0:ff00:42:8329.
     * @param address
     * @return host address without port information
     */
    public static String extractHostAddressWithoutPort(String address)
    {
        if (address.contains(":"))
        {
            String host = address.substring(0, address.lastIndexOf(':'));
            // remove brackets from ipv6 addresses
            return host.startsWith("[") ? host.substring(1, host.length() - 1) : host;
        }
        return address;
    }
}
