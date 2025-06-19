package org.apache.cassandra.sidecar.common.server.dns;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Determines which DnsResolver to use. Currently supported implementations are
 * default and resolveToIp. The former will resolve hostname to address and
 * address to hostname whereas the latter will only resolve hostname to address.
 */
public enum DnsResolverEnum implements DnsResolver
{
    @JsonProperty("default")
    DEFAULT("default")
    {
        @Override
        public String reverseResolve(String address) throws UnknownHostException
        {
            return InetAddress.getByName(address).getHostName();
        }
    },
    @JsonProperty("resolveToIp")
    RESOLVE_TO_IP("resolveToIp")
    {
        @Override
        public String reverseResolve(String address) throws UnknownHostException
        {
            return InetAddress.getByName(address).getHostAddress();
        }
    };

    final String name;

    DnsResolverEnum(String name)
    {
        this.name = name;
    }

    @Override
    public String toString()
    {
        return name;
    }
}
