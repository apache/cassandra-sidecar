package org.apache.cassandra.sidecar.testing;

import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

public class ResolveToIpDnsResolverTest {
    @Test
    void testResolve() throws UnknownHostException
    {
        DnsResolver resolver = DnsResolver.RESOLVE_TO_IP;
        assertThat(resolver.resolve("localhost")).isEqualTo("127.0.0.1");
        assertThat(resolver.resolve("localhost2")).isEqualTo("127.0.0.2");
    }

    @Test
    void testReverseResolve() throws UnknownHostException
    {
        DnsResolver resolver = DnsResolver.RESOLVE_TO_IP;
        assertThat(resolver.reverseResolve("127.0.0.1")).isEqualTo("127.0.0.1");
        assertThat(resolver.reverseResolve("127.0.0.2")).isEqualTo("127.0.0.2");
    }
}
