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

package org.apache.cassandra.sidecar.acl;

import java.security.cert.X509Certificate;
import java.util.Collections;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.CredentialValidationException;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.acl.authentication.CassandraIdentityExtractor;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;
import org.apache.cassandra.testing.utils.tls.CertificateBuilder;

import static org.apache.cassandra.sidecar.ExecutorPoolsHelper.createdSharedTestPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link org.apache.cassandra.sidecar.acl.authentication.CassandraIdentityExtractor}
 */
class CassandraIdentityExtractorTest
{
    Vertx vertx;
    ExecutorPools executorPools;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        executorPools = createdSharedTestPool(vertx);
    }

    @AfterEach
    void teardown()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testExtractingIdentityWithRole() throws Exception
    {
        IdentityToRoleCache cache = identityRoleCache();
        cache.warmUp(5);

        CassandraIdentityExtractor identityExtractor = new CassandraIdentityExtractor(cache, Collections.emptySet());

        X509Certificate certificate = certificate("spiffe://cassandra/sidecar/test");
        assertThat(identityExtractor.validIdentities(new CertificateCredentials(certificate)).size()).isOne();
        assertThat(identityExtractor.validIdentities(new CertificateCredentials(certificate))).contains("spiffe://cassandra/sidecar/test");
    }

    @Test
    void testExtractingIdentityWithoutRole() throws Exception
    {
        IdentityToRoleCache cache = identityRoleCache();
        cache.warmUp(5);

        CassandraIdentityExtractor identityExtractor = new CassandraIdentityExtractor(cache, Collections.emptySet());

        X509Certificate certificate = certificate("spiffe://identity/without/role");
        assertThatThrownBy(() -> identityExtractor.validIdentities(new CertificateCredentials(certificate)))
        .isInstanceOf(CredentialValidationException.class);
    }

    @Test
    void testAdminIdentities() throws Exception
    {
        IdentityToRoleCache cache = identityRoleCache();

        // passing empty cache
        CassandraIdentityExtractor identityExtractor = new CassandraIdentityExtractor(cache, Collections.singleton("spiffe://sidecar/admin/identity"));

        X509Certificate certificate = certificate("spiffe://sidecar/admin/identity");
        assertThat(identityExtractor.validIdentities(new CertificateCredentials(certificate)).size()).isOne();
        assertThat(identityExtractor.validIdentities(new CertificateCredentials(certificate))).contains("spiffe://sidecar/admin/identity");
    }

    @Test
    void testEmptyIdentities() throws Exception
    {
        IdentityToRoleCache cache = identityRoleCache();

        // passing empty cache
        CassandraIdentityExtractor identityExtractor = new CassandraIdentityExtractor(cache, Collections.emptySet());
        X509Certificate certificate = certificate("spiffe://sidecar/admin/identity");
        assertThatThrownBy(() -> identityExtractor.validIdentities(new CertificateCredentials(certificate))).isInstanceOf(CredentialValidationException.class);
    }

    private IdentityToRoleCache identityRoleCache()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.findRoleFromIdentity("spiffe://cassandra/sidecar/test")).thenReturn("cassandra-role");
        when(mockDbAccessor.findAllIdentityToRoles()).thenReturn(Collections.singletonMap("spiffe://cassandra/sidecar/test", "cassandra-role"));

        SidecarConfiguration mockSidecarConfig = mock(SidecarConfiguration.class);
        AccessControlConfiguration mockAccessControlConfig = mock(AccessControlConfiguration.class);
        when(mockSidecarConfig.accessControlConfiguration()).thenReturn(mockAccessControlConfig);
        CacheConfiguration mockCacheConfig = mock(CacheConfiguration.class);
        when(mockCacheConfig.enabled()).thenReturn(true);
        when(mockCacheConfig.expireAfterAccess()).thenReturn(MillisecondBoundConfiguration.parse("3s"));
        when(mockCacheConfig.maximumSize()).thenReturn(10L);
        when(mockAccessControlConfig.permissionCacheConfiguration()).thenReturn(mockCacheConfig);

        return new IdentityToRoleCache(vertx, executorPools, mockSidecarConfig, mockDbAccessor);
    }

    private X509Certificate certificate(String identity) throws Exception
    {
        return new CertificateBuilder()
               .subject("CN=Sidecar Auth, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
               .addSanUriName(identity)
               .buildSelfSigned()
               .certificate();
    }
}
