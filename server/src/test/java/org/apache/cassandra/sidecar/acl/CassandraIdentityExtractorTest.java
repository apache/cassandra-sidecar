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
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
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
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetricsImpl;
import org.apache.cassandra.testing.utils.tls.CertificateBuilder;

import static org.apache.cassandra.sidecar.ExecutorPoolsHelper.createdSharedTestPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link org.apache.cassandra.sidecar.acl.authentication.CassandraIdentityExtractor}
 */
@ExtendWith(VertxExtension.class)
class CassandraIdentityExtractorTest
{
    private static final MetricRegistryFactory FACTORY
    = new MetricRegistryFactory(CassandraIdentityExtractorTest.class.getName(),
                                Collections.emptyList(),
                                Collections.emptyList());
    Vertx vertx;
    ExecutorPools executorPools;
    SidecarMetrics sidecarMetrics;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        executorPools = createdSharedTestPool(vertx);
        sidecarMetrics = new SidecarMetricsImpl(FACTORY, null);
    }

    @AfterEach
    void teardown()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testExtractingIdentityWithRole(VertxTestContext testContext) throws Exception
    {
        IdentityToRoleCache cache = identityRoleCache();
        cache.warmUp(5);

        AdminIdentityResolver mockAdminIdentityResolver = mock(AdminIdentityResolver.class);
        when(mockAdminIdentityResolver.isAdmin("spiffe://cassandra/sidecar/test"))
        .thenReturn(Future.succeededFuture(false));
        CassandraIdentityExtractor identityExtractor = new CassandraIdentityExtractor(mockAdminIdentityResolver, cache);

        X509Certificate certificate = certificate("spiffe://cassandra/sidecar/test");

        identityExtractor.validIdentities(new CertificateCredentials(certificate))
                         .onSuccess(identities -> {
                             testContext.verify(() -> {
                                 assertThat(identities.size()).isOne();
                                 assertThat(identities).contains("spiffe://cassandra/sidecar/test");
                             });
                             testContext.completeNow();
                         })
                         .onFailure(testContext::failNow);
    }

    @Test
    void testExtractingIdentityWithoutRole(VertxTestContext testContext) throws Exception
    {
        IdentityToRoleCache cache = identityRoleCache();
        cache.warmUp(5);

        AdminIdentityResolver mockAdminIdentityResolver = mock(AdminIdentityResolver.class);
        when(mockAdminIdentityResolver.isAdmin("spiffe://identity/without/role"))
        .thenReturn(Future.succeededFuture(false));
        CassandraIdentityExtractor identityExtractor = new CassandraIdentityExtractor(mockAdminIdentityResolver, cache);

        X509Certificate certificate = certificate("spiffe://identity/without/role");
        identityExtractor.validIdentities(new CertificateCredentials(certificate))
                         .onComplete(ar -> {
                             testContext.verify(() -> {
                                 assertThat(ar.failed()).isTrue();
                                 assertThat(ar.cause()).isInstanceOf(CredentialValidationException.class);
                             });
                             testContext.completeNow();
                         });
    }

    @Test
    void testAdminIdentities(VertxTestContext testContext) throws Exception
    {
        IdentityToRoleCache cache = identityRoleCache();

        AdminIdentityResolver mockAdminIdentityResolver = mock(AdminIdentityResolver.class);
        when(mockAdminIdentityResolver.isAdmin("spiffe://sidecar/admin/identity"))
        .thenReturn(Future.succeededFuture(true));

        // passing empty cache
        CassandraIdentityExtractor identityExtractor = new CassandraIdentityExtractor(mockAdminIdentityResolver, cache);

        X509Certificate certificate = certificate("spiffe://sidecar/admin/identity");
        identityExtractor.validIdentities(new CertificateCredentials(certificate))
                         .onSuccess(identities -> {
                             testContext.verify(() -> {
                                 assertThat(identities.size()).isOne();
                                 assertThat(identities).contains("spiffe://sidecar/admin/identity");
                             });
                             testContext.completeNow();
                         })
                         .onFailure(testContext::failNow);
    }

    @Test
    void testEmptyIdentities(VertxTestContext testContext) throws Exception
    {
        IdentityToRoleCache cache = identityRoleCache();

        AdminIdentityResolver mockAdminIdentityResolver = mock(AdminIdentityResolver.class);
        when(mockAdminIdentityResolver.isAdmin("spiffe://sidecar/admin/identity"))
        .thenReturn(Future.succeededFuture(false));

        // passing empty cache
        CassandraIdentityExtractor identityExtractor = new CassandraIdentityExtractor(mockAdminIdentityResolver, cache);
        X509Certificate certificate = certificate("spiffe://sidecar/admin/identity");
        identityExtractor.validIdentities(new CertificateCredentials(certificate))
                         .onComplete(ar -> {
                             testContext.verify(() -> {
                                 assertThat(ar.failed()).isTrue();
                                 assertThat(ar.cause()).isInstanceOf(CredentialValidationException.class);
                             });
                             testContext.completeNow();
                         });
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

        return new IdentityToRoleCache(vertx, executorPools, mockSidecarConfig, mockDbAccessor, sidecarMetrics);
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
