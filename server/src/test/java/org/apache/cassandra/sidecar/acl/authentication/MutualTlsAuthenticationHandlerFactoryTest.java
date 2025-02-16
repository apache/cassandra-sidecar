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

package org.apache.cassandra.sidecar.acl.authentication;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

import static org.apache.cassandra.sidecar.ExecutorPoolsHelper.createdSharedTestPool;
import static org.apache.cassandra.sidecar.acl.authentication.MutualTlsAuthenticationHandlerFactory.CERTIFICATE_IDENTITY_EXTRACTOR_PARAM_KEY;
import static org.apache.cassandra.sidecar.acl.authentication.MutualTlsAuthenticationHandlerFactory.CERTIFICATE_VALIDATOR_PARAM_KEY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link org.apache.cassandra.sidecar.acl.authentication.MutualTlsAuthenticationHandlerFactory}
 */
class MutualTlsAuthenticationHandlerFactoryTest
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
    void cleanup()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testNullParameters()
    {
        testConfigError(null, "Parameters cannot be null for MutualTlsAuthenticationHandlerFactory");
    }

    @Test
    void testMissingParameters()
    {
        Map<String, String> missingValidatorParams = new HashMap<String, String>()
        {{
            put(CERTIFICATE_IDENTITY_EXTRACTOR_PARAM_KEY, "org.apache.cassandra.sidecar.acl.authentication.CassandraIdentityExtractor");
        }};
        testConfigError(missingValidatorParams, String.format("Missing %s parameter for MutualTlsAuthenticationHandler creation",
                                                              CERTIFICATE_VALIDATOR_PARAM_KEY));
        Map<String, String> missingExtractorParams = new HashMap<String, String>()
        {{
            put(CERTIFICATE_VALIDATOR_PARAM_KEY, "io.vertx.ext.auth.mtls.impl.AllowAllCertificateValidator");
        }};
        testConfigError(missingExtractorParams, String.format("Missing %s parameter for MutualTlsAuthenticationHandler creation",
                                                              CERTIFICATE_IDENTITY_EXTRACTOR_PARAM_KEY));
    }

    @Test
    void testUnrecognizedParameters()
    {
        Map<String, String> missingValidatorParams = new HashMap<String, String>()
        {{
            put(CERTIFICATE_VALIDATOR_PARAM_KEY, "UnrecognizedCertificateValidator");
            put(CERTIFICATE_IDENTITY_EXTRACTOR_PARAM_KEY, "org.apache.cassandra.sidecar.acl.authentication.CassandraIdentityExtractor");
        }};
        testConfigError(missingValidatorParams, "Error creating MutualTlsAuthenticationHandler");
    }

    private void testConfigError(Map<String, String> parameters, String expectedErrMsg)
    {
        SidecarConfiguration mockSidecarConfig = mock(SidecarConfiguration.class);
        AccessControlConfiguration mockAccessControlConfig = mock(AccessControlConfiguration.class);
        CacheConfiguration mockCacheConfig = mock(CacheConfiguration.class);
        when(mockAccessControlConfig.adminIdentities()).thenReturn(Collections.emptySet());
        when(mockAccessControlConfig.permissionCacheConfiguration()).thenReturn(mockCacheConfig);
        when(mockSidecarConfig.accessControlConfiguration()).thenReturn(mockAccessControlConfig);
        SystemAuthDatabaseAccessor mockAccessor = mock(SystemAuthDatabaseAccessor.class);
        IdentityToRoleCache identityToRoleCache = new IdentityToRoleCache(vertx, executorPools, mockSidecarConfig, mockAccessor);
        MutualTlsAuthenticationHandlerFactory factory = new MutualTlsAuthenticationHandlerFactory(identityToRoleCache);
        assertThatThrownBy(() -> factory.create(vertx, mockAccessControlConfig, parameters))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage(expectedErrMsg);
    }
}
