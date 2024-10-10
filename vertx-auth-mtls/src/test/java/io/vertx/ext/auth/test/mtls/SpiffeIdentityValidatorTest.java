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

package io.vertx.ext.auth.test.mtls;

import org.junit.jupiter.api.Test;

import io.vertx.ext.auth.mtls.impl.SpiffeIdentityValidator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link io.vertx.ext.auth.mtls.impl.SpiffeIdentityValidator}
 */
public class SpiffeIdentityValidatorTest
{
    @Test
    public void testValidIdentity()
    {
        SpiffeIdentityValidator identityValidator = new SpiffeIdentityValidator("testCerts");
        assertThat(identityValidator.isValidIdentity("spiffe://testCerts/auth/mtls")).isTrue();
    }

    @Test
    public void testInvalidPrefix()
    {
        SpiffeIdentityValidator identityValidator = new SpiffeIdentityValidator("testCerts");
        assertThat(identityValidator.isValidIdentity("testCerts/auth/mtls")).isFalse();
    }

    @Test
    public void testInvalidIdentity()
    {
        SpiffeIdentityValidator identityValidator = new SpiffeIdentityValidator("testCerts");
        assertThat(identityValidator.isValidIdentity("spiffe://withNoPath")).isFalse();
        assertThat(identityValidator.isValidIdentity("spiffe://")).isFalse();
    }

    @Test
    public void testNonTrustedDomain()
    {
        SpiffeIdentityValidator identityValidator = new SpiffeIdentityValidator("testCerts");
        assertThat(identityValidator.isValidIdentity("spiffe://nonTrustedDomain/auth/mtls")).isFalse();
    }
}
