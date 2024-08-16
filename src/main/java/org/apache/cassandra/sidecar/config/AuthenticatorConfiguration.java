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

package org.apache.cassandra.sidecar.config;

import java.util.Set;

import org.apache.cassandra.sidecar.auth.authentication.AuthenticatorConfig;
import org.apache.cassandra.sidecar.auth.authentication.CertificateValidatorConfig;
import org.apache.cassandra.sidecar.auth.authentication.IdentityValidatorConfig;

/**
 * Encapsulates Authentication Configuration
 */
public interface AuthenticatorConfiguration
{
    AuthenticatorConfig DEFAULT_AUTHENTICATOR = AuthenticatorConfig.AllowAll;
    IdentityValidatorConfig DEFAULT_ID_VALIDATOR = IdentityValidatorConfig.AllowAll;
    CertificateValidatorConfig DEFAULT_CERT_VALIDATOR = CertificateValidatorConfig.AllowAll;

    /**
     * Returns the desired authentication scheme as provided in the yaml file
     *
     * @return A {@code String} representation of desired authentication scheme
     */
    AuthenticatorConfig authConfig();

    /**
     * Returns the desired certificate validator as provided in the yaml file
     *
     * @return A {@code String} representation of desired certificate validator
     */
    CertificateValidatorConfig certValidator();

    /**
     * Returns the desired identity validator as provided in the yaml file
     *
     * @return A {@code String} representation of desired identity validator
     */
    IdentityValidatorConfig idValidator();

    /**
     * Returns a {@code Set} of authorized identities. Values are all of type {@code String}.
     *
     * @return A {@code Set<String>} of authorized identities
     */
    Set<String> authorizedIdentities();
}
