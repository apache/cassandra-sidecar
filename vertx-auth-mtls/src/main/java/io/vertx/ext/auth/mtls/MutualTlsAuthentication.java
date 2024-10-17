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

package io.vertx.ext.auth.mtls;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.ext.auth.authentication.AuthenticationProvider;
import io.vertx.ext.auth.mtls.impl.MutualTlsAuthenticationImpl;

/**
 * Factory interface for creating a MTLS {@link io.vertx.ext.auth.authentication.AuthenticationProvider}.
 */
@VertxGen
public interface MutualTlsAuthentication extends AuthenticationProvider
{
    /**
     * Create a MTLS authentication provider
     *
     * @param certificateValidator {@link CertificateValidator} for validating details within {@link io.vertx.ext.auth.authentication.CertificateCredentials}
     * @param identityExtractor    {@link CertificateIdentityExtractor} for extracting valid identity out of {@link io.vertx.ext.auth.authentication.CertificateCredentials}
     * @return the authentication provider
     */
    static MutualTlsAuthentication create(CertificateValidator certificateValidator,
                                          CertificateIdentityExtractor identityExtractor)
    {
        return new MutualTlsAuthenticationImpl(certificateValidator, identityExtractor);
    }
}
