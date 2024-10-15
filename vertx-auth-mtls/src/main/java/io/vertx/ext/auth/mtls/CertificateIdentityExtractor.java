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

import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.CredentialValidationException;

/**
 * {@link CertificateIdentityExtractor} extracts a valid identity from certificate chain. Interface can be extended to
 * implement custom certificate identity validators.
 */
public interface CertificateIdentityExtractor
{
    /**
     * Extracts a valid identity out of {@link CertificateCredentials} certificate chain. This identity can later be used
     * for authorizing user's resource level permissions. If a valid identity could not be extracted, then throws
     * {@code CredentialValidationException}
     *
     * <p>An example of identity could be the following:
     * <ul>
     *  <li>an identifier in SAN of the certificate like SPIFFE
     *  <li>CN of the certificate
     *  <li>any other fields in the certificate can be combined and be used as identifier of the certificate
     * </ul>
     *
     * @param certificateCredentials certificate chain of user that is already verified
     * @return {@code String} identity string extracted from certificate, uniquely represents client
     * @throws CredentialValidationException when a valid identity cannot be extracted from certificate chain.
     */
    String validIdentity(CertificateCredentials certificateCredentials) throws CredentialValidationException;
}
