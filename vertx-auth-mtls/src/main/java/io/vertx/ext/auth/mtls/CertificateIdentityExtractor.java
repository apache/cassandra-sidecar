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

import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;

import io.vertx.ext.auth.authentication.CredentialValidationException;

/**
 * Extracts a valid identity from certificate chain provided.
 */
public interface CertificateIdentityExtractor
{
    /**
     * Extracts identity out of {@code Certificate[]} certificate chain. If required, this identity can later be used
     * for authorizing the user's resource level permissions.
     *
     * <p>An example of identity could be the following:
     * <ul>
     *  <li>an identifier in SAN of the certificate like SPIFFE
     *  <li>CN of the certificate
     *  <li>any other fields in the certificate can be combined and be used as identifier of the certificate
     * </ul>
     *
     * @param certificateChain certificate chain of user
     * @return identity {@code String} extracted from certificate
     * @throws CredentialValidationException when identity cannot be extracted
     */
    String identity(Certificate[] certificateChain) throws CredentialValidationException, CertificateParsingException;
}
