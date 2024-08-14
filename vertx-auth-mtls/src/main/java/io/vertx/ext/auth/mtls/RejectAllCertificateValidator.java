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

import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.mtls.exceptions.AuthenticationException;

/**
 * Certificate validator that rejects all certificates
 */
public class RejectAllCertificateValidator implements MutualTlsCertificateValidator
{
    /**
     * Always rejects credentials
     *
     * @param credentials client credentials
     * @return {@code false}
     */
    public boolean isValidCertificate(Credentials credentials)
    {
        return false;
    }

    /**
     * Returns the default reject all identity
     *
     * @param clientCertificateChain client certificate chain
     * @return default identity
     * @throws AuthenticationException when identity cannot be extracted
     */
    public String identity(Certificate[] clientCertificateChain) throws AuthenticationException
    {
        return "RejectAll";
    }
}