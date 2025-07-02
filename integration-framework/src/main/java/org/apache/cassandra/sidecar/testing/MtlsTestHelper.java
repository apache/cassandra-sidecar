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

package org.apache.cassandra.sidecar.testing;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.cassandra.testing.utils.tls.CertificateBuilder;
import org.apache.cassandra.testing.utils.tls.CertificateBundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class that encapsulates testing with Mutual TLS.
 */
public class MtlsTestHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MtlsTestHelper.class);
    public static final char[] EMPTY_PASSWORD = new char[0];
    public static final String EMPTY_PASSWORD_STRING = "";
    /**
     * A system property that can enable / disable testing with Mutual TLS
     */
    public static final String CASSANDRA_INTEGRATION_TEST_ENABLE_MTLS = "cassandra.integration.sidecar.test.enable_mtls";
    private final boolean enableMtlsForTesting;
    CertificateBundle certificateAuthority;
    Path truststorePath;
    Path serverKeyStorePath;
    Path clientKeyStorePath;

    public MtlsTestHelper(Path secretsPath) throws Exception
    {
        this(secretsPath, System.getProperty(CASSANDRA_INTEGRATION_TEST_ENABLE_MTLS, "false")
                                .equals("true"));
    }

    public MtlsTestHelper(Path secretsPath,
                          boolean enableMtlsForTesting)
            throws Exception
    {
        this.enableMtlsForTesting = enableMtlsForTesting;
        maybeInitializeSecrets(Objects.requireNonNull(secretsPath, "secretsPath cannot be null"));
    }

    void maybeInitializeSecrets(Path secretsPath) throws Exception
    {
        if (!enableMtlsForTesting)
        {
            return;
        }

        certificateAuthority = new CertificateBuilder().subject("CN=Apache Cassandra Root CA, OU=Certification Authority, O=Unknown, C=Unknown")
                                                       .alias("fakerootca")
                                                       .isCertificateAuthority(true)
                                                       .buildSelfSigned();
        truststorePath = certificateAuthority.toTempKeyStorePath(secretsPath, EMPTY_PASSWORD, EMPTY_PASSWORD);

        CertificateBuilder serverKeyStoreBuilder = new CertificateBuilder().subject(
                "CN=Apache Cassandra, OU=mtls_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                                                           .addSanDnsName("localhost");
        // Add SANs for every potential hostname Sidecar will serve
        for (int i = 1; i <= 20; i++)
        {
            serverKeyStoreBuilder.addSanDnsName("localhost" + i);
        }

        CertificateBundle serverKeyStore = serverKeyStoreBuilder.buildIssuedBy(certificateAuthority);
        serverKeyStorePath = serverKeyStore.toTempKeyStorePath(secretsPath, EMPTY_PASSWORD, EMPTY_PASSWORD);
        CertificateBundle clientKeyStore = new CertificateBuilder().subject("CN=Apache Cassandra, OU=mtls_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                                                   .addSanDnsName("localhost")
                                                                   .buildIssuedBy(certificateAuthority);
        clientKeyStorePath = clientKeyStore.toTempKeyStorePath(secretsPath, EMPTY_PASSWORD, EMPTY_PASSWORD);
    }

    public boolean isEnabled()
    {
        return enableMtlsForTesting;
    }

    public String trustStorePath()
    {
        return truststorePath.toString();
    }

    public String clientKeyStorePath()
    {
        return clientKeyStorePath.toString();
    }

    public String trustStorePassword()
    {
        return EMPTY_PASSWORD_STRING;
    }

    public String trustStoreType()
    {
        return "PKCS12";
    }

    public String serverKeyStorePath()
    {
        return serverKeyStorePath.toString();
    }

    public String serverKeyStorePassword()
    {
        return EMPTY_PASSWORD_STRING;
    }

    public String serverKeyStoreType()
    {
        return "PKCS12";
    }

    public Map<String, String> mtlOptionMap()
    {
        if (!isEnabled())
        {
            return Collections.emptyMap();
        }

        LOGGER.info("Test mTLS certificate is enabled. " + "Will use test keystore as truststore so the client will trust the server");
        Map<String, String> optionMap = new HashMap<>();
        optionMap.put("truststore_path", trustStorePath());
        optionMap.put("truststore_password", EMPTY_PASSWORD_STRING);
        optionMap.put("truststore_type", trustStoreType());
        optionMap.put("keystore_path", clientKeyStorePath.toString());
        optionMap.put("keystore_password", EMPTY_PASSWORD_STRING);
        optionMap.put("keystore_type", "PKCS12");
        return optionMap;
    }
}
