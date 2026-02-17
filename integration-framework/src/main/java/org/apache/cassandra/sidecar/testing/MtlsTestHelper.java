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
import java.util.Objects;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.SidecarClientConfiguration;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarClientConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.apache.cassandra.testing.utils.tls.CertificateBuilder;
import org.apache.cassandra.testing.utils.tls.CertificateBundle;

/**
 * A class that encapsulates testing with Mutual TLS.
 */
public class MtlsTestHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MtlsTestHelper.class);
    public static final String PASSWORD_STRING = "cassandra";
    public static final char[] PASSWORD = PASSWORD_STRING.toCharArray();
    /**
     * A system property that can enable / disable testing with Mutual TLS
     */
    public static final String CASSANDRA_INTEGRATION_TEST_ENABLE_MTLS = "cassandra.integration.sidecar.test.enable_mtls";
    private final boolean enableMtlsForTesting;
    private final Path secretsPath;
    CertificateBundle certificateAuthority;
    Path truststorePath;
    Path serverKeyStorePath;
    Path clientKeyStorePath;

    public MtlsTestHelper(Path secretsPath) throws Exception
    {
        this(secretsPath, System.getProperty(CASSANDRA_INTEGRATION_TEST_ENABLE_MTLS, "false").equals("true"));
    }

    public MtlsTestHelper(Path secretsPath, boolean enableMtlsForTesting) throws Exception
    {
        this.enableMtlsForTesting = enableMtlsForTesting;
        this.secretsPath = Objects.requireNonNull(secretsPath, "secretsPath cannot be null");
        maybeInitializeSecrets();
    }

    void maybeInitializeSecrets() throws Exception
    {
        if (!enableMtlsForTesting)
        {
            return;
        }

        certificateAuthority =
        new CertificateBuilder().subject("CN=Apache cassandra Root CA, OU=Certification Authority, O=Unknown, C=Unknown")
                                .isCertificateAuthority(true)
                                .buildSelfSigned();
        truststorePath = certificateAuthority.toTempKeyStorePath(secretsPath, PASSWORD, PASSWORD);

        CertificateBuilder serverKeyStoreBuilder =
        new CertificateBuilder().subject("CN=Apache Cassandra, OU=mtls_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                .addSanDnsName("localhost");
        // Add SANs for every potential hostname Sidecar will serve
        for (int i = 1; i <= 20; i++)
        {
            serverKeyStoreBuilder.addSanDnsName("localhost" + i)
                                 .addSanIpAddress("127.0.0." + i);
        }

        CertificateBundle serverKeyStore = serverKeyStoreBuilder.buildIssuedBy(certificateAuthority);
        serverKeyStorePath = serverKeyStore.toTempKeyStorePath(secretsPath, PASSWORD, PASSWORD);
        clientKeyStorePath = issueClientKeyStore(null);
    }

    public Path issueClientKeyStore(Consumer<CertificateBuilder> certificateBuilderConsumer) throws Exception
    {
        CertificateBuilder builder = new CertificateBuilder()
                                     .subject("CN=Apache Cassandra, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                     .addSanDnsName("localhost")
                                     .addSanIpAddress("127.0.0.1");

        if (certificateBuilderConsumer != null)
        {
            certificateBuilderConsumer.accept(builder);
        }
        CertificateBundle clientKeystore = builder.buildIssuedBy(certificateAuthority);
        return clientKeystore.toTempKeyStorePath(secretsPath, PASSWORD, PASSWORD);
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

    public String clientKeyStorePassword()
    {
        return PASSWORD_STRING;
    }

    public String trustStorePassword()
    {
        return PASSWORD_STRING;
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
        return PASSWORD_STRING;
    }

    public String serverKeyStoreType()
    {
        return "PKCS12";
    }

    /**
     * Creates SSL configuration with the specified keystore and shared truststore.
     *
     * @param keyStorePath the path to the keystore
     * @param keyStorePassword the keystore password
     * @param keyStoreType the keystore type
     * @return SslConfiguration with the provided keystore and shared truststore
     */
    private SslConfiguration createSslConfiguration(String keyStorePath,
                                                    String keyStorePassword,
                                                    String keyStoreType)
    {
        KeyStoreConfiguration truststoreConfiguration =
        new KeyStoreConfigurationImpl(trustStorePath(),
                                      trustStorePassword(),
                                      trustStoreType(),
                                      SecondBoundConfiguration.parse("60s"));

        KeyStoreConfiguration keyStoreConfiguration =
        new KeyStoreConfigurationImpl(keyStorePath,
                                      keyStorePassword,
                                      keyStoreType,
                                      SecondBoundConfiguration.parse("60s"));

        return SslConfigurationImpl.builder()
                                   .enabled(true)
                                   .keystore(keyStoreConfiguration)
                                   .truststore(truststoreConfiguration)
                                   .build();
    }

    /**
     * Creates SSL configuration for the Sidecar server with mTLS settings if enabled.
     *
     * @return SslConfiguration with server keystore/truststore, or null if mTLS is not enabled
     */
    public SslConfiguration createServerSslConfiguration()
    {
        if (!isEnabled())
        {
            LOGGER.info("Not enabling mTLS for testing. Set '{}' to 'true' if you would like mTLS enabled.",
                        CASSANDRA_INTEGRATION_TEST_ENABLE_MTLS);
            return null;
        }

        LOGGER.info("Enabling test mTLS certificate/keystore for server.");
        return createSslConfiguration(serverKeyStorePath(),
                                      serverKeyStorePassword(),
                                      serverKeyStoreType());
    }

    /**
     * Creates a SidecarClientConfiguration with mTLS settings if mTLS is enabled.
     *
     * @return a SidecarClientConfiguration with mTLS settings, or null if mTLS is not enabled
     */
    public SidecarClientConfiguration createSidecarClientConfiguration()
    {
        if (!isEnabled())
        {
            LOGGER.info("Not enabling mTLS for testing. Set '{}' to 'true' if you would like mTLS enabled.",
                        CASSANDRA_INTEGRATION_TEST_ENABLE_MTLS);
            return new SidecarClientConfigurationImpl(null);
        }

        LOGGER.info("Enabling test mTLS certificate/keystore for client.");
        SslConfiguration clientSslConfiguration =
        createSslConfiguration(clientKeyStorePath(),
                              clientKeyStorePassword(),
                              serverKeyStoreType());

        return new SidecarClientConfigurationImpl(clientSslConfiguration);
    }
}
