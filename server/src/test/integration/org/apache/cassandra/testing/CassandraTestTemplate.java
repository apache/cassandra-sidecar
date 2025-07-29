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

package org.apache.cassandra.testing;

import java.lang.reflect.AnnotatedElement;
import java.net.BindException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.sidecar.common.server.utils.ThrowableUtils;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.testing.utils.tls.CertificateBuilder;
import org.apache.cassandra.testing.utils.tls.CertificateBundle;


/**
 * Creates a test per version of Cassandra we are testing
 * Tests must be marked with {@link CassandraIntegrationTest}
 *
 * <p>This is a mix of parameterized tests + a custom extension.  We need to be able to provide the test context
 * to each test (like an extension) but also need to create multiple tests (like parameterized tests). Unfortunately,
 * the two don't play well with each other.  You can't get access to the parameters from the extension.
 * This test template allows us full control of the test lifecycle and lets us tightly couple the context to each test
 * we generate, since the same test can be run for multiple versions of C*.
 */
public class CassandraTestTemplate implements TestTemplateInvocationContextProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTestTemplate.class);
    private static final int MIN_VERSION_WITH_MTLS = 5;
    private final String truststorePassword = "password";
    private final String serverKeystorePassword = "password";

    private AbstractCassandraTestContext cassandraTestContext;
    private IsolatedDTestClassLoaderWrapper classLoaderWrapper;

    @Override
    public boolean supportsTestTemplate(ExtensionContext context)
    {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context)
    {
        CassandraIntegrationTest annotation = getCassandraIntegrationTestAnnotation(context, true);
        if (annotation.versionDependent())
        {
            return TestVersionSupplier.testVersions()
                                      .map(v -> invocationContext(v, context));
        }
        return Stream.of(invocationContext(TestVersionSupplier.testVersions().findFirst().get(), context));
    }

    /**
     * Returns a {@link TestTemplateInvocationContext}
     *
     * @param version a version for the test
     * @param context the <em>context</em> in which the current test or container is being executed.
     * @return the <em>context</em> of a single invocation of a
     * {@linkplain org.junit.jupiter.api.TestTemplate test template}
     */
    private TestTemplateInvocationContext invocationContext(TestVersion version, ExtensionContext context)
    {
        return new CassandraTestTemplateInvocationContext(context, version);
    }

    private static CassandraIntegrationTest getCassandraIntegrationTestAnnotation(ExtensionContext context,
                                                                                  boolean throwIfNotFound)
    {
        Optional<AnnotatedElement> annotatedElement = context.getElement();
        CassandraIntegrationTest result = annotatedElement.map(e -> e.getAnnotation(CassandraIntegrationTest.class))
                                                          .orElse(null);
        if (result == null && throwIfNotFound)
        {
            throw new RuntimeException("CassandraTestTemplate could not "
                                       + "find @CassandraIntegrationTest annotation");
        }
        return result;
    }

    private final class CassandraTestTemplateInvocationContext implements TestTemplateInvocationContext
    {
        private final ExtensionContext context;
        private final TestVersion version;

        private CassandraTestTemplateInvocationContext(ExtensionContext context, TestVersion version)
        {
            this.context = context;
            this.version = version;
        }

        /**
         * A display name can be configured per test still - this adds the C* version we're testing automatically
         * as a suffix to the name
         *
         * @param invocationIndex the index to the invocation
         * @return the display name
         */
        @Override
        public String getDisplayName(int invocationIndex)
        {
            return context.getDisplayName() + ": " + version.version();
        }

        /**
         * Used to register the extensions required to start and stop the in-jvm dtest environment
         *
         * @return a list of registered {@link Extension extensions}
         */
        @Override
        public List<Extension> getAdditionalExtensions()
        {
            return Arrays.asList(parameterResolver(), afterEach(), beforeEach());
        }

        private BeforeEachCallback beforeEach()
        {
            return beforeEachCtx -> {
                CassandraIntegrationTest annotation = getCassandraIntegrationTestAnnotation(context, true);
                // spin up a C* cluster using the in-jvm dtest
                Versions versions = Versions.find();
                int nodesPerDc = annotation.nodesPerDc();
                int dcCount = annotation.numDcs();
                int newNodesPerDc = annotation.newNodesPerDc(); // if the test wants to add more nodes later
                Preconditions.checkArgument(newNodesPerDc >= 0,
                                            "newNodesPerDc cannot be a negative number");
                int finalNodeCount = dcCount * (nodesPerDc + newNodesPerDc);
                Versions.Version requestedVersion = versions.getLatest(new Semver(version.version(),
                                                                                  Semver.SemverType.LOOSE));
                SimpleCassandraVersion versionParsed = SimpleCassandraVersion.create(version.version());

                classLoaderWrapper = new IsolatedDTestClassLoaderWrapper();
                classLoaderWrapper.initializeDTestJarClassLoader(version, TestVersion.class);

                Map<String, Object> additionalInstanceConfig = new HashMap<>();
                ClusterBuilderConfiguration clusterConfiguration =
                new ClusterBuilderConfiguration().nodesPerDc(nodesPerDc)
                                                 .newNodesPerDc(newNodesPerDc)
                                                 .dynamicPortAllocation(true)
                                                 .dcCount(dcCount)
                                                 .numDataDirsPerInstance(annotation.numDataDirsPerInstance())
                                                 .additionalInstanceConfig(additionalInstanceConfig);

                processAnnotationToFeatureConfiguration(annotation, clusterConfiguration);

                Path tempDirPath = Files.createTempDirectory("certs");
                CertificateBundle ca = ca();
                Path serverKeystorePath = serverKeystorePath(ca, tempDirPath, finalNodeCount);
                Path truststorePath = truststorePath(ca, tempDirPath);

                switch (annotation.authMode())
                {
                    case PASSWORD:
                    {
                        additionalInstanceConfig.put("authenticator", "org.apache.cassandra.auth.PasswordAuthenticator");
                        break;
                    }
                    case MUTUAL_TLS:
                    {
                        // mTLS authentication was added in Cassandra starting 5.0 version
                        if (requestedVersion.version.getMajor() >= MIN_VERSION_WITH_MTLS)
                        {
                            additionalInstanceConfig.put("authenticator.class_name", "org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator");
                            additionalInstanceConfig.put("authenticator.parameters", Collections.singletonMap("validator_class_name",
                                                                                                              "org.apache.cassandra.auth.SpiffeCertificateValidator"));
                            additionalInstanceConfig.put("role_manager", "CassandraRoleManager");
                            additionalInstanceConfig.put("authorizer", "CassandraAuthorizer");
                            additionalInstanceConfig.put("client_encryption_options.enabled", "true");
                            additionalInstanceConfig.put("client_encryption_options.optional", "true");
                            additionalInstanceConfig.put("client_encryption_options.require_client_auth", "true");
                            additionalInstanceConfig.put("client_encryption_options.require_endpoint_verification", "false");
                            additionalInstanceConfig.put("client_encryption_options.keystore", serverKeystorePath.toAbsolutePath().toString());
                            additionalInstanceConfig.put("client_encryption_options.keystore_password", serverKeystorePassword);
                            additionalInstanceConfig.put("client_encryption_options.truststore", truststorePath.toAbsolutePath().toString());
                            additionalInstanceConfig.put("client_encryption_options.truststore_password", truststorePassword);
                        }
                        break;
                    }
                    default:
                }

                if (annotation.enableSsl() && !annotation.authMode().equals(AuthMode.MUTUAL_TLS))
                {
                    // dot-separated options are not supported in 4.0
                    additionalInstanceConfig.put("client_encryption_options", Map.of("enabled", "true",
                                                                                     "require_client_auth", "false",
                                                                                     "keystore", serverKeystorePath.toAbsolutePath().toString(),
                                                                                     "keystore_password", serverKeystorePassword));
                }

                if (annotation.buildCluster())
                {
                    IClusterExtension<? extends IInstance> cluster;
                    clusterConfiguration.startCluster(annotation.startCluster());
                    cluster = retriableStartCluster(classLoaderWrapper, version.version(), clusterConfiguration, 3);
                    cassandraTestContext = new CassandraTestContext(versionParsed, cluster, ca, serverKeystorePath,
                                                                    truststorePath, annotation);
                }
                else
                {
                    cassandraTestContext = new ConfigurableCassandraTestContext(versionParsed, clusterConfiguration, ca,
                                                                                serverKeystorePath, truststorePath,
                                                                                annotation, classLoaderWrapper, version.version());
                }
                LOGGER.info("Testing {} against in-jvm dtest cluster", version);
                LOGGER.info("Created Cassandra test context {}", cassandraTestContext);
            }

            ;
        }

        /**
         * Shuts down the in-jvm dtest cluster after an individual test and any user-defined teardown methods
         * have been executed
         *
         * @return the {@link AfterEachCallback}
         */
        private AfterEachCallback afterEach()
        {
            return postProcessorCtx -> {
                if (cassandraTestContext != null)
                {
                    cassandraTestContext.close();
                }
                if (classLoaderWrapper != null)
                {
                    classLoaderWrapper.closeDTestJarClassLoader();
                }
            };
        }

        /**
         * Configures the list {@link Feature features} requested in the {@link CassandraIntegrationTest} annotation
         * in the {@link ClusterBuilderConfiguration} object.
         *
         * @param annotation           the configured annotation
         * @param clusterConfiguration the configuration for the cluster build
         */
        private void processAnnotationToFeatureConfiguration(CassandraIntegrationTest annotation, ClusterBuilderConfiguration clusterConfiguration)
        {
            if (annotation.nativeTransport())
            {
                clusterConfiguration.requestFeature(Feature.NATIVE_PROTOCOL);
            }
            if (annotation.jmx())
            {
                clusterConfiguration.requestFeature(Feature.JMX);
            }
            if (annotation.gossip())
            {
                clusterConfiguration.requestFeature(Feature.GOSSIP);
            }
            if (annotation.network())
            {
                clusterConfiguration.requestFeature(Feature.NETWORK);
            }
        }

        /**
         * Required for Junit to know the CassandraTestContext can be used in these tests
         *
         * @return a {@link ParameterResolver}
         */
        private ParameterResolver parameterResolver()
        {
            return new ParameterResolver()
            {
                @Override
                public boolean supportsParameter(ParameterContext parameterContext,
                                                 ExtensionContext extensionContext)
                {
                    Class<?> parameterType = parameterContext.getParameter().getType();
                    CassandraIntegrationTest annotation =
                    getCassandraIntegrationTestAnnotation(extensionContext, false);
                    if (annotation == null)
                    {
                        return false;
                    }
                    if (parameterType.equals(AbstractCassandraTestContext.class))
                    {
                        return true;
                    }
                    if (annotation.buildCluster())
                    {
                        if (parameterType.equals(CassandraTestContext.class))
                        {
                            return true;
                        }
                        else if (parameterType.equals(ConfigurableCassandraTestContext.class))
                        {
                            throw new IllegalArgumentException("CassandraIntegrationTest.buildCluster is true but"
                                                               + " a configurable context was requested. Please "
                                                               + "either request a CassandraTestContext "
                                                               + "as a parameter or set buildCluster to false");
                        }
                    }
                    else
                    {
                        if (parameterType.equals(ConfigurableCassandraTestContext.class))
                        {
                            return true;
                        }
                        else if (parameterType.equals(CassandraTestContext.class))
                        {
                            throw new IllegalArgumentException("CassandraIntegrationTest.buildCluster is false "
                                                               + "but a built cluster was requested. Please "
                                                               + "either request a "
                                                               + "ConfigurableCassandraTestContext as a "
                                                               + "parameter or set buildCluster to true"
                                                               + "(the default)");
                        }
                    }
                    return false;
                }

                @Override
                public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
                {
                    return cassandraTestContext;
                }
            };
        }
    }

    private CertificateBundle ca() throws Exception
    {
        return new CertificateBuilder()
               .subject("CN=Apache cassandra Root CA, OU=Certification Authority, O=Unknown, C=Unknown")
               .isCertificateAuthority(true)
               .buildSelfSigned();
    }

    private Path truststorePath(CertificateBundle ca, Path path) throws Exception
    {
        return ca.toTempKeyStorePath(path, truststorePassword.toCharArray(), truststorePassword.toCharArray());
    }

    private Path serverKeystorePath(CertificateBundle ca, Path path, int totalNodes) throws Exception
    {
        CertificateBuilder builder = new CertificateBuilder();
        builder.subject("CN=Apache Cassandra, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
               .addSanDnsName("localhost");
        for (int i = 1; i <= totalNodes; i++)
        {
            builder.addSanIpAddress("127.0.0." + i);
        }
        CertificateBundle keystore = builder.buildIssuedBy(ca);
        return keystore.toTempKeyStorePath(path, serverKeystorePassword.toCharArray(), serverKeystorePassword.toCharArray());
    }

    public static IClusterExtension<? extends IInstance> retriableStartCluster(IsolatedDTestClassLoaderWrapper classLoaderWrapper,
                                                                               String testVersion,
                                                                               ClusterBuilderConfiguration clusterConfiguration,
                                                                               int maxAttempts)
    {
        Throwable lastCause = null;
        for (int i = 0; i < maxAttempts; i++)
        {
            try
            {
                return classLoaderWrapper.loadCluster(testVersion, clusterConfiguration);
            }
            catch (Throwable cause)
            {
                boolean addressAlreadyInUse = ThrowableUtils.getCause(cause, CassandraTestTemplate::portNotAvailableToBind) != null;
                if (addressAlreadyInUse)
                {
                    LOGGER.warn("Failed to provision cluster due to port collision after {} retries", i, cause);
                    lastCause = cause;
                }
                else
                {
                    throw new RuntimeException("Failed to provision cluster", cause);
                }
            }
        }

        throw new RuntimeException("Failed to provision cluster after exhausting all attempts", lastCause);
    }

    private static boolean portNotAvailableToBind(Throwable cause)
    {
        return (cause instanceof BindException && StringUtils.contains(cause.getMessage(), "Address already in use"))
               || StringUtils.contains(cause.getMessage(), "is in use by another process");
    }

    static
    {
        System.setProperty("cassandra.ring_delay_ms", "5000"); // down from 30s default; this change has no effect if GOSSIP feature is enabled
        System.setProperty("cassandra.consistent.rangemovement", "false");
        System.setProperty("cassandra.consistent.simultaneousmoves.allow", "true");
        // End gossip delay settings
        // Set the location of dtest jars
        System.setProperty("cassandra.test.dtest_jar_path",
                           System.getProperty("cassandra.test.dtest_jar_path", "dtest-jars"));
        // Disable tcnative in netty as it can cause jni issues and logs lots errors
        System.setProperty("cassandra.disable_tcactive_openssl", "true");
        // As we enable gossip by default, make the checks happen faster
        System.setProperty("cassandra.gossip_settle_min_wait_ms", "500"); // Default 5000
        System.setProperty("cassandra.gossip_settle_interval_ms", "250"); // Default 1000
        System.setProperty("cassandra.gossip_settle_poll_success_required", "6"); // Default 3
    }
}
