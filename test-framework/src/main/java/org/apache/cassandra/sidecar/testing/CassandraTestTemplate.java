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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
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
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.sidecar.adapters.base.CassandraFactory;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.SimpleCassandraVersion;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.utils.SidecarVersionProvider;


/**
 * Creates a test per version of Cassandra we are testing
 * Tests must be marked with {@link CassandraIntegrationTest}
 * <p>
 * This is a mix of parameterized tests + a custom extension.  we need to be able to provide the test context
 * to each test (like an extension) but also need to create multiple tests (like parameterized tests).  Unfortunately
 * the two don't play well with each other.  You can't get access to the parameters from the extension.
 * This test template allows us full control of the test lifecycle and lets us tightly couple the context to each test
 * we generate, since the same test can be run for multiple versions of C*.
 */
public class CassandraTestTemplate implements TestTemplateInvocationContextProvider
{

    private static final Logger logger = LoggerFactory.getLogger(CassandraTestTemplate.class);
    private static SidecarVersionProvider svp = new SidecarVersionProvider("/sidecar.version");

    private UpgradeableCluster cluster;

    @Override
    public boolean supportsTestTemplate(ExtensionContext context)
    {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context)
    {
        return new TestVersionSupplier().testVersions()
                                        .map(v -> invocationContext(v, context));
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
        return new TestTemplateInvocationContext()
        {
            private CassandraTestContext cassandraTestContext;

            /**
             * A display name can be configured per test still - this adds the C* version we're testing automatically
             * as a suffix to the name
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
             * @return a list of registered {@link Extension extensions}
             */
            @Override
            public List<Extension> getAdditionalExtensions()
            {
                return Arrays.asList(parameterResolver(), postProcessor(), beforeEach());
            }

            private BeforeEachCallback beforeEach()
            {
                return beforeEachCtx -> {
                    CassandraIntegrationTest annotation =
                    context.getElement().map(e -> e.getAnnotation(CassandraIntegrationTest.class)).get();
                    // spin up a C* cluster using the in-jvm dtest
                    Versions versions = Versions.find();
                    int nodesPerDc = annotation.nodesPerDc();
                    int dcCount = annotation.numDcs();
                    int newNodesPerDc = annotation.newNodesPerDc(); // if the test wants to add more nodes later
                    int finalNodeCount = dcCount * (nodesPerDc + newNodesPerDc);
                    Versions.Version requestedVersion = versions.getLatest(new Semver(version.version(),
                                                                                      Semver.SemverType.LOOSE));
                    UpgradeableCluster.Builder builder =
                    UpgradeableCluster.build(nodesPerDc)
                                      .withVersion(requestedVersion)
                                      .withDCs(dcCount)
                                      .withDataDirCount(annotation.numDataDirsPerInstance())
                                      .withConfig(config -> {
                                          if (annotation.nativeTransport())
                                          {
                                              config.with(Feature.NATIVE_PROTOCOL);
                                          }
                                          if (annotation.jmx())
                                          {
                                              config.with(Feature.JMX);
                                          }
                                          if (annotation.gossip())
                                          {
                                              config.with(Feature.GOSSIP);
                                          }
                                          if (annotation.network())
                                          {
                                              config.with(Feature.NETWORK);
                                          }
                                      });
                    TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(finalNodeCount,
                                                                                        builder.getTokenCount());
                    builder.withTokenSupplier(tokenSupplier);
                    cluster = builder.start();

                    logger.info("Testing {} against in-jvm dtest cluster", version);
                    CassandraVersionProvider versionProvider = cassandraVersionProvider(DnsResolver.DEFAULT);
                    SimpleCassandraVersion versionParsed = SimpleCassandraVersion.create(version.version());
                    cassandraTestContext = new CassandraTestContext(versionParsed, cluster, versionProvider);
                    logger.info("Created test context {}", cassandraTestContext);
                };
            }

            /**
             * Shuts down the in-jvm dtest cluster when the test is finished
             * @return the {@link AfterTestExecutionCallback}
             */
            private AfterTestExecutionCallback postProcessor()
            {
                return postProcessorCtx -> {
                    // Tear down the client-side before the cluster as we need to close some server-side connections
                    // that can only be closed by clients?
                    cassandraTestContext.close();
                    // tear down the in-jvm cluster
                    cluster.close();
                };
            }

            /**
             * Required for Junit to know the CassandraTestContext can be used in these tests
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
                        return parameterContext.getParameter().getType().equals(CassandraTestContext.class);
                    }

                    @Override
                    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
                    {
                        return cassandraTestContext;
                    }
                };
            }
        };
    }

    public CassandraVersionProvider cassandraVersionProvider(DnsResolver dnsResolver)
    {
        return new CassandraVersionProvider.Builder()
               .add(new CassandraFactory(dnsResolver, svp.sidecarVersion())).build();
    }

    static
    {
        // Settings to reduce the test setup delay incurred if gossip is enabled
        System.setProperty("cassandra.ring_delay_ms", "5000"); // down from 30s default
        System.setProperty("cassandra.consistent.rangemovement", "false");
        System.setProperty("cassandra.consistent.simultaneousmoves.allow", "true");
        // End gossip delay settings
        // Set the location of dtest jars
        System.setProperty("cassandra.test.dtest_jar_path", "dtest-jars");
    }
}
