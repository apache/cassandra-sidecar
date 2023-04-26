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

package org.apache.cassandra.sidecar.common.testing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

import com.datastax.driver.core.NettyOptions;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.ICassandraAdapter;
import org.apache.cassandra.sidecar.common.ICassandraFactory;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.SimpleCassandraVersion;
import org.apache.cassandra.sidecar.common.containers.ExtendedCassandraContainer;

import static org.apache.cassandra.sidecar.common.containers.ExtendedCassandraContainer.JMX_PORT;
import static org.apache.cassandra.sidecar.common.containers.ExtendedCassandraContainer.RMI_SERVER_HOSTNAME;

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
    public static final String CONTAINER_CASSANDRA_DATA_PATH = "/var/lib/cassandra";

    @Override
    public boolean supportsTestTemplate(ExtensionContext context)
    {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context)
    {
        return new TestVersionSupplier().getTestVersions()
                                        .map(v -> invocationContext(v, context));
    }

    /**
     * Returns a {@link TestTemplateInvocationContext}
     *
     * @param version a version for the test
     * @param context the <em>context</em> in which the current test or container is being executed.
     * @return the <em>context</em> of a single invocation of a
     *         {@linkplain org.junit.jupiter.api.TestTemplate test template}
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
                return context.getDisplayName() + ": " + version.getVersion();
            }

            /**
             * Used to register the extensions required to start and stop the docker environment
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
                    // spin up a C* instance using Testcontainers
                    ICassandraFactory factory = version.getFactory();

                    // Create a temp directory to be mounted inside the container
                    Path dataDirectoryPath = Files.createTempDirectory("cassandra-sidecar-test-");
                    ExtendedCassandraContainer container = new ExtendedCassandraContainer(version.getImage())
                                                      // Mount the temp directory as the Cassandra data directory
                                                      .withFileSystemBind(dataDirectoryPath.toFile().getAbsolutePath(),
                                                                          CONTAINER_CASSANDRA_DATA_PATH);
                    container.start();
                    logger.info("Testing {} against docker container", version);

                    CQLSessionProvider session = new CQLSessionProvider(container.getContactPoint(),
                                                                        new NettyOptions());
                    JmxClient jmxClient = new JmxClient(RMI_SERVER_HOSTNAME, JMX_PORT);

                    SimpleCassandraVersion versionParsed = SimpleCassandraVersion.create(version.getVersion());

                    ICassandraAdapter cassandra = factory.create(session, jmxClient);

                    cassandraTestContext = new CassandraTestContext(versionParsed,
                                                                    container,
                                                                    session,
                                                                    jmxClient,
                                                                    cassandra,
                                                                    dataDirectoryPath);
                    logger.info("Created test context {}", cassandraTestContext);
                };
            }

            /**
             * Shuts down the docker container when the test is finished
             * @return the {@link AfterTestExecutionCallback}
             */
            private AfterTestExecutionCallback postProcessor()
            {
                return postProcessorCtx -> {
                    // tear down the docker instance
                    cassandraTestContext.container.stop();
                    // cleanup temp directory
                    deleteDataDirectory(cassandraTestContext.dataDirectoryPath);
                };
            }

            private void deleteDataDirectory(Path dataDirectoryPath)
            {
                try
                {
                    Files.walk(dataDirectoryPath)
                         .map(Path::toFile)
                         .forEach(File::delete);
                    Files.delete(dataDirectoryPath);
                }
                catch (IOException ignored)
                {
                }
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
                        return parameterContext.getParameter()
                                               .getType()
                                               .equals(CassandraTestContext.class);
                    }

                    @Override
                    public Object resolveParameter(ParameterContext parameterContext,
                                                   ExtensionContext extensionContext)
                    {
                        return cassandraTestContext;
                    }
                };
            }
        };
    }
}
