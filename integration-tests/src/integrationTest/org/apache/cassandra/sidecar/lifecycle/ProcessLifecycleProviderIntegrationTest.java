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

package org.apache.cassandra.sidecar.lifecycle;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.copyDirectoryRecursively;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.extractGzippedTarball;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.replacePlaceholdersInFileWithPattern;

/**
 * Process lifecycle provider integration test.
 */
@ExtendWith(VertxExtension.class)
@Tag("heavy")
class ProcessLifecycleProviderIntegrationTest
{
    static final String TEST_NODE = "localhost";
    static final int TEST_NODE_ID = 1;
    static final int TIMEOUT_SECONDS = 30;

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessLifecycleProviderIntegrationTest.class);
    private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    static String sidecarDeploymentId;
    static Path lifecycleDir;

    static final String TARBALL_PATH = System.getProperty("cassandra.test.tarball_path");

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    private static Path tmpDir;

    private static Server server;
    private static Vertx vertx;
    private static WebClient client;

    /**
     * Test-only subclass of ProcessRuntimeConfiguration that adds the -R flag required for Cassandra 5.0+ when running as root
     */
    static class TestProcessRuntimeConfiguration extends ProcessRuntimeConfiguration
    {
        public TestProcessRuntimeConfiguration(Builder builder)
        {
            super(builder);
        }

        @Override
        public ProcessBuilder buildStartCommand(String pidFileLocation, Path stdoutFileLocation, Path stderrFileLocation)
        {
            ProcessBuilder pb = super.buildStartCommand(pidFileLocation, stdoutFileLocation, stderrFileLocation);
            List<String> command = new ArrayList<>(pb.command());
            // Insert -R at position 3 (after cassandraBin, "-p" and pidFileLocation)
            command.add(3, "-R");
            pb.command(command);
            return pb;
        }
    }

    @BeforeAll
    public static void setup() throws Exception
    {
        LOGGER.info("Created temporary directory for test: {}", tmpDir);

        // Setup Cassandra
        Path cassandraInstallDir = tmpDir.resolve("opt");
        Path cassandraConfDir = Files.createDirectories(tmpDir.resolve("etc/cassandra"));
        ProcessRuntimeConfiguration cassandraConfig = installCassandra(cassandraInstallDir, cassandraConfDir);

        // Setup sidecar configuration
        lifecycleDir = Files.createDirectories(tmpDir.resolve("var/lib/sidecar/lifecycle"));
        Path sidecarYaml = createSidecarYaml(cassandraConfig, lifecycleDir);
        LOGGER.info("Testing with cassandra config at: {} and sidecar yaml at: {}", cassandraConfig.cassandraConf(), sidecarYaml);
        configureSidecar(sidecarYaml);

        sidecarDeploymentId = server.start().toCompletionStage().toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);
    }

    @AfterAll
    public static void tearDown() throws ExecutionException, InterruptedException, TimeoutException
    {
        server.stop(sidecarDeploymentId).toCompletionStage().toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);
        // Make sure server is stopped
        forceCassandraStop();
    }

    private static void forceCassandraStop()
    {
        Path pidFileLocation = Path.of(ProcessLifecycleProvider.pidFileLocation(lifecycleDir.toString(), TEST_NODE_ID));
        if (!pidFileLocation.toFile().exists())
        {
            LOGGER.info("No PID file exists, Cassandra already stopped.");
            return;
        }
        Long pid = ProcessLifecycleProvider.readPidFromFile(pidFileLocation);
        try
        {
            Optional<ProcessHandle> processHandle = ProcessHandle.of(pid);
            if (processHandle.isPresent())
            {
                LOGGER.info("Killing Cassandra process with PID {}", pid);
                CompletableFuture<ProcessHandle> terminationFuture = processHandle.get().onExit();
                processHandle.get().destroyForcibly();
                terminationFuture.get(TIMEOUT_SECONDS, SECONDS);
            }
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            LOGGER.error("Failed to kill Cassandra process with PID {}", pid, e);
            throw new RuntimeException("Failed to kill Cassandra process", e);
        }
    }

    static void configureSidecar(Path sidecarYamlPath)
    {
        Injector injector = Guice.createInjector(SidecarModules.all(sidecarYamlPath));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        client = WebClient.create(vertx);
    }

    @Test
    void testProcessLifecycleProviderStartAndStopAndRecoveryAfterCrash() throws Exception
    {
        LifecycleProviderIntegrationTester tester = new LifecycleProviderIntegrationTester(
        client,
        TEST_NODE,
        server.actualPort(),
        ProcessLifecycleProviderIntegrationTest::forceCassandraStop);

        tester.testLifecycleProviderStartAndStopAndRecoveryAfterCrash();
    }

    private static Path createSidecarYaml(ProcessRuntimeConfiguration cassandraConfig, Path lifecycleDir) throws IOException, URISyntaxException
    {
        Path sidecarConfDir = Files.createDirectories(tmpDir.resolve("etc/sidecar"));
        URL sidecarYamlTemplateUrl = ProcessLifecycleProviderIntegrationTest.class.getResource("/config/sidecar.yaml.template");
        Path sidecarYamlTemplatePath = Path.of(Objects.requireNonNull(sidecarYamlTemplateUrl).toURI());
        Path sidecarYaml = sidecarConfDir.resolve("sidecar.yaml");
        replacePlaceholdersInFileWithPattern(sidecarYamlTemplatePath,
                                             Map.of("cassandraHome", cassandraConfig.cassandraHome().toString(),
                                                    "lifecycleDir", lifecycleDir.toString(),
                                                    "cassandraConfDir", cassandraConfig.cassandraConfDir.toString(),
                                                    "cassandraStorageDir", Objects.requireNonNull(cassandraConfig.storageDir).toString(),
                                                    "cassandraLogDir", cassandraConfig.cassandraLogDir.toString()),
                                             sidecarYaml);
        return sidecarYaml;
    }

    public static ProcessRuntimeConfiguration installCassandra(Path installDir, Path confDir) throws IOException
    {
        if (TARBALL_PATH == null || TARBALL_PATH.isEmpty())
        {
            throw new IllegalStateException("System property 'cassandra.test.tarball_path' is not set");
        }
        Files.createDirectories(installDir);
        extractGzippedTarball(Path.of(TARBALL_PATH), installDir);
        File[] files = installDir.toFile().listFiles();
        assert files != null && files.length == 1 && files[0].isDirectory() : "Expected a single directory in " + installDir;
        Path cassandraHome = files[0].toPath();
        Path originalCassandraConfDir = cassandraHome.resolve("conf");
        copyDirectoryRecursively(originalCassandraConfDir, confDir);
        Path cassandraStorageDir = Files.createDirectories(tmpDir.resolve("var/lib/cassandra"));
        Path cassandraLogDir = Files.createDirectories(tmpDir.resolve("var/log"));

        ProcessRuntimeConfiguration.Builder builder = ProcessRuntimeConfiguration.builder()
                                          .instance(instanceMetadata())
                                          .cassandraHome(cassandraHome.toString())
                                          .cassandraConfDir(confDir.toString())
                                          .cassandraLogDir(cassandraLogDir.toString())
                                          .storageDir(cassandraStorageDir.toString());
        return new TestProcessRuntimeConfiguration(builder);
    }

    static InstanceMetadata instanceMetadata()
    {
        return InstanceMetadataImpl.builder()
                                   .id(TEST_NODE_ID)
                                   .metricRegistry(METRIC_REGISTRY)
                                   .storageDir("/tmp/storage_dir")
                                   .build();
    }
}
