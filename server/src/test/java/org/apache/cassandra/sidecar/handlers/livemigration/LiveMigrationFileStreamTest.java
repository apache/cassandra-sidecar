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

package org.apache.cassandra.sidecar.handlers.livemigration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.HelperTestModules;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.LiveMigrationConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationStatusTracker;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static org.apache.cassandra.sidecar.handlers.livemigration.InstanceMetadataTestUtil.LIVE_MIGRATION_CDC_RAW_DIR_PATH;
import static org.apache.cassandra.sidecar.handlers.livemigration.InstanceMetadataTestUtil.LIVE_MIGRATION_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.handlers.livemigration.InstanceMetadataTestUtil.getInstanceMetadata;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.createFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class LiveMigrationFileStreamTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationFileStreamTest.class);

    private static final int FIRST_ID = 1000110;
    private static final String FIRST_INSTANCE_IP = "127.0.0.1";
    private static final int SECOND_ID = 1000111;
    private static final String SECOND_INSTANCE_IP = "127.0.0.2";
    private static final int THIRD_ID = 1000112;
    private static final String THIRD_INSTANCE_IP = "127.0.0.3";
    private static final String DUMMY_CONTENT = "data";

    private final Vertx vertx = Vertx.vertx();
    @TempDir
    Path tempDir;
    Server server;
    List<String> firstInstanceDataDirs;
    List<String> secondInstanceDataDirs;
    List<String> thirdInstanceDataDirs;
    private Injector injector;

    @BeforeEach
    public void setup() throws InterruptedException
    {
        InstanceMetadata firstInstanceMeta = getInstanceMetadata(FIRST_INSTANCE_IP, FIRST_ID, tempDir);
        firstInstanceDataDirs = firstInstanceMeta.dataDirs();
        InstanceMetadata secondInstanceMeta = getInstanceMetadata(SECOND_INSTANCE_IP, SECOND_ID, tempDir);
        secondInstanceDataDirs = secondInstanceMeta.dataDirs();
        InstanceMetadata thirdInstanceMeta = getInstanceMetadata(THIRD_INSTANCE_IP, THIRD_ID, tempDir);
        thirdInstanceDataDirs = thirdInstanceMeta.dataDirs();
        FileStreamHandlerTestModule handlerTestModule = new FileStreamHandlerTestModule(
        Arrays.asList(firstInstanceMeta, secondInstanceMeta, thirdInstanceMeta));
        injector = Guice.createInjector(Modules.override(SidecarModules.all())
                                               .with(Modules.override(new TestModule())
                                                            .with(handlerTestModule)));

        server = injector.getInstance(Server.class);
        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(15, TimeUnit.SECONDS);
    }

    @AfterEach
    public void tearDown() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    @Test
    public void testRouteSucceeds(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        String testRoute = LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0" + filePath;
        shouldSucceed(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP);
    }

    @Test
    public void testDownloadFailsWhenLiveMigrationMarkedAsCompleted(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        LiveMigrationStatusTracker statusTracker = injector.getInstance(LiveMigrationStatusTracker.class);
        when(statusTracker.hasMigrationCompleted(any(InstanceMetadata.class)))
        .thenReturn(Future.succeededFuture(true));


        String testRoute = LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0" + filePath;
        shouldThrowError(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, 400);
    }

    @Test
    public void testRouteUnConfiguredDirFile(VertxTestContext context)
    {
        // CDC dir not configured while constructing dummy InstanceMetadata in getInstanceMetadata()
        // Yet requesting a dummy file to see if the endpoint returns proper error

        String testRoute = LIVE_MIGRATION_CDC_RAW_DIR_PATH + "/0/Commitlog-7-1.db";
        shouldThrowError(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, 404);
    }

    @Test
    public void testRouteDoubleDotAtTheEndAfterDirIndex(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        String url = LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/..";
        shouldThrowError(context, url, // This url itself will not be identified by Vertx, 404 is expected
                         FIRST_INSTANCE_IP, THIRD_INSTANCE_IP, FIRST_INSTANCE_IP, 404);
    }

    @Test
    public void testRouteDoubleDotAtTheEnd(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        shouldThrowError(context, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks/tb-1234/ks-tb-1234-Data.db/..",
                         FIRST_INSTANCE_IP, THIRD_INSTANCE_IP, FIRST_INSTANCE_IP, 400);
    }

    @Test
    public void testRouteDoubleDotAfterDirIndex(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        shouldThrowError(context, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/../ks/tb-1234/ks-tb-1234-Data.db",
                         FIRST_INSTANCE_IP, THIRD_INSTANCE_IP, FIRST_INSTANCE_IP, 400);
    }

    @Test
    public void testRequestUsingEncodedDots(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        // Using escape character of '.' i.e. %2E
        shouldThrowError(context, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks/tb-1234/%2E%2E/%2E%2E/secrets",
                         FIRST_INSTANCE_IP, THIRD_INSTANCE_IP, FIRST_INSTANCE_IP, 400);
    }


    @Test
    public void testRequestInvalidPathUsingEncodedDots(VertxTestContext context) throws IOException
    {
        // If %2E%2E were wrongly decoded and the path were collapsed unchecked, three "../"
        // segments from <tempDir>/<id>/d1/data would walk up to <tempDir>/, where this decoy
        // lives. Plant it so the test actually proves the request can't reach it. Vert.x rejects
        // 3+ consecutive encoded ".." patterns, so the request never reaches the resolver.
        createFile("decoy", tempDir.resolve("secrets").toString());

        shouldThrowError(context, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/%2E%2E/%2E%2E/%2E%2E/secrets",
                         FIRST_INSTANCE_IP, THIRD_INSTANCE_IP, FIRST_INSTANCE_IP, 404);
    }

    @Test
    public void testRequestUsingDoubleEncodedDots(VertxTestContext context) throws IOException
    {
        // Double-encoded traversal: "%252e%252e%2f" decodes only once in Vert.x's normalizer to "%2e%2e/"
        // ("%25" is a reserved char that is never decoded), so "%252e%252e" stays a literal path segment
        // and is never collapsed into "..". Plant a secret two levels above the data dir
        // (data/0 -> <tempDir>/<id>/d1/data, so "../../secret.txt" would resolve to <tempDir>/<id>/secret.txt)
        // and confirm the double-encoded request cannot traverse out to reach it.
        createFile("super-secret", tempDir.resolve(String.valueOf(FIRST_ID)).resolve("secret.txt").toString());
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + "/ks/tb-1234/ks-tb-1234-Data.db");

        String testRoute = LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/%252e%252e/%252e%252e/secret.txt";
        shouldThrowError(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, 404);
    }

    @Test
    public void testRouteFailsForSymlinkEscapingDataDir(VertxTestContext context) throws IOException
    {
        // A symlink planted inside the data dir that points outside the migration tree cannot be caught by
        // the lexical ".." containment check. realPath() resolves symlinks via toRealPath() and rejects
        // the escape with 403, so the file behind the symlink is not served.
        Path outside = tempDir.resolve("outside-tree");
        Files.createDirectories(outside);
        createFile("top-secret", outside.resolve("secret.txt").toString());

        Path dataDir = Paths.get(firstInstanceDataDirs.get(0));
        Files.createDirectories(dataDir);
        Files.createSymbolicLink(dataDir.resolve("escape"), outside);

        String testRoute = LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/escape/secret.txt";
        shouldThrowError(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, 403);
    }

    @Test
    public void testRouteSucceedsForSymlinkWithinDataDir(VertxTestContext context) throws IOException
    {
        // A symlink that resolves to a target still inside the data dir is legitimate and must be served -
        // the real-path containment check must not over-block symlinks within the migration tree.
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        Path dataDir = Paths.get(firstInstanceDataDirs.get(0));
        Files.createSymbolicLink(dataDir.resolve("link-ks"), dataDir.resolve("ks"));

        String testRoute = LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/link-ks/tb-1234/ks-tb-1234-Data.db";
        shouldSucceed(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP);
    }


    @Test
    public void testRequestDirectory(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        // Requesting directory, it should not succeed.
        shouldThrowError(context, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks/tb-1234",
                         FIRST_INSTANCE_IP, THIRD_INSTANCE_IP, FIRST_INSTANCE_IP, 400);
    }

    @Test
    public void testRouteDirtyRouteThatDoesNotMatchPath(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        shouldThrowError(context, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/../../.." + filePath,
                         FIRST_INSTANCE_IP, THIRD_INSTANCE_IP, FIRST_INSTANCE_IP, 404);
    }

    @Test
    public void testRouteWithoutDirIndex(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        // This route doesn't have data file dir index init. It should fail.
        shouldThrowError(context, LIVE_MIGRATION_DATA_FILE_DIR_PATH + filePath,
                         FIRST_INSTANCE_IP, THIRD_INSTANCE_IP, FIRST_INSTANCE_IP, 400);
    }

    @Test
    public void testRouteHavingNegativeDirIndex(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        // This route has negative dataHomeDir which is invalid. It should fail.
        shouldThrowError(context, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/-1" + filePath,
                         FIRST_INSTANCE_IP, THIRD_INSTANCE_IP, FIRST_INSTANCE_IP, 400);
    }

    @Test
    public void testRouteHavingDirIndexThatDoesNotExist(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        // dirIndex 100 is greater than the configured data dir count, so the URL doesn't match
        // any configured prefix - reports it as a missing resource (404), since
        // the URL is well-formed but addresses no directory configured on this instance.
        shouldThrowError(context, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/100" + filePath,
                         FIRST_INSTANCE_IP, THIRD_INSTANCE_IP, FIRST_INSTANCE_IP, 404);
    }

    @Test
    public void testRouteFailsForDestinationInstance(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        String testRoute = LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0" + filePath;
        shouldThrowError(context, testRoute, FIRST_INSTANCE_IP, THIRD_INSTANCE_IP, THIRD_INSTANCE_IP, 404);
    }

    @Test
    public void testRouteFailsForNonLiveMigratingInstance(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, secondInstanceDataDirs.get(0) + filePath);

        String testRoute = LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0" + filePath;
        // In this test second instance is neither source nor destination
        shouldThrowError(context, testRoute, "127.0.0.4", "127.0.0.5", SECOND_INSTANCE_IP, 404);
    }


    @Test
    public void testRouteFailsExcludedDataDirFile(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/test.txt";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        String testRoute = LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0" + filePath;
        mockFileExclusion(Collections.singleton("glob:${DATA_FILE_DIR}" + filePath));

        shouldThrowError(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, 404);
    }

    @Test
    public void testRouteFailsExcludedFileSecondDataDirWildcardExclusion(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/test.txt";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(1) + filePath);

        String testRoute = LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/1" + filePath;
        mockFileExclusion(Collections.singleton("glob:${DATA_FILE_DIR_1}/ks/**"));

        shouldThrowError(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, 404);
    }


    @Test
    public void testRouteFailsWhenParentDirExcluded(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/test.txt";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        String testRoute = LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0" + filePath;
        mockDirExclusion(Collections.singleton("glob:${DATA_FILE_DIR}/ks"));

        shouldThrowError(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, 404);
    }

    @Test
    public void testRouteSucceedsWhenOtherDirExcluded(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/test.txt";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        String testRoute = LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0" + filePath;
        mockDirExclusion(Collections.singleton("glob:${DATA_FILE_DIR}/ks2"));

        shouldSucceed(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP);
    }


    @SuppressWarnings("SameParameterValue")
    void shouldSucceed(VertxTestContext context,
                       String testRoute,
                       String source,
                       String destination,
                       String requestHost)
    {
        mockLiveMigrationMap(source, destination);

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), requestHost, testRoute)
              .as(BodyCodec.buffer())
              .send(context.succeeding(resp -> context.verify(() -> {
                  assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  assertThat(resp.bodyAsString()).isEqualTo(DUMMY_CONTENT);
                  client.close();
                  context.completeNow();
              })));
    }

    void shouldThrowError(VertxTestContext context,
                          String testRoute,
                          String source,
                          String destination,
                          String requestHost,
                          int expectedStatusCode)
    {
        mockLiveMigrationMap(source, destination);

        WebClient client = WebClient.create(vertx);
        String url = String.format("http://%s:%d%s", requestHost, server.actualPort(), testRoute);
        client.getAbs(url)
              .as(BodyCodec.buffer())
              .send(context.succeeding(resp -> context.verify(() -> {
                  assertThat(resp.statusCode()).isEqualTo(expectedStatusCode);
                  client.close();
                  context.completeNow();
              })));
    }

    void mockLiveMigrationMap(String source, String destination)
    {
        LiveMigrationConfiguration liveMigrationConfig = injector.getInstance(SidecarConfiguration.class)
                                                                 .liveMigrationConfiguration();
        when(liveMigrationConfig.migrationMap()).thenReturn(Map.of(source, destination));
    }

    void mockFileExclusion(Set<String> fileExclusions)
    {
        LiveMigrationConfiguration configuration = injector.getInstance(SidecarConfiguration.class)
                                                           .liveMigrationConfiguration();
        when(configuration.filesToExclude()).thenReturn(fileExclusions);
    }

    void mockDirExclusion(Set<String> dirExclusions)
    {
        LiveMigrationConfiguration configuration = injector.getInstance(SidecarConfiguration.class)
                                                           .liveMigrationConfiguration();
        when(configuration.directoriesToExclude()).thenReturn(dirExclusions);
    }


    private static class FileStreamHandlerTestModule extends AbstractModule
    {

        private final List<InstanceMetadata> instanceMetaList;

        public FileStreamHandlerTestModule(List<InstanceMetadata> instanceMetaList)
        {
            this.instanceMetaList = instanceMetaList;
        }

        @Override
        protected void configure()
        {
            LiveMigrationConfiguration liveMigrationConfigurationSpy = spy(new LiveMigrationConfigurationImpl(
            Set.of(), Set.of("glob:${DATA_FILE_DIR}/*/*/snapshots"), Map.of(), 10));

            SidecarConfiguration sidecarConfiguration =
            SidecarConfigurationImpl.builder()
                                    .liveMigrationConfiguration(liveMigrationConfigurationSpy)
                                    .build();

            bind(SidecarConfiguration.class).toInstance(sidecarConfiguration);
            install(new HelperTestModules.InstanceMetadataTestModule(instanceMetaList));

            LiveMigrationStatusTracker statusTracker = mock(LiveMigrationStatusTracker.class);
            when(statusTracker.hasMigrationCompleted(any(InstanceMetadata.class)))
            .thenReturn(Future.succeededFuture(false));
            bind(LiveMigrationStatusTracker.class).toInstance(statusTracker);
        }
    }
}
