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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.HelperTestModules;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.LIVE_MIGRATION_CDC_RAW_DIR_PATH;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.LIVE_MIGRATION_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.createFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class LiveMigrationFileStreamHandlerTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationFileStreamHandlerTest.class);

    private static final int FIRST_ID = 1000110;
    private static final String FIRST_INSTANCE_IP = "127.0.0.1";
    private static final int SECOND_ID = 1000111;
    private static final String SECOND_INSTANCE_IP = "127.0.0.2";
    private static final int THIRD_ID = 1000112;
    private static final String THIRD_INSTANCE_IP = "127.0.0.3";
    private static final String DUMMY_CONTENT = "data";

    private static final MetricRegistryFactory REGISTRY_FACTORY =
    new MetricRegistryFactory("cassandra_sidecar_" + UUID.randomUUID(), Collections.emptyList(), Collections.emptyList());
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
        InstanceMetadata firstInstanceMeta = getInstanceMetadata(FIRST_INSTANCE_IP, FIRST_ID);
        firstInstanceDataDirs = firstInstanceMeta.dataDirs();
        InstanceMetadata secondInstanceMeta = getInstanceMetadata(SECOND_INSTANCE_IP, SECOND_ID);
        secondInstanceDataDirs = secondInstanceMeta.dataDirs();
        InstanceMetadata thirdInstanceMeta = getInstanceMetadata(THIRD_INSTANCE_IP, THIRD_ID);
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

    private InstanceMetadata getInstanceMetadata(String instanceIp,
                                                 int instanceId)
    {
        String root = tempDir.resolve(String.valueOf(instanceId)).toString();
        List<String> dataDirs = Arrays.asList(root + "/d1/data", root + "/d2/data");
        MetricRegistry instanceSpecificRegistry = REGISTRY_FACTORY.getOrCreate(instanceId);

        return InstanceMetadataImpl.builder()
                                   .id(instanceId)
                                   .host(instanceIp)
                                   .port(9042)
                                   .dataDirs(dataDirs)
                                   .hintsDir(root + "/hints")
                                   .commitlogDir(root + "/commitlog")
                                   .savedCachesDir(root + "/saved_caches")
                                   .stagingDir(root + "/staging")
                                   .metricRegistry(instanceSpecificRegistry)
                                   .build();
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

        shouldThrowError(context, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/data/0/..",
                         FIRST_INSTANCE_IP, THIRD_INSTANCE_IP, FIRST_INSTANCE_IP, 400);
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
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        createFile(DUMMY_CONTENT, firstInstanceDataDirs.get(0) + filePath);

        // Vertx is stopping routes having three "%2E%2E" in the path and returning 404
        shouldThrowError(context, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/%2E%2E/%2E%2E/%2E%2E/secrets",
                         FIRST_INSTANCE_IP, THIRD_INSTANCE_IP, FIRST_INSTANCE_IP, 404);
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

        // This route has 100 as dataHomeDir index which is greater than instance data home dir count. It should fail.
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

            LiveMigrationConfiguration mockLiveMigrationConfiguration = mock(LiveMigrationConfiguration.class);
            when(mockLiveMigrationConfiguration.filesToExclude())
            .thenReturn(Collections.emptySet());
            when(mockLiveMigrationConfiguration.directoriesToExclude())
            .thenReturn(Collections.singleton("glob:${DATA_FILE_DIR}/*/*/snapshots"));
            when(mockLiveMigrationConfiguration.migrationMap())
            .thenReturn(Collections.emptyMap());

            SidecarConfiguration sidecarConfiguration = SidecarConfigurationImpl.builder()
                                                                                .liveMigrationConfiguration(mockLiveMigrationConfiguration)
                                                                                .build();

            bind(SidecarConfiguration.class).toInstance(sidecarConfiguration);
            install(new HelperTestModules.InstanceMetadataTestModule(instanceMetaList));
        }
    }
}
