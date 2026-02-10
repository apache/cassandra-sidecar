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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import org.apache.cassandra.sidecar.HelperTestModules.InstanceMetadataTestModule;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.response.InstanceFileInfo;
import org.apache.cassandra.sidecar.common.response.InstanceFileInfo.FileType;
import org.apache.cassandra.sidecar.common.response.InstanceFilesListResponse;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_FILES_ROUTE;
import static org.apache.cassandra.sidecar.livemigration.InstanceFileInfoTestUtil.findInstanceFileInfo;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.LIVE_MIGRATION_COMMITLOG_DIR_PATH;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.LIVE_MIGRATION_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.LIVE_MIGRATION_HINTS_DIR_PATH;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.LIVE_MIGRATION_SAVED_CACHES_DIR_PATH;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.createFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class LiveMigrationListInstanceFilesHandlerTest
{
    private static final int FIRST_ID = 1000110;
    private static final String FIRST_INSTANCE_IP = "127.0.0.1";
    private static final int SECOND_ID = 1000111;
    private static final String SECOND_INSTANCE_IP = "127.0.0.2";
    private static final int THIRD_ID = 1000112;
    private static final String THIRD_INSTANCE_IP = "127.0.0.3";
    private static final MetricRegistryFactory REGISTRY_FACTORY =
    new MetricRegistryFactory("cassandra_sidecar_" + UUID.randomUUID(), Collections.emptyList(), Collections.emptyList());
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationListInstanceFilesHandlerTest.class);

    private final Vertx vertx = Vertx.vertx();

    @TempDir
    Path tempDir;

    Injector injector;
    Server server;
    private List<String> dataDirs;
    private String commitlogDir;
    private String hintsDir;
    private String savedCachesDir;

    @BeforeEach
    public void setup() throws InterruptedException
    {
        InstanceMetadata sourceInstanceMeta = getInstanceMetadata(FIRST_INSTANCE_IP, FIRST_ID);
        InstanceMetadata destinationInstanceMeta = getInstanceMetadata(SECOND_INSTANCE_IP, SECOND_ID);
        InstanceMetadata nonSourceOrDestinationInstanceMeta = getInstanceMetadata(THIRD_INSTANCE_IP, THIRD_ID);
        ListInstanceFilesHandlerTestModule handlerTestModule = new ListInstanceFilesHandlerTestModule(
        Arrays.asList(sourceInstanceMeta, destinationInstanceMeta, nonSourceOrDestinationInstanceMeta));
        injector = Guice.createInjector(Modules.override(SidecarModules.all())
                                               .with(Modules.override(new TestModule())
                                                            .with(handlerTestModule)));
        InstancesMetadata instancesMetadata = injector.getInstance(InstancesMetadata.class);
        InstanceMetadata firstInstanceMetadata = instancesMetadata.instanceFromId(FIRST_ID);
        dataDirs = firstInstanceMetadata.dataDirs();
        commitlogDir = firstInstanceMetadata.commitlogDir();
        hintsDir = firstInstanceMetadata.hintsDir();
        savedCachesDir = firstInstanceMetadata.savedCachesDir();

        server = injector.getInstance(Server.class);
        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(15, TimeUnit.SECONDS);
    }


    @AfterEach
    void after() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    private InstanceMetadata getInstanceMetadata(String instanceIp,
                                                 int instanceId)
    {
        String root = tempDir.toString();
        dataDirs = Arrays.asList(root + "/d1/data", root + "/d2/data");
        MetricRegistry instanceSpecificRegistry = REGISTRY_FACTORY.getOrCreate(instanceId);

        return InstanceMetadataImpl.builder()
                                   .id(instanceId)
                                   .host(instanceIp)
                                   .port(9042)
                                   .storagePort(7000)
                                   .dataDirs(dataDirs)
                                   .hintsDir(root + "/hints")
                                   .commitlogDir(root + "/commitlog")
                                   .savedCachesDir(root + "/saved_caches")
                                   .stagingDir(root + "/staging")
                                   .metricRegistry(instanceSpecificRegistry)
                                   .build();
    }

    @Test
    public void testApiOnSource(VertxTestContext context) throws IOException
    {
        String dummyContent = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz";
        int fileSize = dummyContent.length();

        // First data directory files
        createFile(dummyContent, dataDirs.get(0), "ks1/table1/snapshots/snapshot1.db"); // This file doesn't qualify to be returned as it is a snapshot file

        // Second data directory files
        createFile(dummyContent, dataDirs.get(1), "ks1/table1/mf-kb-data.db");
        createFile(dummyContent, dataDirs.get(1), "ks2/table2/mf-kc-data.db");
        createFile(dummyContent, dataDirs.get(1), "ks2/table2/snapshots-magic.txt");
        createFile(dummyContent, dataDirs.get(1), "snapshots/table3/mf-kc-data.db");
        createFile(dummyContent, dataDirs.get(1), "heapdumps/table1/heapdumps-magic.txt");

        // Commitlog directory files
        createFile(dummyContent, commitlogDir, "Commitlog-7-1.log");
        createFile(dummyContent, commitlogDir, "Commitlog-7-2.log");

        // Hints directory files
        createFile(dummyContent, hintsDir, "abcd-1.hints");
        createFile(dummyContent, hintsDir, "def1-2.hints");

        // Saved caches directory files
        createFile(dummyContent, savedCachesDir, "cache1.db");
        createFile(dummyContent, savedCachesDir, "cache2.db");

        List<String> expectedFilesUrls = Arrays.asList(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/1" + "/ks1/table1/mf-kb-data.db",
                                                       LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/1" + "/ks2/table2/mf-kc-data.db",
                                                       LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/1" + "/ks2/table2/snapshots-magic.txt",
                                                       LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/1" + "/snapshots/table3/mf-kc-data.db",
                                                       LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/1" + "/heapdumps/table1/heapdumps-magic.txt",

                                                       LIVE_MIGRATION_COMMITLOG_DIR_PATH + "/0" + "/Commitlog-7-1.log",
                                                       LIVE_MIGRATION_COMMITLOG_DIR_PATH + "/0" + "/Commitlog-7-2.log",

                                                       LIVE_MIGRATION_HINTS_DIR_PATH + "/0" + "/abcd-1.hints",
                                                       LIVE_MIGRATION_HINTS_DIR_PATH + "/0" + "/def1-2.hints",

                                                       LIVE_MIGRATION_SAVED_CACHES_DIR_PATH + "/0" + "/cache1.db",
                                                       LIVE_MIGRATION_SAVED_CACHES_DIR_PATH + "/0" + "/cache2.db");

        LiveMigrationConfiguration liveMigrationConfig = injector.getInstance(SidecarConfiguration.class)
                                                                 .liveMigrationConfiguration();
        when(liveMigrationConfig.migrationMap()).thenReturn(Map.of(FIRST_INSTANCE_IP, SECOND_INSTANCE_IP));

        List<String> unexpectedFilesUrls = new ArrayList<>();
        unexpectedFilesUrls.add(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0" + "ks1/table1/snapshots/snapshot1.db");

        Set<String> unexpectedDirUrls = new HashSet<>();
        // Snapshots are excluded by default
        unexpectedDirUrls.add(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0" + "/ks1/table1/snapshots");
        unexpectedDirUrls.add(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/1" + "/ks1/table1/snapshots");

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_FILES_ROUTE)
              .as(BodyCodec.buffer())
              .send(resp -> context.verify(() -> {

                  assertThat(resp.result().statusCode()).isEqualTo(HttpResponseStatus.OK.code());

                  InstanceFilesListResponse instanceFilesList = resp.result().bodyAsJson(InstanceFilesListResponse.class);
                  assertThat(instanceFilesList).isNotNull();
                  assertThat(instanceFilesList.getTotalSize()).isEqualTo((long) expectedFilesUrls.size() * fileSize);

                  for (String expectedFileUrl : expectedFilesUrls)
                  {
                      InstanceFileInfo fileInfo = findInstanceFileInfo(instanceFilesList.getFiles(), expectedFileUrl);
                      assertThat(fileInfo).isNotNull();
                      assertThat(fileInfo.fileType).isEqualTo(FileType.FILE);
                      assertThat(fileInfo.size).isEqualTo(fileSize);
                  }

                  for (String unexpectedFileUrl : unexpectedFilesUrls)
                  {
                      InstanceFileInfo fileInfo = findInstanceFileInfo(instanceFilesList.getFiles(), unexpectedFileUrl);
                      assertThat(fileInfo).isNull();
                  }

                  for (String unexpectedDirUrl : unexpectedDirUrls)
                  {
                      InstanceFileInfo dirInfo = findInstanceFileInfo(instanceFilesList.getFiles(), unexpectedDirUrl);
                      assertThat(dirInfo).isNull();
                  }


                  List<InstanceFileInfo> fileInfos = instanceFilesList.getFiles()
                                                                      .stream()
                                                                      .filter(file -> file.fileType.equals(FileType.FILE))
                                                                      .collect(Collectors.toList());

                  assertThat(fileInfos).hasSize(expectedFilesUrls.size());

                  client.close();
                  context.completeNow();
              }));
    }

    @Test
    public void testApiOnDestination(VertxTestContext context) throws IOException
    {
        String dummyContent = "01234567890123456789012345678901";
        int filesSize = dummyContent.length();
        // First data directory files
        createFile(dummyContent, dataDirs.get(0), "ks1/table1/snapshots/snapshot1.db"); // This file doesn't qualify to be returned as it is a snapshot file

        createFile(dummyContent, dataDirs.get(1), "/ks1/table1/mf-kb-data.db");
        createFile(dummyContent, dataDirs.get(1), "/ks2/table2/mf-kc-data.db");
        createFile(dummyContent, dataDirs.get(1), "/ks2/table2/snapshots-magic.txt");

        List<String> expectedFileUrls = Arrays.asList(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/1" + "/ks1/table1/mf-kb-data.db",
                                                      LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/1" + "/ks2/table2/mf-kc-data.db",
                                                      LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/1" + "/ks2/table2/snapshots-magic.txt");

        List<String> unexpectedFileUrls = new ArrayList<>();
        unexpectedFileUrls.add(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0" + "/ks1/table1/snapshots/snapshot1.db"); // index of first data home directory

        Set<String> unexpectedDirUrls = new HashSet<>();
        unexpectedDirUrls.add(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0" + "/ks1/table1/snapshots");
        unexpectedDirUrls.add(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/1" + "/ks1/table1/snapshots");

        final Map<String, String> migrationMap = new HashMap<>()
        {{
            put(SECOND_INSTANCE_IP, FIRST_INSTANCE_IP);
        }};

        LiveMigrationConfiguration mockLiveMigrationConfig = injector.getInstance(SidecarConfiguration.class)
                                                                     .liveMigrationConfiguration();
        when(mockLiveMigrationConfig.migrationMap()).thenReturn(migrationMap);


        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_FILES_ROUTE)
              .as(BodyCodec.buffer())
              .send(resp -> context.verify(() -> {
                  assertThat(resp.result().statusCode()).isEqualTo(HttpResponseStatus.OK.code());

                  InstanceFilesListResponse instanceFilesList = resp.result().bodyAsJson(InstanceFilesListResponse.class);
                  assertThat(instanceFilesList).isNotNull();
                  assertThat(instanceFilesList.getTotalSize()).isEqualTo((long) expectedFileUrls.size() * filesSize);

                  for (String expectedFileUrl : expectedFileUrls)
                  {
                      InstanceFileInfo fileInfo = findInstanceFileInfo(instanceFilesList.getFiles(), expectedFileUrl);
                      assertThat(fileInfo).isNotNull();
                      assertThat(fileInfo.fileType).isEqualTo(FileType.FILE);
                      assertThat(fileInfo.size).isEqualTo(filesSize);
                  }

                  for (String unexpectedFileUrl : unexpectedFileUrls)
                  {
                      InstanceFileInfo fileInfo = findInstanceFileInfo(instanceFilesList.getFiles(), unexpectedFileUrl);
                      assertThat(fileInfo).isNull();
                  }

                  for (String unexpectedDirUrl : unexpectedDirUrls)
                  {
                      InstanceFileInfo dirInfo = findInstanceFileInfo(instanceFilesList.getFiles(), unexpectedDirUrl);
                      assertThat(dirInfo).isNull();
                  }

                  List<InstanceFileInfo> fileInfos = instanceFilesList.getFiles()
                                                                      .stream()
                                                                      .filter(file -> file.fileType.equals(FileType.FILE))
                                                                      .collect(Collectors.toList());

                  assertThat(fileInfos).hasSize(expectedFileUrls.size());

                  client.close();
                  context.completeNow();
              }));
    }

    @Test
    public void testApiOnNonSourceOrDestination(VertxTestContext context)
    {
        final Map<String, String> migrationMap = new HashMap<>()
        {{
            put(SECOND_INSTANCE_IP, THIRD_INSTANCE_IP);
        }};

        LiveMigrationConfiguration mockLiveMigrationConfiguration = injector.getInstance(SidecarConfiguration.class)
                                                                            .liveMigrationConfiguration();
        when(mockLiveMigrationConfiguration.migrationMap()).thenReturn(migrationMap);

        WebClient client = WebClient.create(vertx);

        client.get(server.actualPort(), "127.0.0.1", LIVE_MIGRATION_FILES_ROUTE)
              .as(BodyCodec.buffer())
              .send(resp -> {
                  assertThat(resp.result().statusCode()).isEqualTo(HttpResponseStatus.NOT_FOUND.code());
                  assertThat(resp.result().statusMessage()).isEqualTo("Not Found");

                  client.close();
                  context.completeNow();
              });
    }

    private static class ListInstanceFilesHandlerTestModule extends AbstractModule
    {

        private final List<InstanceMetadata> instanceMetaList;

        public ListInstanceFilesHandlerTestModule(List<InstanceMetadata> instanceMetaList)
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
            install(new InstanceMetadataTestModule(instanceMetaList));
        }
    }
}
