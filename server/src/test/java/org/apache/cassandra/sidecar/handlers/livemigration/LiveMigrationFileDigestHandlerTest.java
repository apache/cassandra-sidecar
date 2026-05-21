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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.HelperTestModules.InstanceMetadataTestModule;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.LiveMigrationConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.utils.DigestAlgorithm;
import org.apache.cassandra.sidecar.utils.XXHash32Provider;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_FILES_ROUTE;
import static org.apache.cassandra.sidecar.handlers.livemigration.InstanceMetadataTestUtil.getInstanceMetadata;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.createFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for the LiveMigrationFileDigestHandler that validates file digest computation across different directory types and digest algorithms.
 */
@SuppressWarnings("SameParameterValue")
@ExtendWith(VertxExtension.class)
public class LiveMigrationFileDigestHandlerTest
{
    @SuppressWarnings("SpellCheckingInspection")
    public static final String CHARACTER_SET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    public static final Random RANDOM = new Random();
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationFileDigestHandlerTest.class);
    private static final int FIRST_ID = 1000110;
    private static final String FIRST_INSTANCE_IP = "127.0.0.1";
    private static final int SECOND_ID = 1000111;
    private static final String SECOND_INSTANCE_IP = "127.0.0.2";
    private static final int THIRD_ID = 1000112;
    private static final String THIRD_INSTANCE_IP = "127.0.0.3";
    private static final int MAX_CONCURRENT_FILE_REQUESTS = 2;
    private final Vertx vertx = Vertx.vertx();
    @TempDir
    Path tempDir;
    InstanceMetadata firstInstanceMeta;
    List<String> firstInstanceDataDirs;
    List<String> secondInstanceDataDirs;
    List<String> thirdInstanceDataDirs;
    MessageDigest md5;
    private Server server;
    private Injector injector;

    public LiveMigrationFileDigestHandlerTest() throws NoSuchAlgorithmException
    {
        md5 = MessageDigest.getInstance("MD5");
    }

    @BeforeEach
    void setup() throws InterruptedException
    {
        firstInstanceMeta = getInstanceMetadata(FIRST_INSTANCE_IP, FIRST_ID, tempDir);
        firstInstanceDataDirs = firstInstanceMeta.dataDirs();
        InstanceMetadata secondInstanceMeta = getInstanceMetadata(SECOND_INSTANCE_IP, SECOND_ID, tempDir);
        secondInstanceDataDirs = secondInstanceMeta.dataDirs();
        InstanceMetadata thirdInstanceMeta = getInstanceMetadata(THIRD_INSTANCE_IP, THIRD_ID, tempDir);
        thirdInstanceDataDirs = thirdInstanceMeta.dataDirs();

        LiveMigrationFileDigestHandlerTestModule handlerTestModule = new LiveMigrationFileDigestHandlerTestModule(
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
    void after() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    @Test
    public void testRouteSucceedsForFirstDataDirForMd5DigestAlgo(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        String dummyText = getDummyData(RANDOM.nextInt(128));
        createFile(dummyText, firstInstanceDataDirs.get(0) + filePath);

        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/data/0" + filePath + "?digestAlgorithm=md5";
        shouldSucceed(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, md5Sum(dummyText));
    }

    @Test
    public void testRouteSucceedsForSecondDataDir(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        String dummyText = getDummyData(RANDOM.nextInt(128));
        createFile(dummyText, firstInstanceDataDirs.get(1) + filePath);

        // This time using XXHash32 as the digest algorithm
        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/data/1" + filePath + "?digestAlgorithm=xxhash32";
        shouldSucceed(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, xxhash32(dummyText));
    }

    @Test
    public void testRouteSucceedsForCommitLogDir(VertxTestContext context) throws IOException
    {
        String filePath = "/commit-1.db";
        String dummyText = getDummyData(RANDOM.nextInt(128));
        createFile(dummyText, firstInstanceMeta.commitlogDir() + filePath);

        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/commitlog/0" + filePath + "?digestAlgorithm=xxhash32";
        shouldSucceed(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, xxhash32(dummyText));
    }

    @Test
    public void testRouteFailsForInvalidDir(VertxTestContext context) throws IOException
    {
        String filePath = "/commit-1.db";
        String dummyText = getDummyData(RANDOM.nextInt(128));
        createFile(dummyText, firstInstanceMeta.commitlogDir() + filePath);

        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/commitlog/1" + filePath + "?digestAlgorithm=xxhash32";
        shouldFail(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, 404);
    }

    @Test
    public void testRouteSucceedsForHintsDir(VertxTestContext context) throws IOException
    {
        String filePath = "/hints-1.db";
        String dummyText = getDummyData(RANDOM.nextInt(128));
        createFile(dummyText, firstInstanceMeta.hintsDir() + filePath);

        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/hints/0" + filePath + "?digestAlgorithm=xxhash32";
        shouldSucceed(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, xxhash32(dummyText));
    }

    @Test
    public void testRouteSucceedsForSavedCachesDir(VertxTestContext context) throws IOException
    {
        String filePath = "/cache-101029383.txt";
        String dummyText = getDummyData(RANDOM.nextInt(128));
        createFile(dummyText, firstInstanceMeta.savedCachesDir() + filePath);

        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/saved_caches/0" + filePath
                           + "?digestAlgorithm=md5";
        shouldSucceed(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, md5Sum(dummyText));
    }

    @Test
    public void testRouteSucceedsForCdcDir(VertxTestContext context) throws IOException
    {
        String filePath = "/Commitlog-8.db";
        String dummyText = getDummyData(RANDOM.nextInt(128));
        createFile(dummyText, firstInstanceMeta.cdcDir() + filePath);

        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/cdc_raw/0" + filePath
                           + "?digestAlgorithm=md5";
        shouldSucceed(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, md5Sum(dummyText));
    }

    @Test
    public void testRouteFailsForDestination(VertxTestContext context) throws IOException
    {
        String filePath = "/Commitlog-8.db";
        String dummyText = getDummyData(RANDOM.nextInt(128));
        createFile(dummyText, firstInstanceMeta.cdcDir() + filePath);

        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/cdc_raw/0" + filePath
                           + "?digestAlgorithm=md5";
        shouldFail(context, testRoute, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, FIRST_INSTANCE_IP, 404);
    }

    @Test
    public void testRouteFailsForInstanceNotRelatedToLiveMigration(VertxTestContext context) throws IOException
    {
        String filePath = "/Commitlog-8.db";
        String dummyText = getDummyData(RANDOM.nextInt(128));
        createFile(dummyText, firstInstanceMeta.cdcDir() + filePath);

        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/cdc_raw/0" + filePath
                           + "?digestAlgorithm=md5";
        shouldFail(context, testRoute, SECOND_INSTANCE_IP, THIRD_INSTANCE_IP, FIRST_INSTANCE_IP, 404);
    }

    @Test
    public void testRequestWithEmptyDigestAlgoShouldFail(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        String dummyText = getDummyData(RANDOM.nextInt(128));
        createFile(dummyText, firstInstanceDataDirs.get(0) + filePath);

        // Digest algo is just a white space
        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/data/0" + filePath + "?digestAlgorithm= \t\n";
        shouldFail(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, 400);
    }

    @Test
    public void testRequestWithUnSupportedDigestAlgoShouldFail(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        String dummyText = getDummyData(RANDOM.nextInt(128));
        createFile(dummyText, firstInstanceDataDirs.get(0) + filePath);

        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/data/0" + filePath + "?digestAlgorithm=UNKnown$";
        shouldFail(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, 400);
    }

    @Test
    public void testRouteFailsForNonExistentFile(VertxTestContext context)
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-NonExistent.db";
        // Don't create the file - it should not exist

        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/data/0" + filePath + "?digestAlgorithm=md5";
        shouldFail(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, 404);
    }

    @Test
    public void testRouteSucceedsForEmptyFile(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Empty.db";
        createFile("", firstInstanceDataDirs.get(0) + filePath);

        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/data/0" + filePath + "?digestAlgorithm=md5";
        shouldSucceed(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, md5Sum(""));
    }

    @Test
    public void testRouteSucceedsForEmptyFileWithXxhash32(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Empty.db";
        createFile("", firstInstanceDataDirs.get(0) + filePath);

        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/data/0" + filePath + "?digestAlgorithm=xxhash32";
        shouldSucceed(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, xxhash32(""));
    }

    @Test
    public void testRequestWithUppercaseDigestAlgorithm(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        String dummyText = getDummyData(RANDOM.nextInt(1024 * 1024));
        createFile(dummyText, firstInstanceDataDirs.get(0) + filePath);

        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/data/0" + filePath + "?digestAlgorithm=MD5";
        shouldSucceed(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, md5Sum(dummyText));
    }

    @Test
    public void testRequestWithMixedCaseDigestAlgorithm(VertxTestContext context) throws IOException
    {
        String filePath = "/ks/tb-1234/ks-tb-1234-Data.db";
        String dummyText = getDummyData(RANDOM.nextInt(1024 * 1024));
        createFile(dummyText, firstInstanceDataDirs.get(0) + filePath);

        String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/data/0" + filePath + "?digestAlgorithm=XxHash32";
        shouldSucceed(context, testRoute, FIRST_INSTANCE_IP, SECOND_INSTANCE_IP, FIRST_INSTANCE_IP, xxhash32(dummyText));
    }

    @Test
    public void testConcurrencyLimitRejectsExcessRequests(VertxTestContext context) throws IOException
    {
        // Configuration has MAX_CONCURRENT_FILE_REQUESTS=2, send 3 requests rapidly
        // At least one should be rejected
        String dummyText = getDummyData(1024 * 1024); // 1MB file for slower processing
        List<String> filePaths = Arrays.asList(
            "/ks/tb-1234/ks-tb-1234-Concurrent1.db",
            "/ks/tb-1234/ks-tb-1234-Concurrent2.db",
            "/ks/tb-1234/ks-tb-1234-Concurrent3.db"
        );

        // Create all test files
        for (String filePath : filePaths)
        {
            createFile(dummyText, firstInstanceDataDirs.get(0) + filePath);
        }

        mockLiveMigrationMap(FIRST_INSTANCE_IP, SECOND_INSTANCE_IP);

        WebClient client = WebClient.create(vertx);
        AtomicInteger okCount = new AtomicInteger(0);
        AtomicInteger tooManyRequestsCount = new AtomicInteger(0);
        AtomicInteger completedCount = new AtomicInteger(0);

        // Send 3 requests rapidly - with limit of 2, at least one should be rejected
        for (String filePath : filePaths)
        {
            String testRoute = LIVE_MIGRATION_FILES_ROUTE + "/data/0" + filePath + "?digestAlgorithm=md5";
            client.get(server.actualPort(), FIRST_INSTANCE_IP, testRoute)
                  .as(BodyCodec.buffer())
                  .send(response -> {
                      if (response.succeeded())
                      {
                          int statusCode = response.result().statusCode();
                          if (statusCode == HttpResponseStatus.OK.code())
                          {
                              okCount.incrementAndGet();
                          }
                          else if (statusCode == HttpResponseStatus.SERVICE_UNAVAILABLE.code())
                          {
                              tooManyRequestsCount.incrementAndGet();
                          }

                          if (completedCount.incrementAndGet() == filePaths.size())
                          {
                              // All requests completed, verify results
                              context.verify(() -> {
                                  // At least one request should be rejected due to concurrency limit
                                  assertThat(tooManyRequestsCount.get()).isGreaterThan(0);
                                  // Total should equal number of requests
                                  assertThat(okCount.get() + tooManyRequestsCount.get()).isEqualTo(filePaths.size());
                                  client.close();
                                  context.completeNow();
                              });
                          }
                      }
                      else
                      {
                          context.failNow(response.cause());
                      }
                  });
        }
    }

    void shouldFail(VertxTestContext context,
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

    void shouldSucceed(VertxTestContext context,
                       String testRoute,
                       String source,
                       String destination,
                       String requestHost,
                       String expectedDigest)
    {
        mockLiveMigrationMap(source, destination);

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), requestHost, testRoute)
              .as(BodyCodec.buffer())
              .send(context.succeeding(resp -> context.verify(() -> {
                  assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());

                  JsonObject jsonObject = resp.bodyAsJsonObject();
                  assertThat(jsonObject).isNotNull();
                  assertThat(jsonObject.getString("digest")).isEqualTo(expectedDigest);

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

    String getDummyData(int size)
    {
        StringBuilder randomString = new StringBuilder();

        for (int i = 0; i < size; i++)
        {
            int index = RANDOM.nextInt(CHARACTER_SET.length());
            randomString.append(CHARACTER_SET.charAt(index));
        }
        return randomString.toString();
    }

    String md5Sum(String data)
    {
        return Base64.getEncoder()
                     .encodeToString(md5.digest(data.getBytes(StandardCharsets.UTF_8)));
    }

    String xxhash32(String data)
    {
        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);
        DigestAlgorithm digestAlgorithm = new XXHash32Provider().get(0);
        digestAlgorithm.update(bytes, 0, bytes.length);
        return digestAlgorithm.digest();
    }

    private static class LiveMigrationFileDigestHandlerTestModule extends AbstractModule
    {
        private final List<InstanceMetadata> instanceMetaList;

        public LiveMigrationFileDigestHandlerTestModule(List<InstanceMetadata> instanceMetaList)
        {
            this.instanceMetaList = instanceMetaList;
        }

        @Override
        protected void configure()
        {
            LiveMigrationConfiguration liveMigrationConfigurationSpy = spy(new LiveMigrationConfigurationImpl(
            Set.of(), Set.of("glob:${DATA_FILE_DIR}/*/*/snapshots"), Map.of(), MAX_CONCURRENT_FILE_REQUESTS));

            SidecarConfiguration sidecarConfiguration =
            SidecarConfigurationImpl.builder()
                                    .liveMigrationConfiguration(liveMigrationConfigurationSpy)
                                    .build();

            bind(SidecarConfiguration.class).toInstance(sidecarConfiguration);
            install(new InstanceMetadataTestModule(instanceMetaList));
        }
    }
}
