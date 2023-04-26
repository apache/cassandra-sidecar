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

package org.apache.cassandra.sidecar.routes.sstableuploads;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.cassandra.sidecar.MainModule;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;
import org.apache.cassandra.sidecar.snapshots.SnapshotUtils;

import static org.apache.cassandra.sidecar.snapshots.SnapshotUtils.mockInstancesConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Common functionality for the SSTable Uploads {@link SSTableUploadHandler}, {@link SSTableImportHandler},
 * and {@link SSTableCleanupHandler} tests.
 */
class BaseUploadsHandlerTest
{
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected Vertx vertx;
    protected HttpServer server;
    protected WebClient client;
    protected Configuration config;
    protected CassandraAdapterDelegate mockDelegate;
    protected Configuration mockConfiguration;
    @TempDir
    protected File temporaryFolder;

    @BeforeEach
    void setup() throws InterruptedException
    {
        mockDelegate = mock(CassandraAdapterDelegate.class);
        mockConfiguration = mock(Configuration.class);
        TestModuleOverride testModuleOverride = new TestModuleOverride(mockDelegate, mockConfiguration);
        Injector injector = Guice.createInjector(Modules.override(new MainModule())
                                                        .with(Modules.override(new TestModule())
                                                                     .with(testModuleOverride)));
        server = injector.getInstance(HttpServer.class);
        vertx = injector.getInstance(Vertx.class);
        config = injector.getInstance(Configuration.class);
        client = WebClient.create(vertx);

        VertxTestContext context = new VertxTestContext();
        server.listen(config.getPort(), config.getHost(), context.succeedingThenComplete());

        Metadata mockMetadata = mock(Metadata.class);
        KeyspaceMetadata mockKeyspaceMetadata = mock(KeyspaceMetadata.class);
        TableMetadata mockTableMetadata = mock(TableMetadata.class);
        when(mockMetadata.getKeyspace("ks")).thenReturn(mockKeyspaceMetadata);
        when(mockMetadata.getKeyspace("ks").getTable("tbl")).thenReturn(mockTableMetadata);
        when(mockDelegate.metadata()).thenReturn(mockMetadata);

        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        final CountDownLatch closeLatch = new CountDownLatch(1);
        server.close(res -> closeLatch.countDown());
        vertx.close();
        if (closeLatch.await(60, TimeUnit.SECONDS))
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }

    /**
     * Create files for the upload under the staging directory. The directory structure looks like the following
     * staging:
     * <pre>
     * |- uuid
     *    |- files
     * </pre>
     *
     * @param uploadId the unique identifier for the upload
     * @return the temporary staging directory
     */
    protected Path createStagedUploadFiles(UUID uploadId) throws IOException
    {
        Path stagedUpload = Paths.get(SnapshotUtils.makeStagingDir(temporaryFolder.getCanonicalPath()))
                                 .resolve(uploadId.toString())
                                 .resolve("ks")
                                 .resolve("table");
        Files.createDirectories(stagedUpload);

        int filesCount = 5;
        for (int i = 0; i < filesCount; i++)
        {
            Files.createFile(stagedUpload.resolve(i + ".db"));
        }

        try (Stream<Path> list = Files.list(stagedUpload))
        {
            String[] files = list
                             .map(Path::toString)
                             .toArray(String[]::new);
            assertThat(files).isNotNull().hasSize(filesCount);
            return stagedUpload;
        }
    }

    class TestModuleOverride extends AbstractModule
    {
        private final CassandraAdapterDelegate delegate;
        private final Configuration mockConfiguration;

        TestModuleOverride(CassandraAdapterDelegate delegate, Configuration mockConfiguration)
        {
            this.delegate = delegate;
            this.mockConfiguration = mockConfiguration;
        }

        @Provides
        @Singleton
        public InstancesConfig getInstancesConfig() throws IOException
        {
            return mockInstancesConfig(temporaryFolder.getCanonicalPath(), delegate, delegate, null, null);
        }

        @Singleton
        @Provides
        public CassandraAdapterDelegate delegate()
        {
            return delegate;
        }

        @Singleton
        @Provides
        public Configuration abstractConfig(InstancesConfig instancesConfig)
        {
            when(mockConfiguration.getInstancesConfig()).thenReturn(instancesConfig);
            when(mockConfiguration.getHost()).thenReturn("127.0.0.1");
            when(mockConfiguration.getPort()).thenReturn(6475);
            when(mockConfiguration.getHealthCheckFrequencyMillis()).thenReturn(1000);
            when(mockConfiguration.isSslEnabled()).thenReturn(false);
            when(mockConfiguration.getRateLimitStreamRequestsPerSecond()).thenReturn(1L);
            when(mockConfiguration.getThrottleDelayInSeconds()).thenReturn(5L);
            when(mockConfiguration.getThrottleTimeoutInSeconds()).thenReturn(10L);
            when(mockConfiguration.getRequestIdleTimeoutMillis()).thenReturn(500);
            when(mockConfiguration.getRequestTimeoutMillis()).thenReturn(1000L);
            when(mockConfiguration.getSSTableImportPollIntervalMillis()).thenReturn(100);
            when(mockConfiguration.ssTableImportCacheConfiguration()).thenReturn(new CacheConfiguration(60_000, 100));
            when(mockConfiguration.getConcurrentUploadsLimit()).thenReturn(3);
            when(mockConfiguration.getMinSpacePercentRequiredForUpload()).thenReturn(0F);
            WorkerPoolConfiguration workerPoolConf = new WorkerPoolConfiguration("test-pool", 10,
                                                                                 TimeUnit.SECONDS.toMillis(30));
            when(mockConfiguration.serverWorkerPoolConfiguration()).thenReturn(workerPoolConf);
            when(mockConfiguration.serverInternalWorkerPoolConfiguration()).thenReturn(workerPoolConf);
            return mockConfiguration;
        }
    }
}
