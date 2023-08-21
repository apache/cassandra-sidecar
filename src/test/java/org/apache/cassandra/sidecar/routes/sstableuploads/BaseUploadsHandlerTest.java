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
import org.apache.cassandra.sidecar.MainModule;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.config.SSTableUploadConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.impl.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.config.impl.SidecarConfigurationImpl;
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
    protected CassandraAdapterDelegate mockDelegate;
    protected SidecarConfiguration sidecarConfiguration;
    @TempDir
    protected File temporaryFolder;
    protected SSTableUploadConfiguration mockSSTableUploadConfiguration;

    @BeforeEach
    void setup() throws InterruptedException
    {
        mockDelegate = mock(CassandraAdapterDelegate.class);
        TestModule testModule = new TestModule();
        ServiceConfiguration serviceConfiguration = testModule.configuration().serviceConfiguration();
        mockSSTableUploadConfiguration = mock(SSTableUploadConfiguration.class);
        when(mockSSTableUploadConfiguration.concurrentUploadsLimit()).thenReturn(3);
        when(mockSSTableUploadConfiguration.minimumSpacePercentageRequired()).thenReturn(0F);
        sidecarConfiguration =
        ((SidecarConfigurationImpl) testModule.configuration())
        .unbuild()
        .serviceConfiguration(((ServiceConfigurationImpl) serviceConfiguration)
                              .unbuild()
                              .requestIdleTimeoutMillis(500)
                              .requestTimeoutMillis(1000L)
                              .ssTableUploadConfiguration(mockSSTableUploadConfiguration)
                              .build())
        .build();
        TestModuleOverride testModuleOverride = new TestModuleOverride(mockDelegate);
        Injector injector = Guice.createInjector(Modules.override(new MainModule())
                                                        .with(Modules.override(testModule)
                                                                     .with(testModuleOverride)));
        server = injector.getInstance(HttpServer.class);
        vertx = injector.getInstance(Vertx.class);
        client = WebClient.create(vertx);

        VertxTestContext context = new VertxTestContext();
        server.listen(0, "localhost", context.succeedingThenComplete());

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

        TestModuleOverride(CassandraAdapterDelegate delegate)
        {
            this.delegate = delegate;
        }

        @Provides
        @Singleton
        public InstancesConfig instancesConfig() throws IOException
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
        public SidecarConfiguration configuration()
        {
            return sidecarConfiguration;
        }
    }
}
