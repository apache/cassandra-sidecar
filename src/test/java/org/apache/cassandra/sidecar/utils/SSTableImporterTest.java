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

package org.apache.cassandra.sidecar.utils;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.TableOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class SSTableImporterTest
{
    private Vertx vertx;
    private InstanceMetadataFetcher mockMetadataFetcher;
    private Configuration mockConfiguration;
    private TableOperations mockTableOperations1;
    private ExecutorPools executorPools;
    private SSTableUploadsPathBuilder mockUploadPathBuilder;
    private SSTableImporter importer;

    @BeforeEach
    public void setup() throws InterruptedException
    {
        vertx = Vertx.vertx();
        mockMetadataFetcher = mock(InstanceMetadataFetcher.class);
        mockConfiguration = mock(Configuration.class);
        CassandraAdapterDelegate mockCassandraAdapterDelegate1 = mock(CassandraAdapterDelegate.class);
        CassandraAdapterDelegate mockCassandraAdapterDelegate2 = mock(CassandraAdapterDelegate.class);
        CassandraAdapterDelegate mockCassandraAdapterDelegate3 = mock(CassandraAdapterDelegate.class);
        mockTableOperations1 = mock(TableOperations.class);
        TableOperations mockTableOperations2 = mock(TableOperations.class);
        when(mockConfiguration.getSSTableImportPollIntervalMillis()).thenReturn(10);
        when(mockMetadataFetcher.delegate("localhost")).thenReturn(mockCassandraAdapterDelegate1);
        when(mockMetadataFetcher.delegate("127.0.0.2")).thenReturn(mockCassandraAdapterDelegate2);
        when(mockMetadataFetcher.delegate("127.0.0.3")).thenReturn(mockCassandraAdapterDelegate3);
        when(mockCassandraAdapterDelegate1.tableOperations()).thenReturn(mockTableOperations1);
        when(mockTableOperations1.importNewSSTables("ks", "tbl", "/dir", true, true,
                                                    true, true, true, true, false))
        .thenReturn(Collections.emptyList());
        when(mockTableOperations1.importNewSSTables("ks", "tbl", "/failed-dir", true, true,
                                                    true, true, true, true, false))
        .thenReturn(Collections.singletonList("/failed-dir"));
        when(mockCassandraAdapterDelegate2.tableOperations()).thenReturn(mockTableOperations2);
        when(mockTableOperations2.importNewSSTables("ks", "tbl", "/dir", true, true,
                                                    true, true, true, true, false))
        .thenThrow(new RuntimeException("Exception during import"));
        WorkerPoolConfiguration workerPoolConf = new WorkerPoolConfiguration("test-pool", 10,
                                                                             TimeUnit.SECONDS.toMillis(30));
        when(mockConfiguration.serverWorkerPoolConfiguration()).thenReturn(workerPoolConf);
        when(mockConfiguration.serverInternalWorkerPoolConfiguration()).thenReturn(workerPoolConf);
        executorPools = new ExecutorPools(vertx, mockConfiguration);
        mockUploadPathBuilder = mock(SSTableUploadsPathBuilder.class);

        // since we are not actually creating any files for the test, we need to handle cleanup such that we don't
        // get NullPointerExceptions because the mock is not wired up, and we need to prevent vertx from actually
        // doing a vertx.filesystem().deleteRecursive(). So we return a failed future with a fake path when checking
        // if the directory exists.
        when(mockUploadPathBuilder.resolveStagingDirectory(anyString(), anyString()))
        .thenReturn(Future.failedFuture("fake-path"));
        when(mockUploadPathBuilder.isValidDirectory("fake-path")).thenReturn(Future.failedFuture("skip cleanup"));
        importer = new SSTableImporter(vertx, mockMetadataFetcher, mockConfiguration, executorPools,
                                       mockUploadPathBuilder);
    }

    @Test
    void testImportSucceeds(VertxTestContext context)
    {
        Future<Void> importFuture = importer.scheduleImport(new SSTableImporter.ImportOptions.Builder()
                                                            .host("localhost")
                                                            .keyspace("ks")
                                                            .tableName("tbl")
                                                            .directory("/dir")
                                                            .uploadId("0000-0000")
                                                            .build());
        importFuture.onComplete(context.succeeding(v -> {
            assertThat(importer.importQueuePerHost).isNotEmpty();
            assertThat(importer.importQueuePerHost).containsKey("localhost$ks$tbl");
            for (SSTableImporter.ImportQueue queue : importer.importQueuePerHost.values())
            {
                assertThat(queue).isEmpty();
            }
            verify(mockTableOperations1, times(1))
            .importNewSSTables("ks", "tbl", "/dir", true, true, true, true, true, true, false);
            context.completeNow();
        }));
    }

    @Test
    void testImportFailsWhenCassandraIsUnavailable(VertxTestContext context)
    {
        Future<Void> importFuture = importer.scheduleImport(new SSTableImporter.ImportOptions.Builder()
                                                            .host("127.0.0.3")
                                                            .keyspace("ks")
                                                            .tableName("tbl")
                                                            .directory("/dir3")
                                                            .uploadId("0000-0000")
                                                            .build());
        importFuture.onComplete(context.failing(p -> {
            assertThat(p).isInstanceOf(HttpException.class);
            HttpException exception = (HttpException) p;
            assertThat(exception.getStatusCode()).isEqualTo(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
            assertThat(exception.getPayload()).isEqualTo("Cassandra service is unavailable");

            assertThat(importer.importQueuePerHost).isNotEmpty();
            assertThat(importer.importQueuePerHost).containsKey("127.0.0.3$ks$tbl");
            for (SSTableImporter.ImportQueue queue : importer.importQueuePerHost.values())
            {
                assertThat(queue).isEmpty();
            }
            context.completeNow();
        }));
    }

    @Test
    void testImportFailsWhenImportReturnsFailedDirectories(VertxTestContext context)
    {
        Future<Void> importFuture = importer.scheduleImport(new SSTableImporter.ImportOptions.Builder()
                                                            .host("localhost")
                                                            .keyspace("ks")
                                                            .tableName("tbl")
                                                            .directory("/failed-dir")
                                                            .uploadId("0000-0000")
                                                            .build());

        importFuture.onComplete(context.failing(p -> {
            assertThat(p).isInstanceOf(HttpException.class);
            HttpException exception = (HttpException) p;
            assertThat(exception.getStatusCode()).isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
            assertThat(exception.getPayload()).isEqualTo("Failed to import from directories: [/failed-dir]");

            assertThat(importer.importQueuePerHost).isNotEmpty();
            assertThat(importer.importQueuePerHost).containsKey("localhost$ks$tbl");
            for (SSTableImporter.ImportQueue queue : importer.importQueuePerHost.values())
            {
                assertThat(queue).isEmpty();
            }
            context.completeNow();
        }));
    }

    @Test
    void testImportFailsWhenImportThrowsIOException(VertxTestContext context)
    {
        Future<Void> importFuture = importer.scheduleImport(new SSTableImporter.ImportOptions.Builder()
                                                            .host("127.0.0.2")
                                                            .keyspace("ks")
                                                            .tableName("tbl")
                                                            .directory("/dir")
                                                            .uploadId("0000-0000")
                                                            .build());

        importFuture.onComplete(context.failing(p -> {
            assertThat(p).isInstanceOf(RuntimeException.class);
            RuntimeException exception = (RuntimeException) p;
            assertThat(exception.getMessage()).isEqualTo("Exception during import");

            assertThat(importer.importQueuePerHost).isNotEmpty();
            assertThat(importer.importQueuePerHost).containsKey("127.0.0.2$ks$tbl");
            for (SSTableImporter.ImportQueue queue : importer.importQueuePerHost.values())
            {
                assertThat(queue).isEmpty();
            }
            context.completeNow();
        }));
    }

    @Test
    void testCancelImportSucceeds(VertxTestContext context)
    {
        when(mockConfiguration.getSSTableImportPollIntervalMillis()).thenReturn(500);
        SSTableImporter importer = new SSTableImporter(vertx, mockMetadataFetcher, mockConfiguration, executorPools,
                                                       mockUploadPathBuilder);
        SSTableImporter.ImportOptions options = new SSTableImporter.ImportOptions.Builder()
                                                .host("localhost")
                                                .keyspace("ks")
                                                .tableName("tbl")
                                                .directory("/dir")
                                                .uploadId("0000-0000")
                                                .build();
        importer.scheduleImport(options);
        assertThat(importer.cancelImport(options)).isTrue();
        context.completeNow();
    }

    @Test
    void testCancelImportNoOpAfterProcessing(VertxTestContext context)
    {
        SSTableImporter.ImportOptions options = new SSTableImporter.ImportOptions.Builder()
                                                .host("localhost")
                                                .keyspace("ks")
                                                .tableName("tbl")
                                                .directory("/dir")
                                                .uploadId("0000-0000")
                                                .build();
        Future<Void> importFuture = importer.scheduleImport(options);
        importFuture.onComplete(context.succeeding(v -> {
            assertThat(importer.cancelImport(options)).isFalse();
            context.completeNow();
        }));
    }
}
