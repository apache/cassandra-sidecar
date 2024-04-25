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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.codahale.metrics.SharedMetricRegistries;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.TableOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SSTableImportConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.TestServiceConfiguration;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetricsImpl;

import static org.apache.cassandra.sidecar.AssertionUtils.loopAssert;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
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
    private TableOperations mockTableOperations1;
    private ExecutorPools executorPools;
    private SSTableUploadsPathBuilder mockUploadPathBuilder;
    private TestSSTableImporter importer;
    private ServiceConfiguration serviceConfiguration;

    @BeforeEach
    public void setup() throws InterruptedException
    {
        vertx = Vertx.vertx();
        serviceConfiguration =
        TestServiceConfiguration.builder()
                                .sstableImportConfiguration(new SSTableImportConfigurationImpl(10))
                                .build();

        mockMetadataFetcher = mock(InstanceMetadataFetcher.class);
        CassandraAdapterDelegate mockCassandraAdapterDelegate1 = mock(CassandraAdapterDelegate.class);
        CassandraAdapterDelegate mockCassandraAdapterDelegate2 = mock(CassandraAdapterDelegate.class);
        CassandraAdapterDelegate mockCassandraAdapterDelegate3 = mock(CassandraAdapterDelegate.class);
        mockTableOperations1 = mock(TableOperations.class);
        TableOperations mockTableOperations2 = mock(TableOperations.class);

        InstanceMetadata mockInstanceMetadata1 = mock(InstanceMetadata.class);
        InstanceMetadata mockInstanceMetadata2 = mock(InstanceMetadata.class);
        InstanceMetadata mockInstanceMetadata3 = mock(InstanceMetadata.class);
        when(mockInstanceMetadata1.metrics()).thenReturn(instanceMetrics(1));
        when(mockInstanceMetadata1.delegate()).thenReturn(mockCassandraAdapterDelegate1);
        when(mockInstanceMetadata2.metrics()).thenReturn(instanceMetrics(2));
        when(mockInstanceMetadata2.delegate()).thenReturn(mockCassandraAdapterDelegate2);
        when(mockInstanceMetadata3.metrics()).thenReturn(instanceMetrics(3));
        when(mockInstanceMetadata3.delegate()).thenReturn(mockCassandraAdapterDelegate3);
        when(mockMetadataFetcher.instance("localhost")).thenReturn(mockInstanceMetadata1);
        when(mockMetadataFetcher.instance("127.0.0.2")).thenReturn(mockInstanceMetadata2);
        when(mockMetadataFetcher.instance("127.0.0.3")).thenReturn(mockInstanceMetadata3);
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
        executorPools = new ExecutorPools(vertx, serviceConfiguration);
        mockUploadPathBuilder = mock(SSTableUploadsPathBuilder.class);

        // since we are not actually creating any files for the test, we need to handle cleanup such that we don't
        // get NullPointerExceptions because the mock is not wired up, and we need to prevent vertx from actually
        // doing a vertx.filesystem().deleteRecursive(). So we return a failed future with a fake path when checking
        // if the directory exists.
        when(mockUploadPathBuilder.resolveUploadIdDirectory(anyString(), anyString()))
        .thenReturn(Future.failedFuture("fake-path"));
        when(mockUploadPathBuilder.isValidDirectory("fake-path")).thenReturn(Future.failedFuture("skip cleanup"));
        importer = new TestSSTableImporter(vertx, mockMetadataFetcher, serviceConfiguration, executorPools,
                                           mockUploadPathBuilder);
    }

    @AfterEach
    void clear()
    {
        SharedMetricRegistries.clear();
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

        loopAssert(1, () -> {
            // ensure that one element is reported in the import queue
            assertThat(instanceMetrics(1).sstableImport().pendingImports.metric.getValue()).isOne();
            importer.latch.countDown();
        });

        importFuture.onComplete(context.succeeding(v -> {
            assertThat(importer.importQueuePerHost).isNotEmpty();
            assertThat(importer.importQueuePerHost).containsKey(new SSTableImporter.ImportKey("localhost", "ks", "tbl"));
            for (SSTableImporter.ImportQueue queue : importer.importQueuePerHost.values())
            {
                assertThat(queue).isEmpty();
            }
            verify(mockTableOperations1, times(1))
            .importNewSSTables("ks", "tbl", "/dir", true, true, true, true, true, true, false);
            vertx.setTimer(100, handle -> {
                // after successful import, the queue must be drained
                assertThat(instanceMetrics(1).sstableImport().pendingImports.metric.getValue()).isZero();
                assertThat(instanceMetrics(1).sstableImport().successfulImports.metric.getValue()).isOne();
                context.completeNow();
            });
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

        loopAssert(1, () -> {
            // ensure that one element is reported in the import queue
            assertThat(instanceMetrics(3).sstableImport().pendingImports.metric.getValue()).isOne();
            importer.latch.countDown();
        });

        importFuture.onComplete(context.failing(p -> {
            assertThat(p).isInstanceOf(HttpException.class);
            HttpException exception = (HttpException) p;
            assertThat(exception.getStatusCode()).isEqualTo(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
            assertThat(exception.getPayload()).isEqualTo("Cassandra service is unavailable");

            assertThat(importer.importQueuePerHost).isNotEmpty();
            assertThat(importer.importQueuePerHost).containsKey(new SSTableImporter.ImportKey("127.0.0.3", "ks", "tbl"));
            for (SSTableImporter.ImportQueue queue : importer.importQueuePerHost.values())
            {
                assertThat(queue).isEmpty();
            }
            loopAssert(1, () -> {
                // import queue must be drained even in the case of failure, and pendingImports metric should reflect that
                assertThat(instanceMetrics(3).sstableImport().pendingImports.metric.getValue()).isZero();
                context.completeNow();
            });
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

        loopAssert(1, () -> {
            // ensure that one element is reported in the import queue
            assertThat(instanceMetrics(1).sstableImport().pendingImports.metric.getValue()).isOne();
            importer.latch.countDown();
        });

        importFuture.onComplete(context.failing(p -> {
            assertThat(p).isInstanceOf(HttpException.class);
            HttpException exception = (HttpException) p;
            assertThat(exception.getStatusCode()).isEqualTo(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
            assertThat(exception.getPayload()).isEqualTo("Failed to import from directories: [/failed-dir]");

            assertThat(importer.importQueuePerHost).isNotEmpty();
            assertThat(importer.importQueuePerHost).containsKey(new SSTableImporter.ImportKey("localhost", "ks", "tbl"));
            for (SSTableImporter.ImportQueue queue : importer.importQueuePerHost.values())
            {
                assertThat(queue).isEmpty();
            }
            vertx.setTimer(100, v -> {
                assertThat(instanceMetrics(1).sstableImport().pendingImports.metric.getValue()).isZero();
                assertThat(instanceMetrics(1).sstableImport().failedImports.metric.getValue()).isOne();
                context.completeNow();
            });
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

        loopAssert(1, () -> {
            // ensure that one element is reported in the import queue
            assertThat(instanceMetrics(2).sstableImport().pendingImports.metric.getValue()).isOne();
            importer.latch.countDown();
        });

        importFuture.onComplete(context.failing(p -> {
            assertThat(p).isInstanceOf(RuntimeException.class);
            RuntimeException exception = (RuntimeException) p;
            assertThat(exception.getMessage()).isEqualTo("Exception during import");

            assertThat(importer.importQueuePerHost).isNotEmpty();
            assertThat(importer.importQueuePerHost).containsKey(new SSTableImporter.ImportKey("127.0.0.2", "ks", "tbl"));
            for (SSTableImporter.ImportQueue queue : importer.importQueuePerHost.values())
            {
                assertThat(queue).isEmpty();
            }
            vertx.setTimer(100, v -> {
                assertThat(instanceMetrics(2).sstableImport().pendingImports.metric.getValue()).isZero();
                assertThat(instanceMetrics(2).sstableImport().failedImports.metric.getValue()).isOne();
                context.completeNow();
            });
        }));
    }

    @Test
    void testCancelImportSucceeds(VertxTestContext context)
    {
        serviceConfiguration =
        TestServiceConfiguration.builder()
                                .sstableImportConfiguration(new SSTableImportConfigurationImpl(500))
                                .build();

        SSTableImporter importer = new SSTableImporter(vertx, mockMetadataFetcher, serviceConfiguration, executorPools,
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

        loopAssert(1, () -> {
            // ensure that one element is reported in the import queue
            assertThat(instanceMetrics(1).sstableImport().pendingImports.metric.getValue()).isOne();
            importer.latch.countDown();
        });

        importFuture.onComplete(context.succeeding(v -> {
            assertThat(importer.cancelImport(options)).isFalse();
            context.completeNow();
        }));
    }

    @Test
    void testAggregatesMetricsForTheSameHost(VertxTestContext context)
    {
        List<Future<Void>> futures = new ArrayList<>();
        futures.add(importer.scheduleImport(new SSTableImporter.ImportOptions.Builder()
                                            .host("localhost")
                                            .keyspace("ks")
                                            .tableName("tbl")
                                            .directory("/dir")
                                            .uploadId("0000-0000")
                                            .build()));
        futures.add(importer.scheduleImport(new SSTableImporter.ImportOptions.Builder()
                                            .host("localhost")
                                            .keyspace("ks2")
                                            .tableName("tbl")
                                            .directory("/dir")
                                            .uploadId("0000-0001")
                                            .build()));

        loopAssert(1, () -> {
            // ensure that one element is reported in the import queue
            assertThat(instanceMetrics(1).sstableImport().pendingImports.metric.getValue()).isEqualTo(2);
            importer.latch.countDown();
        });

        Future.all(futures)
              .onComplete(context.succeeding(v -> {
                  assertThat(importer.importQueuePerHost).isNotEmpty();
                  assertThat(importer.importQueuePerHost).containsKey(new SSTableImporter.ImportKey("localhost", "ks", "tbl"));
                  assertThat(importer.importQueuePerHost).containsKey(new SSTableImporter.ImportKey("localhost", "ks2", "tbl"));
                  for (SSTableImporter.ImportQueue queue : importer.importQueuePerHost.values())
                  {
                      assertThat(queue).isEmpty();
                  }
                  verify(mockTableOperations1, times(1))
                  .importNewSSTables("ks", "tbl", "/dir", true, true, true, true, true, true, false);
                  verify(mockTableOperations1, times(1))
                  .importNewSSTables("ks2", "tbl", "/dir", true, true, true, true, true, true, false);
                  vertx.setTimer(100, handle -> {
                      // after successful import, the queue must be drained
                      assertThat(instanceMetrics(1).sstableImport().pendingImports.metric.getValue()).isZero();
                      assertThat(instanceMetrics(1).sstableImport().successfulImports.metric.getValue()).isEqualTo(2);
                      context.completeNow();
                  });
              }));
    }

    InstanceMetrics instanceMetrics(int id)
    {
        return new InstanceMetricsImpl(registry(id));
    }

    /**
     * Injects into the maybeDrainImportQueue method to better test the class behavior
     */
    static class TestSSTableImporter extends SSTableImporter
    {
        final CountDownLatch latch = new CountDownLatch(1);

        TestSSTableImporter(Vertx vertx,
                            InstanceMetadataFetcher metadataFetcher,
                            ServiceConfiguration configuration,
                            ExecutorPools executorPools,
                            SSTableUploadsPathBuilder uploadPathBuilder)
        {
            super(vertx, metadataFetcher, configuration, executorPools, uploadPathBuilder);
        }

        @Override
        void maybeDrainImportQueue(ImportQueue queue)
        {
            try
            {
                latch.await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            super.maybeDrainImportQueue(queue);
        }
    }
}
