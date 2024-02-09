///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.cassandra.sidecar.snapshots;
//
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.function.BiConsumer;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//
//import io.vertx.core.Future;
//import io.vertx.core.Vertx;
//import io.vertx.core.file.FileProps;
//import io.vertx.junit5.VertxExtension;
//import io.vertx.junit5.VertxTestContext;
//import org.apache.cassandra.sidecar.cluster.InstancesConfig;
//import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
//import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
//import org.apache.cassandra.sidecar.config.ServiceConfiguration;
//import org.apache.cassandra.sidecar.data.SnapshotRequest;
//import org.apache.cassandra.sidecar.data.StreamSSTableComponentRequest;
//import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
//import org.jetbrains.annotations.NotNull;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
///**
// * Unit tests for {@link CachedSnapshotPathBuilder}
// */
//@ExtendWith(VertxExtension.class)
//class CachedSnapshotPathBuilderTest extends AbstractSnapshotPathBuilderTest
//{
//    TestCachedSnapshotPathBuilder instance2;
//
//    @Override
//    public SnapshotPathBuilder initialize(Vertx vertx, ServiceConfiguration serviceConfiguration,
//                                          InstancesConfig instancesConfig, ExecutorPools executorPools)
//    {
//        instance2 = new TestCachedSnapshotPathBuilder(vertx, serviceConfiguration, instancesConfig,
//                                                      validator, executorPools);
//        instance2.tableDirCache.invalidateAll(); // make sure we start with an empty cache
//        instance2.snapshotListCache.invalidateAll(); // make sure we start with an empty cache
//
//        // To ensure that lastModified timestamp is guaranteed to be smaller for the new data dir
//        // we provide it explicitly
//        try
//        {
//            customizeLastModifiedTimestamp();
//        }
//        catch (IOException ex)
//        {
//            throw new RuntimeException(ex);
//        }
//
//        return new CachedSnapshotPathBuilder(vertx, serviceConfiguration, instancesConfig, validator, executorPools);
//    }
//
//    @Test
//    void testStreamSSTableComponentRequestCache()
//    {
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(0);
//        succeedsWhenPathExists(instance2.build("localhost",
//                                               new StreamSSTableComponentRequest("ks1",
//                                                                                 "table1",
//                                                                                 "backup.2022-03-17-04-PDT",
//                                                                                 "data.db")),
//                               dataDir0.getAbsolutePath()
//                               + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT/data.db");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(1);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(1);
//
//        // Try again with the same values as before
//        succeedsWhenPathExists(instance2.build("localhost",
//                                               new StreamSSTableComponentRequest("ks1",
//                                                                                 "table1",
//                                                                                 "backup.2022-03-17-04-PDT",
//                                                                                 "data.db")),
//                               dataDir0.getAbsolutePath()
//                               + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT/data.db");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(1);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(1);
//
//        // Now we try a different component within the same host, ks, and table name
//        succeedsWhenPathExists(instance2.build("localhost",
//                                               new StreamSSTableComponentRequest("ks1",
//                                                                                 "table1",
//                                                                                 "backup.2022-03-17-04-PDT",
//                                                                                 "nb-203-big-TOC.txt")),
//                               dataDir0.getAbsolutePath()
//                               + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT/nb-203-big-TOC.txt");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(1);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(1);
//
//        // Now let's try a different ks/table
//        succeedsWhenPathExists(instance2.build("localhost",
//                                               new StreamSSTableComponentRequest("ks4",
//                                                                                 "table4",
//                                                                                 "this_is_a_valid_snapshot_name_i_❤_u",
//                                                                                 "data.db")),
//                               dataDir1.getAbsolutePath()
//                               + "/data/ks4/table4-a72c8740a57611ec935db766a70c44a1"
//                               + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(2);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(2);
//
//        // Let's try the same ks as before but different table
//        succeedsWhenPathExists(instance2.build("localhost",
//                                               new StreamSSTableComponentRequest("ks4",
//                                                                                 "table4abc",
//                                                                                 "this_is_a_valid_snapshot_name_i_❤_u",
//                                                                                 "data.db")),
//                               dataDir1.getAbsolutePath()
//                               + "/data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1"
//                               + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(3);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(3);
//
//        // Let's try a different component in the same table as before
//        succeedsWhenPathExists(instance2.build("localhost",
//                                               new StreamSSTableComponentRequest("ks4",
//                                                                                 "table4abc",
//                                                                                 "this_is_a_valid_snapshot_name_i_❤_u",
//                                                                                 "index.db")),
//                               dataDir1.getAbsolutePath()
//                               + "/data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1"
//                               + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(3);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(3);
//
//        // Now let's try a directory that doesn't exist
//        failsWithNoSuchFileException(instance2.build("localhost",
//                                                     new StreamSSTableComponentRequest("ks1",
//                                                                                       "table",
//                                                                                       "snapshot",
//                                                                                       "component.db")),
//                                     "Table 'table' does not exist");
//        // the value of the entry is INVALID_PATH
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(4);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(4);
//
//        // Accessing the same non-existing directory should return the same INVALID_PATH from cache
//        failsWithNoSuchFileException(instance2.build("localhost",
//                                                     new StreamSSTableComponentRequest("ks1",
//                                                                                       "table",
//                                                                                       "snapshot",
//                                                                                       "component.db")),
//                                     "Table 'table' does not exist");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(4);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(4);
//    }
//
//    @Test
//    void testListSnapshotFilesRequestCache() throws IOException
//    {
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(0);
//        succeedsWhenPathExists(instance2.build("localhost",
//                                               new SnapshotRequest("ks1",
//                                                                   "table1",
//                                                                   "backup.2022-03-17-04-PDT",
//                                                                   false,
//                                                                   null)),
//                               dataDir0.getAbsolutePath()
//                               + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(1);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(1);
//
//        // Try again with the same values as before
//        succeedsWhenPathExists(instance2.build("localhost",
//                                               new SnapshotRequest("ks1",
//                                                                   "table1",
//                                                                   "backup.2022-03-17-04-PDT",
//                                                                   false,
//                                                                   null)),
//                               dataDir0.getAbsolutePath()
//                               + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(1);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(1);
//
//        // Now we try a different value for includeSecondaryIndexFiles within the same host, ks, and table name
//        succeedsWhenPathExists(instance2.build("localhost",
//                                               new SnapshotRequest("ks1",
//                                                                   "table1",
//                                                                   "backup.2022-03-17-04-PDT",
//                                                                   true,
//                                                                   null)),
//                               dataDir0.getAbsolutePath()
//                               + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(1);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(1);
//
//        // Now let's try a different ks/table
//        succeedsWhenPathExists(instance2.build("localhost",
//                                               new SnapshotRequest("ks4",
//                                                                   "table4",
//                                                                   "this_is_a_valid_snapshot_name_i_❤_u",
//                                                                   false,
//                                                                   null)),
//                               dataDir1.getAbsolutePath()
//                               + "/data/ks4/table4-a72c8740a57611ec935db766a70c44a1"
//                               + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(2);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(2);
//
//        // Let's try the same ks as before but different table
//        succeedsWhenPathExists(instance2.build("localhost",
//                                               new SnapshotRequest("ks4",
//                                                                   "table4abc",
//                                                                   "this_is_a_valid_snapshot_name_i_❤_u",
//                                                                   false,
//                                                                   null)),
//                               dataDir1.getAbsolutePath()
//                               + "/data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1"
//                               + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(3);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(3);
//
//        // Let's try a different value for includeSecondaryIndexFiles with the same table as before
//        succeedsWhenPathExists(instance2.build("localhost",
//                                               new SnapshotRequest("ks4",
//                                                                   "table4abc",
//                                                                   "this_is_a_valid_snapshot_name_i_❤_u",
//                                                                   true,
//                                                                   null)),
//                               dataDir1.getAbsolutePath()
//                               + "/data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1"
//                               + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(3);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(3);
//
//        // Now move the snapshot directory to make sure that the existence validation happens always
//        Path moveRoot = dataDir1.toPath().resolve("data")
//                                .resolve("ks4")
//                                .resolve("table4abc-a72c8740a57611ec935db766a70c44a1")
//                                .resolve("snapshots");
//        Path from = moveRoot.resolve("this_is_a_valid_snapshot_name_i_❤_u");
//        Path to = moveRoot.resolve("move_me");
//
//        Files.move(from, to);
//
//        // the snapshot directory
//        // /data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1/snapshots/this_is_a_valid_snapshot_name_i_❤_u
//        // no longer exists, so we expect a failure
//        failsWithNoSuchFileException(instance2.build("localhost",
//                                                     new SnapshotRequest("ks4",
//                                                                         "table4abc",
//                                                                         "this_is_a_valid_snapshot_name_i_❤_u",
//                                                                         true,
//                                                                         null)),
//                                     "Snapshot directory 'this_is_a_valid_snapshot_name_i_❤_u' does not exist");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(3);
//
//        // restore the directory
//        Files.move(to, from);
//
//        // Now let's try a directory that doesn't exist
//        failsWithNoSuchFileException(instance2.build("localhost",
//                                                     new SnapshotRequest("ks1",
//                                                                         "table",
//                                                                         "snapshot",
//                                                                         false,
//                                                                         null)),
//                                     "Table 'table' does not exist");
//        // the value of the entry is INVALID_PATH
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(4);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(4);
//
//        // Accessing the same non-existing directory should return the same INVALID_PATH from cache
//        failsWithNoSuchFileException(instance2.build("localhost",
//                                                     new SnapshotRequest("ks1",
//                                                                         "table",
//                                                                         "snapshot",
//                                                                         false,
//                                                                         null)),
//                                     "Table 'table' does not exist");
//        assertThat(instance2.tableDirCache.estimatedSize()).isEqualTo(4);
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(4);
//    }
//
//    @Test
//    void testListSnapshotDirectoryCache() throws ExecutionException, InterruptedException
//    {
//        // get a copy of the existing files inside /ks1/table1/snapshots/backup.2022-03-17-04-PDT
//        List<String> existingFiles1 = instance.listSnapshotDirectory(dataDir0.getAbsolutePath()
//                                                                     + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT",
//                                                                     false)
//                                              .toCompletionStage()
//                                              .toCompletableFuture()
//                                              .get()
//                                              .stream()
//                                              .map(snapshotFile -> snapshotFile.path)
//                                              .sorted()
//                                              .collect(Collectors.toList());
//
//        // get a copy of the existing files inside
//        // /data/ks4/table4-a72c8740a57611ec935db766a70c44a1/snapshots/this_is_a_valid_snapshot_name_i_❤_u
//        List<String> existingFiles2 = instance.listSnapshotDirectory(dataDir1.getAbsolutePath()
//                                                                     + "/data/ks4"
//                                                                     + "/table4-a72c8740a57611ec935db766a70c44a1"
//                                                                     + "/snapshots"
//                                                                     + "/this_is_a_valid_snapshot_name_i_❤_u",
//                                                                     false)
//                                              .toCompletionStage()
//                                              .toCompletableFuture()
//                                              .get()
//                                              .stream()
//                                              .map(snapshotFile -> snapshotFile.path)
//                                              .sorted()
//                                              .collect(Collectors.toList());
//
//        instance2.snapshotListCache.invalidateAll(); // make sure our cache is emptied out before testing
//        assertThat(instance2.snapshotListCache.estimatedSize()).isEqualTo(0);
//        succeedsWhenDirectoryExists(instance2.listSnapshotDirectory(dataDir0.getAbsolutePath()
//                                                                    + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT",
//                                                                    false),
//                                    existingFiles1);
//        assertThat(instance2.snapshotListCache.estimatedSize()).isEqualTo(1);
//
//        // Try again with the same values as before
//        succeedsWhenDirectoryExists(instance2.listSnapshotDirectory(dataDir0.getAbsolutePath()
//                                                                    + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT",
//                                                                    false),
//                                    existingFiles1);
//        assertThat(instance2.snapshotListCache.estimatedSize()).isEqualTo(1);
//
//        // Now we try a different value for includeSecondaryIndexFiles within the same directory
//        // we don't expect index files in this snapshot so the existingFiles list is the same
//        succeedsWhenDirectoryExists(instance2.listSnapshotDirectory(dataDir0.getAbsolutePath()
//                                                                    + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT",
//                                                                    true),
//                                    existingFiles1);
//        assertThat(instance2.snapshotListCache.estimatedSize()).isEqualTo(2);
//
//        // Now let's try a different snapshot
//        succeedsWhenDirectoryExists(instance2.listSnapshotDirectory(dataDir1.getAbsolutePath()
//                                                                    + "/data/ks4"
//                                                                    + "/table4-a72c8740a57611ec935db766a70c44a1"
//                                                                    + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u",
//                                                                    false),
//                                    existingFiles2);
//        assertThat(instance2.snapshotListCache.estimatedSize()).isEqualTo(3);
//
//        // Let's try the same snapshot as before but different includeSecondaryIndexFiles value
//        succeedsWhenDirectoryExists(instance2.listSnapshotDirectory(dataDir1.getAbsolutePath()
//                                                                    + "/data/ks4"
//                                                                    + "/table4-a72c8740a57611ec935db766a70c44a1"
//                                                                    + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u",
//                                                                    true),
//                                    existingFiles2);
//        assertThat(instance2.snapshotListCache.estimatedSize()).isEqualTo(4);
//    }
//
//    @Test
//    void testCacheConcurrencyOnSucceededFuture() throws InterruptedException
//    {
//        testConcurrency(this::succeedsWhenPathExists,
//                        instance2.build("localhost",
//                                        new StreamSSTableComponentRequest("ks1",
//                                                                          "table1",
//                                                                          "backup.2022-03-17-04-PDT",
//                                                                          "data.db")),
//                        dataDir0.getAbsolutePath() + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT/data.db");
//        testConcurrency(this::succeedsWhenPathExists,
//                        instance2.build("localhost",
//                                        new SnapshotRequest("ks1",
//                                                            "table1",
//                                                            "backup.2022-03-17-04-PDT",
//                                                            false,
//                                                            null)),
//                        dataDir0.getAbsolutePath() + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT");
//    }
//
//    @Test
//    void testCacheConcurrencyOnFailedFuture() throws InterruptedException
//    {
//        testConcurrency(this::failsWithNoSuchFileException,
//                        instance2.build("localhost",
//                                        new StreamSSTableComponentRequest("ks1",
//                                                                          "table",
//                                                                          "snapshot",
//                                                                          "component.db")),
//                        "Table 'table' does not exist");
//        testConcurrency(this::failsWithNoSuchFileException,
//                        instance2.build("localhost",
//                                        new SnapshotRequest("ks1", "table", "snapshot", true, null)),
//                        "Table 'table' does not exist");
//    }
//
//    void testConcurrency(BiConsumer<Future<String>, String> consumer,
//                         Future<String> future,
//                         String expected) throws InterruptedException
//    {
//        final int nThreads = 20;
//        final ExecutorService pool = Executors.newFixedThreadPool(nThreads);
//        final CountDownLatch latch = new CountDownLatch(nThreads);
//
//        for (int i = 0; i < nThreads; i++)
//        {
//            pool.submit(() -> {
//                try
//                {
//                    // Invoke the method roughly at the same time
//                    latch.countDown();
//                    latch.await();
//                    // The first thread to win creates the future, the rest should get the same instance
//                    consumer.accept(future, expected);
//                }
//                catch (InterruptedException e)
//                {
//                    throw new RuntimeException(e);
//                }
//            });
//        }
//
//        pool.shutdown();
//        assertThat(pool.awaitTermination(1, TimeUnit.MINUTES)).isTrue();
//        assertThat(instance2.futureResolutionCount.get()).isEqualTo(1);
//    }
//
//    void succeedsWhenDirectoryExists(Future<List<SnapshotPathBuilder.SnapshotFile>> future, List<String> expected)
//    {
//        VertxTestContext testContext = new VertxTestContext();
//        future.onComplete(testContext.succeedingThenComplete());
//        // awaitCompletion has the semantics of a java.util.concurrent.CountDownLatch
//        try
//        {
//            assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
//        }
//        catch (InterruptedException e)
//        {
//            throw new RuntimeException(e);
//        }
//        assertThat(testContext.failed()).isFalse();
//        List<String> actual = future.result()
//                                    .stream()
//                                    .map(snapshotFile -> snapshotFile.path)
//                                    .sorted()
//                                    .collect(Collectors.toList());
//        assertThat(actual).isEqualTo(expected);
//    }
//
//    private void customizeLastModifiedTimestamp() throws IOException
//    {
//        // Guarantees that the timestamp returned for the new directory is always greater than the old directory
//        FileProps oldKs4FileProps = mock(FileProps.class);
//        FileProps newKs4FileProps = mock(FileProps.class);
//        when(oldKs4FileProps.isDirectory()).thenReturn(true);
//        when(oldKs4FileProps.lastModifiedTime()).thenReturn(2L);
//        when(newKs4FileProps.isDirectory()).thenReturn(true);
//        when(newKs4FileProps.lastModifiedTime()).thenReturn(4L);
//
//        instance2.customFileToFilePropsMap.put(dataDir1.getCanonicalPath()
//                                               + "/data/ks4/table4-a6442310a57611ec8b980b0b2009844e" , oldKs4FileProps);
//        instance2.customFileToFilePropsMap.put(dataDir1.getCanonicalPath()
//                                               + "/data/ks4/table4-a72c8740a57611ec935db766a70c44a1", newKs4FileProps);
//    }
//
//    /**
//     * Keep track of how many futures are resolved to ensure that future results are cached
//     */
//    static class TestCachedSnapshotPathBuilder extends CachedSnapshotPathBuilder
//    {
//        final AtomicInteger futureResolutionCount = new AtomicInteger();
//        final Map<String, FileProps> customFileToFilePropsMap = new HashMap<>();
//
//        public TestCachedSnapshotPathBuilder(Vertx vertx,
//                                             ServiceConfiguration serviceConfiguration,
//                                             InstancesConfig instancesConfig,
//                                             CassandraInputValidator validator,
//                                             ExecutorPools executorPools)
//        {
//            super(vertx, serviceConfiguration, instancesConfig, validator, executorPools);
//        }
//
//        @Override
//        protected Future<String> getTableDirectory(List<String> dataDirs, QualifiedTableName name)
//        {
//            // keep track of how many times this future has been executed, to make sure the future
//            // is only processed once when it is in the cache
//            futureResolutionCount.incrementAndGet();
//            return super.getTableDirectory(dataDirs, name);
//        }
//
//        @Override
//        public Future<List<SnapshotFile>> listSnapshotDirectory(String snapshotDirectory,
//                                                                boolean includeSecondaryIndexFiles)
//        {
//            // keep track of how many times this future has been executed, to make sure the future
//            // is only processed once when it is in the cache
//            futureResolutionCount.incrementAndGet();
//            return super.listSnapshotDirectory(snapshotDirectory, includeSecondaryIndexFiles);
//        }
//
//        @Override
//        protected @NotNull Function<String, Future<FileProps>> filePropsProvider()
//        {
//            return path -> {
//                FileProps fileProps = customFileToFilePropsMap.get(path);
//                if (fileProps != null)
//                {
//                    return Future.succeededFuture(fileProps);
//                }
//                return fs.props(path);
//            };
//        }
//    }
//}
