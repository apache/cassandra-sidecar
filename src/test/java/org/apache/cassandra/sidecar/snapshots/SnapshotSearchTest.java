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

package org.apache.cassandra.sidecar.snapshots;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;

import static org.apache.cassandra.sidecar.snapshots.SnapshotUtils.mockInstancesConfig;
import static org.apache.cassandra.sidecar.snapshots.SnapshotUtils.snapshot1Instance1Files;
import static org.apache.cassandra.sidecar.snapshots.SnapshotUtils.snapshot1Instance2Files;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for searching snapshots
 */
@ExtendWith(VertxExtension.class)
public class SnapshotSearchTest
{
    @TempDir
    Path temporaryFolder;

    SnapshotPathBuilder instance;
    Vertx vertx = Vertx.vertx();
    String rootDir;

    @BeforeEach
    public void setup() throws IOException
    {
        rootDir = temporaryFolder.toFile().getCanonicalPath();
        SnapshotUtils.initializeTmpDirectory(temporaryFolder.toFile());
        InstancesConfig mockInstancesConfig = mockInstancesConfig(vertx, rootDir);

        CassandraInputValidator validator = new CassandraInputValidator();
        ExecutorPools executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
        instance = new SnapshotPathBuilder(vertx, mockInstancesConfig, validator, executorPools);
    }

    @Test
    public void testListSnapshotDirectoryIncludeSecondaryIndex() throws InterruptedException
    {
        findAndListSnapshotHelper("localhost", "snapshot1", true,
                                  Arrays.asList(rootDir + "/d1/data/keyspace1/table1-1234/snapshots/snapshot1",
                                                rootDir + "/d1/data/keyspace1/table2-1234/snapshots/snapshot1"),
                                  Arrays.asList(rootDir + "/d1/data/keyspace1/table1-1234/snapshots/snapshot1"
                                                + "/.index/secondary.db",
                                                rootDir + "/d1/data/keyspace1/table1-1234/snapshots/snapshot1/1.db",
                                                rootDir + "/d1/data/keyspace1/table2-1234/snapshots/snapshot1/3.db"));
    }

    @Test
    public void testListSnapshotDirectoryDoNotIncludeSecondaryIndex() throws InterruptedException
    {
        findAndListSnapshotHelper("localhost", "snapshot1", false,
                                  Arrays.asList(rootDir + "/d1/data/keyspace1/table1-1234/snapshots/snapshot1",
                                                rootDir + "/d1/data/keyspace1/table2-1234/snapshots/snapshot1"),
                                  snapshot1Instance1Files());
    }

    @Test
    public void testListSnapshotDirectoryPerInstance() throws InterruptedException
    {
        // When host name is instance1's host name, we should get files of snapshot1 from instance 1
        findAndListSnapshotHelper("localhost", "snapshot1", false,
                                  Arrays.asList(rootDir + "/d1/data/keyspace1/table1-1234/snapshots/snapshot1",
                                                rootDir + "/d1/data/keyspace1/table2-1234/snapshots/snapshot1"),
                                  snapshot1Instance1Files());

        // When host name is instance2's host name, we should get files of snapshot1 from instance 1
        findAndListSnapshotHelper("localhost2", "snapshot1", false,
                                  Arrays.asList(rootDir + "/d2/data/keyspace1/table1-1234/snapshots/snapshot1",
                                                rootDir + "/d2/data/keyspace1/table2-1234/snapshots/snapshot1"),
                                  snapshot1Instance2Files());
    }

    // Helper methods

    private void findAndListSnapshotHelper(String host, String snapshotName,
                                           boolean includeSecondaryIndexFiles,
                                           List<String> expectedDirectories,
                                           List<String> expectedFiles) throws InterruptedException
    {
        VertxTestContext testContext = new VertxTestContext();
        Future<List<String>> future = instance.findSnapshotDirectories(host, snapshotName);
        future.onComplete(testContext.succeedingThenComplete());
        // awaitCompletion has the semantics of a java.util.concurrent.CountDownLatch
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        assertThat(testContext.failed()).isFalse();
        List<String> snapshotDirectories = future.result();
        assertThat(snapshotDirectories).isNotNull();
        Collections.sort(snapshotDirectories);
        assertThat(snapshotDirectories).isEqualTo(expectedDirectories);

        List<Future<List<SnapshotPathBuilder.SnapshotFile>>> futures =
        snapshotDirectories.stream()
                           .map(directory -> instance.listSnapshotDirectory(directory, includeSecondaryIndexFiles))
                           .collect(Collectors.toList());

        VertxTestContext compositeFutureContext = new VertxTestContext();
        CompositeFuture ar = Future.all(futures);
        ar.onComplete(compositeFutureContext.succeedingThenComplete());
        assertThat(compositeFutureContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        assertThat(compositeFutureContext.failed()).isFalse();

        // flat map results
        //noinspection unchecked
        List<String> snapshotFiles = ar.list()
                                       .stream()
                                       .flatMap(l -> ((List<SnapshotPathBuilder.SnapshotFile>) l).stream())
                                       .map(snapshotFile -> snapshotFile.path)
                                       .sorted()
                                       .collect(Collectors.toList());

        assertThat(snapshotFiles.size()).isEqualTo(expectedFiles.size());

        for (int i = 0; i < expectedFiles.size(); i++)
        {
            assertThat(snapshotFiles.get(i)).endsWith(expectedFiles.get(i));
        }
    }
}
