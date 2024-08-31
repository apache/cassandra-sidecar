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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * An abstract class for SnapshotPathBuilder tests
 */
public abstract class AbstractSnapshotPathBuilderTest
{
    @TempDir
    protected File dataDir0;

    @TempDir
    protected File dataDir1;

    protected SnapshotPathBuilder instance;
    protected Vertx vertx = Vertx.vertx();
    protected ExecutorPools executorPools;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    protected void setup() throws IOException
    {
        CassandraInputValidator validator = new CassandraInputValidator();

        InstancesConfig mockInstancesConfig = mock(InstancesConfig.class);
        InstanceMetadata mockInstanceMeta = mock(InstanceMetadata.class);
        InstanceMetadata mockInvalidDataDirInstanceMeta = mock(InstanceMetadata.class);
        InstanceMetadata mockEmptyDataDirInstanceMeta = mock(InstanceMetadata.class);

        when(mockInstancesConfig.instanceFromHost("localhost")).thenReturn(mockInstanceMeta);
        when(mockInstanceMeta.dataDirs()).thenReturn(Arrays.asList(dataDir0.getAbsolutePath(),
                                                                   dataDir1.getAbsolutePath()));

        when(mockInstancesConfig.instanceFromHost("invalidDataDirInstance")).thenReturn(mockInvalidDataDirInstanceMeta);
        String invalidDirPath = dataDir0.getParentFile().getAbsolutePath() + "/invalid-data-dir";
        when(mockInvalidDataDirInstanceMeta.dataDirs()).thenReturn(Collections.singletonList(invalidDirPath));

        when(mockInstancesConfig.instanceFromHost("emptyDataDirInstance")).thenReturn(mockEmptyDataDirInstanceMeta);
        when(mockEmptyDataDirInstanceMeta.dataDirs()).thenReturn(Collections.emptyList());

        // Create some files and directories
        new File(dataDir0, "not_a_keyspace_dir").createNewFile();
        new File(dataDir0, "ks1/table1/snapshots/backup.2022-03-17-04-PDT/not_a_file.db").mkdirs();
        new File(dataDir0, "ks1/not_a_table_dir").createNewFile();
        new File(dataDir0, "ks1/table1/snapshots/not_a_snapshot_dir").createNewFile();
        new File(dataDir0, "data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd").mkdirs();

        new File(dataDir1, "ks3/table3/snapshots/snapshot1").mkdirs();

        // table was dropped and recreated. The table gets a new uuid
        new File(dataDir1, "data/ks4/table4-a6442310a57611ec8b980b0b2009844e" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u").mkdirs();
        new File(dataDir1, "data/ks4/table4-a72c8740a57611ec935db766a70c44a1" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u").mkdirs();

        // this is a different table with the same "table4" prefix
        new File(dataDir1, "data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u").mkdirs();

        // table && table-<TABLE_UUID>
        new File(dataDir0, "ks1/a_table/snapshots/a_snapshot/").mkdirs();
        new File(dataDir0, "ks1/a_table-a72c8740a57611ec935db766a70c44a1/snapshots/a_snapshot/").mkdirs();

        // create some files inside snapshot backup.2022-03-17-04-PDT
        new File(dataDir0, "ks1/table1/snapshots/backup.2022-03-17-04-PDT/data.db").createNewFile();
        new File(dataDir0, "ks1/table1/snapshots/backup.2022-03-17-04-PDT/index.db").createNewFile();
        new File(dataDir0, "ks1/table1/snapshots/backup.2022-03-17-04-PDT/nb-203-big-TOC.txt").createNewFile();

        // create some files inside snapshot ea823202-a62c-4603-bb6a-4e15d79091cd
        new File(dataDir0, "data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd/data.db")
        .createNewFile();
        new File(dataDir0, "data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd/index.db")
        .createNewFile();
        new File(dataDir0, "data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd/nb-203-big-TOC.txt")
        .createNewFile();

        // create some files inside snapshot snapshot1 in dataDir1
        new File(dataDir1, "ks3/table3/snapshots/snapshot1/data.db").createNewFile();
        new File(dataDir1, "ks3/table3/snapshots/snapshot1/index.db").createNewFile();
        new File(dataDir1, "ks3/table3/snapshots/snapshot1/nb-203-big-TOC.txt").createNewFile();

        // create some files inside snapshot this_is_a_valid_snapshot_name_i_❤_u in dataDir1
        new File(dataDir1, "data/ks4/table4-a6442310a57611ec8b980b0b2009844e" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db").createNewFile();
        new File(dataDir1, "data/ks4/table4-a6442310a57611ec8b980b0b2009844e" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db").createNewFile();
        new File(dataDir1, "data/ks4/table4-a6442310a57611ec8b980b0b2009844e" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/nb-203-big-TOC.txt").createNewFile();

        new File(dataDir1, "data/ks4/table4-a72c8740a57611ec935db766a70c44a1" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db").createNewFile();
        new File(dataDir1, "data/ks4/table4-a72c8740a57611ec935db766a70c44a1" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db").createNewFile();
        new File(dataDir1, "data/ks4/table4-a72c8740a57611ec935db766a70c44a1" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/nb-203-big-TOC.txt").createNewFile();

        new File(dataDir1, "data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db").createNewFile();
        new File(dataDir1, "data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db").createNewFile();
        new File(dataDir1, "data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/nb-203-big-TOC.txt").createNewFile();

        new File(dataDir0, "ks1/a_table/snapshots/a_snapshot/data.db").createNewFile();
        new File(dataDir0, "ks1/a_table/snapshots/a_snapshot/index.db").createNewFile();
        new File(dataDir0, "ks1/a_table/snapshots/a_snapshot/nb-203-big-TOC.txt").createNewFile();

        new File(dataDir0, "ks1/a_table-a72c8740a57611ec935db766a70c44a1" +
                           "/snapshots/a_snapshot/data.db").createNewFile();
        new File(dataDir0, "ks1/a_table-a72c8740a57611ec935db766a70c44a1" +
                           "/snapshots/a_snapshot/index.db").createNewFile();
        new File(dataDir0, "ks1/a_table-a72c8740a57611ec935db766a70c44a1" +
                           "/snapshots/a_snapshot/nb-203-big-TOC.txt").createNewFile();

        vertx = Vertx.vertx();
        ServiceConfiguration serviceConfiguration = new ServiceConfigurationImpl();
        executorPools = new ExecutorPools(vertx, serviceConfiguration);

        instance = initialize(vertx, serviceConfiguration, mockInstancesConfig, validator, executorPools);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterEach
    protected void clear()
    {
        dataDir0.delete();
        dataDir1.delete();
    }

    protected abstract SnapshotPathBuilder initialize(Vertx vertx,
                                                      ServiceConfiguration serviceConfiguration,
                                                      InstancesConfig instancesConfig,
                                                      CassandraInputValidator validator,
                                                      ExecutorPools executorPools);

    @Test
    void succeedsWhenSnapshotExists()
    {
        succeedsWhenPathExists(instance.streamSnapshotFiles(Collections.singletonList(dataDir0.getAbsolutePath() + "/ks1/table1"),
                                                            "backup.2022-03-17-04-PDT", false), snapshotFiles -> {
            assertThat(snapshotFiles).isNotEmpty();

            List<SnapshotPathBuilder.SnapshotFile> expectedSnapshotFiles = new ArrayList<>();
            Path basePath = Paths.get(dataDir0.getAbsolutePath() + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT/");
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("data.db"), 0, 0, null));
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("index.db"), 0, 0, null));
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("nb-203-big-TOC.txt"), 0, 0, null));
            assertThat(snapshotFiles).containsExactlyInAnyOrderElementsOf(expectedSnapshotFiles);
        });

        succeedsWhenPathExists(instance.streamSnapshotFiles(Collections.singletonList(dataDir0.getAbsolutePath() + "/data/ks2/table2"),
                                                            "ea823202-a62c-4603-bb6a-4e15d79091cd", false), snapshotFiles -> {
            assertThat(snapshotFiles).isNotEmpty();
            List<SnapshotPathBuilder.SnapshotFile> expectedSnapshotFiles = new ArrayList<>();
            Path basePath = Paths.get(dataDir0.getAbsolutePath() + "/data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd/");
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("data.db"), 0, 0, null));
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("index.db"), 0, 0, null));
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("nb-203-big-TOC.txt"), 0, 0, null));
            assertThat(snapshotFiles).containsExactlyInAnyOrderElementsOf(expectedSnapshotFiles);
        });

        succeedsWhenPathExists(instance.streamSnapshotFiles(Collections.singletonList(dataDir1.getAbsolutePath() + "/ks3/table3"),
                                                            "snapshot1", false), snapshotFiles -> {
            assertThat(snapshotFiles).isNotEmpty();
            List<SnapshotPathBuilder.SnapshotFile> expectedSnapshotFiles = new ArrayList<>();
            Path basePath = Paths.get(dataDir1.getAbsolutePath() + "/ks3/table3/snapshots/snapshot1");
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("data.db"), 0, 0, null));
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("index.db"), 0, 0, null));
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("nb-203-big-TOC.txt"), 0, 0, null));
            assertThat(snapshotFiles).containsExactlyInAnyOrderElementsOf(expectedSnapshotFiles);
        });

        // table table4 shares the prefix with table table4abc
        succeedsWhenPathExists(instance.streamSnapshotFiles(Collections.singletonList(dataDir1.getAbsolutePath() +
                                                                                      "/data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1"),
                                                            "this_is_a_valid_snapshot_name_i_❤_u", false), snapshotFiles -> {
            assertThat(snapshotFiles).isNotEmpty();
            List<SnapshotPathBuilder.SnapshotFile> expectedSnapshotFiles = new ArrayList<>();
            Path basePath = Paths.get(dataDir1.getAbsolutePath() +
                                      "/data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1/snapshots/this_is_a_valid_snapshot_name_i_❤_u");
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("data.db"), 0, 0, "a72c8740a57611ec935db766a70c44a1"));
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("index.db"), 0, 0, "a72c8740a57611ec935db766a70c44a1"));
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("nb-203-big-TOC.txt"), 0, 0, "a72c8740a57611ec935db766a70c44a1"));
            assertThat(snapshotFiles).containsExactlyInAnyOrderElementsOf(expectedSnapshotFiles);
        });
    }

    private void succeedsWhenPathExists(Future<Stream<SnapshotPathBuilder.SnapshotFile>> streamFuture,
                                        Consumer<List<SnapshotPathBuilder.SnapshotFile>> consumer)
    {
        VertxTestContext testContext = new VertxTestContext();
        streamFuture.onSuccess(stream -> {
            List<SnapshotPathBuilder.SnapshotFile> snapshotFiles = stream.collect(Collectors.toList());
            consumer.accept(snapshotFiles);
            testContext.completeNow();
        }).onFailure(testContext::failNow);

        // awaitCompletion has the semantics of a java.util.concurrent.CountDownLatch
        try
        {
            assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
