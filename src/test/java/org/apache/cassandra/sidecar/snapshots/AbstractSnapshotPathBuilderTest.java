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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.rules.TemporaryFolder;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.ListSnapshotFilesRequest;
import org.apache.cassandra.sidecar.common.data.StreamSSTableComponentRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.from;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

abstract class AbstractSnapshotPathBuilderTest
{
    @TempDir
    File dataDir0;

    @TempDir
    File dataDir1;

    SnapshotPathBuilder instance;
    Vertx vertx = Vertx.vertx();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    void setup() throws IOException
    {
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

        new File(dataDir1, "data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db").createNewFile();
        new File(dataDir1, "data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db").createNewFile();
        new File(dataDir1, "data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1" +
                           "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/nb-203-big-TOC.txt").createNewFile();

        vertx = Vertx.vertx();
        instance = initialize(vertx, mockInstancesConfig);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @AfterEach
    void clear()
    {
        dataDir0.delete();
        dataDir1.delete();
    }

    abstract SnapshotPathBuilder initialize(Vertx vertx, InstancesConfig instancesConfig);

    @ParameterizedTest
    @ValueSource(strings = { "i_❤_u.db", "this-is-not-allowed.jar", "cql-is-not-allowed-here.cql",
                             "json-is-not-allowed-here.json", "crc32-is-not-allowed-here.crc32",
                             "../../../etc/passwd.db" })
    void failsWhenComponentNameContainsInvalidCharacters(String invalidComponentName)
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest("ks",
                                                                                  "table",
                                                                                  "snapshot",
                                                                                  invalidComponentName)))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Bad Request")
        .returns(HttpResponseStatus.BAD_REQUEST.code(), from(t -> ((HttpException) t).getStatusCode()))
        .returns("Invalid component name: " + invalidComponentName, from(t -> ((HttpException) t)
                                                                              .getPayload()));
    }

    @Test
    void failsWhenDataDirsAreEmpty()
    {
        failsWithFileNotFoundException(instance.build("emptyDataDirInstance",
                                                      new StreamSSTableComponentRequest("ks",
                                                                                        "table",
                                                                                        "snapshot",
                                                                                        "component.db")),
                                       "No data directories are available for host 'emptyDataDirInstance'");
        failsWithFileNotFoundException(instance.build("emptyDataDirInstance",
                                                      new ListSnapshotFilesRequest("ks",
                                                                                   "table",
                                                                                   "snapshot",
                                                                                   false)),
                                       "No data directories are available for host 'emptyDataDirInstance'");
    }

    @Test
    void failsWhenInvalidDataDirectory()
    {
        failsWithFileNotFoundException(instance.build("invalidDataDirInstance",
                                                      new StreamSSTableComponentRequest("ks",
                                                                                        "table",
                                                                                        "snapshot",
                                                                                        "component.db")),
                                       "Keyspace 'ks' does not exist");
        failsWithFileNotFoundException(instance.build("invalidDataDirInstance",
                                                      new ListSnapshotFilesRequest("ks",
                                                                                   "table",
                                                                                   "snapshot",
                                                                                   false)),
                                       "Keyspace 'ks' does not exist");
    }

    @Test
    void failsWhenKeyspaceDirectoryDoesNotExist()
    {
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("non_existent",
                                                                                        "table",
                                                                                        "snapshot",
                                                                                        "component.db")),
                                       "Keyspace 'non_existent' does not exist");
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new ListSnapshotFilesRequest("non_existent",
                                                                                   "table",
                                                                                   "snapshot",
                                                                                   false)),
                                       "Keyspace 'non_existent' does not exist");
    }

    @Test
    void failsWhenKeyspaceIsNotADirectory()
    {
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("not_a_keyspace_dir",
                                                                                        "table",
                                                                                        "snapshot",
                                                                                        "component.db")),
                                       "Keyspace 'not_a_keyspace_dir' does not exist");
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new ListSnapshotFilesRequest("not_a_keyspace_dir",
                                                                                   "table",
                                                                                   "snapshot",
                                                                                   false)),
                                       "Keyspace 'not_a_keyspace_dir' does not exist");
    }

    @Test
    void failsWhenTableDoesNotExist()
    {
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("ks1",
                                                                                        "non_existent",
                                                                                        "snapshot",
                                                                                        "component.db")),
                                       "Table 'non_existent' does not exist");
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new ListSnapshotFilesRequest("ks1",
                                                                                   "non_existent",
                                                                                   "snapshot",
                                                                                   false)),
                                       "Table 'non_existent' does not exist");
    }

    @Test
    void failsWhenTableDoesNotExistWithSimilarPrefix()
    {
        // In this scenario, we have other tables with the "table" prefix (i.e table4)
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("ks1",
                                                                                        "table",
                                                                                        "snapshot",
                                                                                        "component.db")),
                                       "Table 'table' does not exist");
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new ListSnapshotFilesRequest("ks1",
                                                                                   "table",
                                                                                   "snapshot",
                                                                                   false)),
                                       "Table 'table' does not exist");
    }

    @Test
    void failsWhenTableNameIsNotADirectory()
    {
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("ks1",
                                                                                        "not_a_table_dir",
                                                                                        "snapshot",
                                                                                        "component.db")),
                                       "Table 'not_a_table_dir' does not exist");
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new ListSnapshotFilesRequest("ks1",
                                                                                   "not_a_table_dir",
                                                                                   "snapshot",
                                                                                   false)),
                                       "Table 'not_a_table_dir' does not exist");
    }

    @Test
    void failsWhenSnapshotDirectoryDoesNotExist()
    {
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("ks1",
                                                                                        "table1",
                                                                                        "non_existent",
                                                                                        "component.db")),
                                       "Component 'component.db' does not exist for snapshot 'non_existent'");
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new ListSnapshotFilesRequest("ks1",
                                                                                   "table1",
                                                                                   "non_existent",
                                                                                   false)),
                                       "Snapshot directory 'non_existent' does not exist");
    }

    @Test
    void failsWhenSnapshotIsNotADirectory()
    {
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("ks1",
                                                                                        "table1",
                                                                                        "not_a_snapshot_dir",
                                                                                        "component.db")),
                                       "Component 'component.db' does not exist for snapshot 'not_a_snapshot_dir'");
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new ListSnapshotFilesRequest("ks1",
                                                                                   "table1",
                                                                                   "not_a_snapshot_dir",
                                                                                   false)),
                                       "Snapshot directory 'not_a_snapshot_dir' does not exist");
    }

    @Test
    void failsWhenComponentFileDoesNotExist()
    {
        String errMsg = "Component 'does-not-exist-TOC.txt' does not exist for snapshot 'backup.2022-03-17-04-PDT'";
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("ks1",
                                                                                        "table1",
                                                                                        "backup.2022-03-17-04-PDT",
                                                                                        "does-not-exist-TOC.txt")),
                                       errMsg);
    }

    @Test
    void failsWhenComponentIsNotAFile()
    {
        String errMsg = "Component 'not_a_file.db' does not exist for snapshot 'backup.2022-03-17-04-PDT'";
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("ks1",
                                                                                        "table1",
                                                                                        "backup.2022-03-17-04-PDT",
                                                                                        "not_a_file.db")),
                                       errMsg);
    }

    @Test
    void succeedsWhenComponentExists()
    {
        String expectedPath;
        expectedPath = dataDir0.getAbsolutePath() + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT/data.db";
        succeedsWhenPathExists(instance.build("localhost",
                                              new StreamSSTableComponentRequest("ks1",
                                                                                "table1",
                                                                                "backup.2022-03-17-04-PDT",
                                                                                "data.db")),
                               expectedPath);
        expectedPath = dataDir0.getAbsolutePath() + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT/index.db";
        succeedsWhenPathExists(instance.build("localhost",
                                              new StreamSSTableComponentRequest("ks1",
                                                                                "table1",
                                                                                "backup.2022-03-17-04-PDT",
                                                                                "index.db")),
                               expectedPath);
        expectedPath = dataDir0.getAbsolutePath() + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT/nb-203-big-TOC.txt";
        succeedsWhenPathExists(instance.build("localhost",
                                              new StreamSSTableComponentRequest("ks1",
                                                                                "table1",
                                                                                "backup.2022-03-17-04-PDT",
                                                                                "nb-203-big-TOC.txt")),
                               expectedPath);
        expectedPath = dataDir0.getAbsolutePath()
                       + "/data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd/data.db";
        succeedsWhenPathExists(instance
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks2",
                                                                        "table2",
                                                                        "ea823202-a62c-4603-bb6a-4e15d79091cd",
                                                                        "data.db")),
                               expectedPath);
        expectedPath = dataDir0.getAbsolutePath()
                       + "/data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd/index.db";
        succeedsWhenPathExists(instance
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks2",
                                                                        "table2",
                                                                        "ea823202-a62c-4603-bb6a-4e15d79091cd",
                                                                        "index.db")),
                               expectedPath);
        expectedPath = dataDir0.getAbsolutePath()
                       + "/data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd/nb-203-big-TOC.txt";
        succeedsWhenPathExists(instance
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks2",
                                                                        "table2",
                                                                        "ea823202-a62c-4603-bb6a-4e15d79091cd",
                                                                        "nb-203-big-TOC.txt")),
                               expectedPath);
        expectedPath = dataDir1.getAbsolutePath() + "/ks3/table3/snapshots/snapshot1/data.db";
        succeedsWhenPathExists(instance.build("localhost",
                                              new StreamSSTableComponentRequest("ks3",
                                                                                "table3",
                                                                                "snapshot1",
                                                                                "data.db")),
                               expectedPath);
        expectedPath = dataDir1.getAbsolutePath() + "/ks3/table3/snapshots/snapshot1/index.db";
        succeedsWhenPathExists(instance.build("localhost",
                                              new StreamSSTableComponentRequest("ks3",
                                                                                "table3",
                                                                                "snapshot1",
                                                                                "index.db")),
                               expectedPath);
        expectedPath = dataDir1.getAbsolutePath() + "/ks3/table3/snapshots/snapshot1/nb-203-big-TOC.txt";
        succeedsWhenPathExists(instance.build("localhost",
                                              new StreamSSTableComponentRequest("ks3",
                                                                                "table3",
                                                                                "snapshot1",
                                                                                "nb-203-big-TOC.txt")),
                               expectedPath);


        // table table4 shares the prefix with table table4abc
        expectedPath = dataDir1.getAbsolutePath()
                       + "/data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1"
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db";
        succeedsWhenPathExists(instance
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks4",
                                                                        "table4abc",
                                                                        "this_is_a_valid_snapshot_name_i_❤_u",
                                                                        "data.db")),
                               expectedPath);
        expectedPath = dataDir1.getAbsolutePath()
                       + "/data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1"
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db";
        succeedsWhenPathExists(instance
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks4",
                                                                        "table4abc",
                                                                        "this_is_a_valid_snapshot_name_i_❤_u",
                                                                        "index.db")),
                               expectedPath);
        expectedPath = dataDir1.getAbsolutePath()
                       + "/data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1"
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/nb-203-big-TOC.txt";
        succeedsWhenPathExists(instance
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks4",
                                                                        "table4abc",
                                                                        "this_is_a_valid_snapshot_name_i_❤_u",
                                                                        "nb-203-big-TOC.txt")),
                               expectedPath);
    }

    @Test
    void succeedsWhenSnapshotExists()
    {
        String expectedPath = dataDir0.getAbsolutePath() + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT";
        succeedsWhenPathExists(instance.build("localhost",
                                              new ListSnapshotFilesRequest("ks1",
                                                                           "table1",
                                                                           "backup.2022-03-17-04-PDT",
                                                                           false)),
                               expectedPath);

        expectedPath = dataDir0.getAbsolutePath() + "/data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd";
        succeedsWhenPathExists(instance.build("localhost",
                                              new ListSnapshotFilesRequest("ks2",
                                                                           "table2",
                                                                           "ea823202-a62c-4603-bb6a-4e15d79091cd",
                                                                           false)),
                               expectedPath);

        expectedPath = dataDir1.getAbsolutePath() + "/ks3/table3/snapshots/snapshot1";
        succeedsWhenPathExists(instance.build("localhost",
                                              new ListSnapshotFilesRequest("ks3",
                                                                           "table3",
                                                                           "snapshot1",
                                                                           false)),
                               expectedPath);

        // table table4 shares the prefix with table table4abc
        expectedPath = dataDir1.getAbsolutePath()
                       + "/data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1"
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u";
        succeedsWhenPathExists(instance.build("localhost",
                                              new ListSnapshotFilesRequest("ks4",
                                                                           "table4abc",
                                                                           "this_is_a_valid_snapshot_name_i_❤_u",
                                                                           false)),
                               expectedPath);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void testTableWithUUIDPicked() throws IOException
    {
        TemporaryFolder tempFolder = new TemporaryFolder();
        tempFolder.create();
        File dataDir = tempFolder.newFolder("data");

        InstancesConfig mockInstancesConfig = mock(InstancesConfig.class);
        InstanceMetadata mockInstanceMeta = mock(InstanceMetadata.class);

        when(mockInstancesConfig.instanceFromHost("localhost")).thenReturn(mockInstanceMeta);
        when(mockInstanceMeta.dataDirs()).thenReturn(Collections.singletonList(dataDir.getAbsolutePath()));

        File atable = new File(dataDir, "data/ks1/a_table");
        atable.mkdirs();
        File atableSnapshot = new File(atable, "snapshots/a_snapshot");
        atableSnapshot.mkdirs();
        new File(atable, "snapshots/a_snapshot/data.db").createNewFile();
        new File(atable, "snapshots/a_snapshot/index.db").createNewFile();
        new File(atable, "snapshots/a_snapshot/nb-203-big-TOC.txt").createNewFile();

        File atableWithUUID = new File(dataDir, "data/ks1/a_table-a72c8740a57611ec935db766a70c44a1");
        atableWithUUID.mkdirs();
        File atableWithUUIDSnapshot = new File(atableWithUUID, "snapshots/a_snapshot");
        atableWithUUIDSnapshot.mkdirs();

        new File(atableWithUUID, "snapshots/a_snapshot/data.db").createNewFile();
        new File(atableWithUUID, "snapshots/a_snapshot/index.db").createNewFile();
        new File(atableWithUUID, "snapshots/a_snapshot/nb-203-big-TOC.txt").createNewFile();
        atableWithUUID.setLastModified(System.currentTimeMillis() + 2000000);

        String expectedPath;
        // a_table and a_table-<TABLE_UUID> - the latter should be picked
        SnapshotPathBuilder newBuilder = new SnapshotPathBuilder(vertx, mockInstancesConfig);
        expectedPath = atableWithUUID.getAbsolutePath() + "/snapshots/a_snapshot/data.db";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks1",
                                                                        "a_table",
                                                                        "a_snapshot",
                                                                        "data.db")),
                               expectedPath);

        expectedPath = atableWithUUID.getAbsolutePath() + "/snapshots/a_snapshot";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new ListSnapshotFilesRequest("ks1",
                                                                   "a_table",
                                                                   "a_snapshot",
                                                                   false)),
                               expectedPath);

        expectedPath = atableWithUUID.getAbsolutePath() + "/snapshots/a_snapshot/index.db";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks1",
                                                                        "a_table",
                                                                        "a_snapshot",
                                                                        "index.db")),
                               expectedPath);

        expectedPath = atableWithUUID.getAbsolutePath() + "/snapshots/a_snapshot";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new ListSnapshotFilesRequest("ks1",
                                                                   "a_table",
                                                                   "a_snapshot",
                                                                   false)),
                               expectedPath);

        expectedPath = atableWithUUID.getAbsolutePath() + "/snapshots/a_snapshot/nb-203-big-TOC.txt";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks1",
                                                                        "a_table",
                                                                        "a_snapshot",
                                                                        "nb-203-big-TOC.txt")),
                               expectedPath);

        expectedPath = atableWithUUID.getAbsolutePath() + "/snapshots/a_snapshot";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new ListSnapshotFilesRequest("ks1",
                                                                   "a_table",
                                                                   "a_snapshot",
                                                                   false)),
                               expectedPath);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void testLastModifiedTablePicked() throws IOException
    {
        TemporaryFolder tempFolder = new TemporaryFolder();
        tempFolder.create();
        File dataDir = tempFolder.newFolder("data");

        InstancesConfig mockInstancesConfig = mock(InstancesConfig.class);
        InstanceMetadata mockInstanceMeta = mock(InstanceMetadata.class);

        when(mockInstancesConfig.instanceFromHost("localhost")).thenReturn(mockInstanceMeta);
        when(mockInstanceMeta.dataDirs()).thenReturn(Collections.singletonList(dataDir.getAbsolutePath()));

        File table4Old = new File(dataDir, "data/ks4/table4-a6442310a57611ec8b980b0b2009844e1");
        table4Old.mkdirs();

        // table was dropped and recreated. The table gets a new uuid
        File table4OldSnapshot = new File(table4Old, "snapshots/this_is_a_valid_snapshot_name_i_❤_u");
        table4OldSnapshot.mkdirs();
        // create some files inside snapshot this_is_a_valid_snapshot_name_i_❤_u in dataDir1
        new File(table4Old, "snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db").createNewFile();
        new File(table4Old, "snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db").createNewFile();
        new File(table4Old, "snapshots/this_is_a_valid_snapshot_name_i_❤_u/nb-203-big-TOC.txt").createNewFile();

        File table4New = new File(dataDir, "data/ks4/table4-a72c8740a57611ec935db766a70c44a1");
        table4New.mkdirs();

        File table4NewSnapshot = new File(table4New, "snapshots/this_is_a_valid_snapshot_name_i_❤_u");
        table4NewSnapshot.mkdirs();

        new File(table4New, "snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db").createNewFile();
        new File(table4New, "snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db").createNewFile();
        new File(table4New, "snapshots/this_is_a_valid_snapshot_name_i_❤_u/nb-203-big-TOC.txt").createNewFile();
        table4New.setLastModified(System.currentTimeMillis() + 2000000);

        String expectedPath;
        SnapshotPathBuilder newBuilder = new SnapshotPathBuilder(vertx, mockInstancesConfig);
        // table4-a72c8740a57611ec935db766a70c44a1 is the last modified, so it is the correct directory
        expectedPath = table4New.getAbsolutePath()
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks4",
                                                                        "table4",
                                                                        "this_is_a_valid_snapshot_name_i_❤_u",
                                                                        "data.db")),
                               expectedPath);

        expectedPath = table4New.getAbsolutePath()
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new ListSnapshotFilesRequest("ks4",
                                                                   "table4",
                                                                   "this_is_a_valid_snapshot_name_i_❤_u",
                                                                   false)),
                               expectedPath);

        expectedPath = table4New.getAbsolutePath()
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks4",
                                                                        "table4",
                                                                        "this_is_a_valid_snapshot_name_i_❤_u",
                                                                        "index.db")),
                               expectedPath);

        expectedPath = table4New.getAbsolutePath()
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new ListSnapshotFilesRequest("ks4",
                                                                   "table4",
                                                                   "this_is_a_valid_snapshot_name_i_❤_u",
                                                                   false)),
                               expectedPath);

        expectedPath = table4New.getAbsolutePath()
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/nb-203-big-TOC.txt";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks4",
                                                                        "table4",
                                                                        "this_is_a_valid_snapshot_name_i_❤_u",
                                                                        "nb-203-big-TOC.txt")),
                               expectedPath);

        expectedPath = table4New.getAbsolutePath()
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new ListSnapshotFilesRequest("ks4",
                                                                   "table4",
                                                                   "this_is_a_valid_snapshot_name_i_❤_u",
                                                                   false)),
                               expectedPath);
    }

    protected void succeedsWhenPathExists(Future<String> future, String expectedPath)
    {
        VertxTestContext testContext = new VertxTestContext();
        future.onComplete(testContext.succeedingThenComplete());
        // awaitCompletion has the semantics of a java.util.concurrent.CountDownLatch
        try
        {
            assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        assertThat(testContext.failed()).isFalse();
        // we use ends with here, because MacOS prepends the /private path for temporary directories
        assertThat(future.result()).endsWith(expectedPath);
    }

    protected void failsWithFileNotFoundException(Future<String> future, String expectedMessage)
    {
        VertxTestContext testContext = new VertxTestContext();
        future.onComplete(testContext.succeedingThenComplete());
        // awaitCompletion has the semantics of a java.util.concurrent.CountDownLatch
        try
        {
            assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        assertThat(testContext.failed()).isTrue();
        assertThat(testContext.causeOfFailure()).isInstanceOf(FileNotFoundException.class)
                                                .returns(expectedMessage, from(Throwable::getMessage));
    }
}
