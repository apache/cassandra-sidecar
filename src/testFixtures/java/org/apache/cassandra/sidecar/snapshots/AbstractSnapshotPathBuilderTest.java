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
import java.nio.file.NoSuchFileException;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.data.StreamSSTableComponentRequest;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.assertj.core.api.InstanceOfAssertFactories;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.from;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * An abstract class for SnapshotPathBuilder tests
 */
@SuppressWarnings("deprecation")
public abstract class AbstractSnapshotPathBuilderTest
{
    @TempDir
    protected File dataDir0;

    @TempDir
    protected File dataDir1;

    protected SnapshotPathBuilder instance;
    protected Vertx vertx = Vertx.vertx();
    protected CassandraInputValidator validator;
    protected ExecutorPools executorPools;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @BeforeEach
    protected void setup() throws IOException
    {
        validator = new CassandraInputValidator();

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

        instance = initialize(vertx, serviceConfiguration, mockInstancesConfig, executorPools);
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
                                                      ExecutorPools executorPools);

    @Test
    void failsWhenKeyspaceIsNull()
    {
        String ks = null;
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest(ks,
                                                                                  "table",
                                                                                  "snapshot",
                                                                                  "component")))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("keyspace must not be null");
    }

    @Test
    void failsWhenKeyspaceContainsInvalidCharacters()
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest("i_❤_u",
                                                                                  "table",
                                                                                  "snapshot",
                                                                                  "component")))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Bad Request")
        .returns(HttpResponseStatus.BAD_REQUEST.code(), from(t -> ((HttpException) t).getStatusCode()))
        .returns("Invalid characters in keyspace: i_❤_u", from(t -> ((HttpException) t).getPayload()));
    }

    @Test
    void failsWhenKeyspaceContainsPathTraversalAttack()
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest("../../../etc/passwd",
                                                                                  "table",
                                                                                  "snapshot",
                                                                                  "component")))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Bad Request")
        .returns(HttpResponseStatus.BAD_REQUEST.code(), from(t -> ((HttpException) t).getStatusCode()))
        .returns("Invalid characters in keyspace: ../../../etc/passwd", from(t -> ((HttpException) t).getPayload()));
    }

    @ParameterizedTest
    @ValueSource(strings = { "system_schema", "system_traces", "system_distributed", "system", "system_auth",
                             "system_views", "system_virtual_schema" })
    void failsWhenKeyspaceIsForbidden(String forbiddenKeyspace)
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest(forbiddenKeyspace,
                                                                                  "table",
                                                                                  "snapshot",
                                                                                  "component")))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Forbidden")
        .returns(HttpResponseStatus.FORBIDDEN.code(), from(t -> ((HttpException) t).getStatusCode()))
        .returns("Forbidden keyspace: " + forbiddenKeyspace, from(t -> ((HttpException) t).getPayload()));
    }

    @Test
    void failsWhenTableNameIsNull()
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest("ks",
                                                                                  null,
                                                                                  "snapshot",
                                                                                  "component")))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("tableName must not be null");
    }

    @Test
    void failsWhenTableNameContainsInvalidCharacters()
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest("ks",
                                                                                  "i_❤_u",
                                                                                  "snapshot",
                                                                                  "component")))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Bad Request")
        .returns(HttpResponseStatus.BAD_REQUEST.code(), from(t -> ((HttpException) t).getStatusCode()))
        .returns("Invalid characters in table name: i_❤_u", from(t -> ((HttpException) t).getPayload()));
    }

    @Test
    void failsWhenTableNameContainsPathTraversalAttack()
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest("ks",
                                                                                  "../../../etc/passwd",
                                                                                  "snapshot",
                                                                                  "component")))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Bad Request")
        .returns(HttpResponseStatus.BAD_REQUEST.code(), from(t -> ((HttpException) t).getStatusCode()))
        .returns("Invalid characters in table name: ../../../etc/passwd",
                 from(t -> ((HttpException) t).getPayload()));
    }

    @Test
    void failsWhenSnapshotNameIsNull()
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest("ks",
                                                                                  "table",
                                                                                  null,
                                                                                  "component.db")))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("snapshotName must not be null");
    }

    @ParameterizedTest
    @ValueSource(strings = { "slash/is-not-allowed", "null-char\0-is-not-allowed", "../../../etc/passwd" })
    void failsWhenSnapshotNameContainsInvalidCharacters(String invalidFileName)
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest("ks",
                                                                                  "table",
                                                                                  invalidFileName,
                                                                                  "component.db")))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Bad Request")
        .returns(HttpResponseStatus.BAD_REQUEST.code(), from(t -> ((HttpException) t).getStatusCode()))
        .returns("Invalid characters in snapshot name: " + invalidFileName,
                 from(t -> ((HttpException) t).getPayload()));
    }

    @Test
    void failsWhenComponentNameIsNull()
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest("ks", "table", "snapshot", null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("componentName must not be null");
    }

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

    @ParameterizedTest
    @ValueSource(strings = { "f@o-Data.db", ".s./../../etc/passwd.db", "../../../bad-Data.db" })
    void failsWhenIndexComponentNameContainsInvalidCharacters(String invalidComponentName)
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest("ks",
                                                                                  "table",
                                                                                  "snapshot",
                                                                                  ".index",
                                                                                  invalidComponentName)))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Bad Request")
        .returns(HttpResponseStatus.BAD_REQUEST.code(), from(t -> ((HttpException) t).getStatusCode()))
        .returns("Invalid component name: " + invalidComponentName, from(t -> ((HttpException) t)
                                                                              .getPayload()));
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "does_not_start_with_dot" })
    void failsWhenIndexNameIsInvalid(String invalidIndexName)
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest("ks",
                                                                                  "table",
                                                                                  "snapshot",
                                                                                  invalidIndexName,
                                                                                  "component.db")))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { ".", "../bad-Data.db", ".f@o/bad-Data.db", ".bl@h/bad-TOC.txt" })
    void failsWhenIndexNameContainsInvalidCharacters(String invalidIndexName)
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest("ks",
                                                                                  "table",
                                                                                  "snapshot",
                                                                                  invalidIndexName,
                                                                                  "component.db")))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Bad Request")
        .returns(HttpResponseStatus.BAD_REQUEST.code(), from(t -> ((HttpException) t).getStatusCode()))
        .extracting(from(t -> ((HttpException) t).getPayload()), as(InstanceOfAssertFactories.STRING))
        .contains("Invalid characters in secondary index: ");
    }

    @Test
    void failsWhenDataDirsAreEmpty()
    {
        failsWithNoSuchFileException(instance.build("emptyDataDirInstance",
                                                    new StreamSSTableComponentRequest("ks",
                                                                                      "table",
                                                                                      "snapshot",
                                                                                      "component.db")),
                                     "No data directories are available for host 'emptyDataDirInstance'");
    }

    @Test
    void failsWhenInvalidDataDirectory()
    {
        failsWithNoSuchFileException(instance.build("invalidDataDirInstance",
                                                    new StreamSSTableComponentRequest("ks",
                                                                                      "table",
                                                                                      "snapshot",
                                                                                      "component.db")),
                                     "Keyspace 'ks' does not exist");
    }

    @Test
    void failsWhenKeyspaceDirectoryDoesNotExist()
    {
        failsWithNoSuchFileException(instance.build("localhost",
                                                    new StreamSSTableComponentRequest("non_existent",
                                                                                      "table",
                                                                                      "snapshot",
                                                                                      "component.db")),
                                     "Keyspace 'non_existent' does not exist");
    }

    @Test
    void failsWhenKeyspaceIsNotADirectory()
    {
        failsWithNoSuchFileException(instance.build("localhost",
                                                    new StreamSSTableComponentRequest("not_a_keyspace_dir",
                                                                                      "table",
                                                                                      "snapshot",
                                                                                      "component.db")),
                                     "Keyspace 'not_a_keyspace_dir' does not exist");
    }

    @Test
    void failsWhenTableDoesNotExist()
    {
        failsWithNoSuchFileException(instance.build("localhost",
                                                    new StreamSSTableComponentRequest("ks1",
                                                                                      "non_existent",
                                                                                      "snapshot",
                                                                                      "component.db")),
                                     "Table 'non_existent' does not exist");
    }

    @Test
    void failsWhenTableDoesNotExistWithSimilarPrefix()
    {
        // In this scenario, we have other tables with the "table" prefix (i.e table4)
        failsWithNoSuchFileException(instance.build("localhost",
                                                    new StreamSSTableComponentRequest("ks1",
                                                                                      "table",
                                                                                      "snapshot",
                                                                                      "component.db")),
                                     "Table 'table' does not exist");
    }

    @Test
    void failsWhenTableNameIsNotADirectory()
    {
        failsWithNoSuchFileException(instance.build("localhost",
                                                    new StreamSSTableComponentRequest("ks1",
                                                                                      "not_a_table_dir",
                                                                                      "snapshot",
                                                                                      "component.db")),
                                     "Table 'not_a_table_dir' does not exist");
    }

    @Test
    void failsWhenSnapshotDirectoryDoesNotExist()
    {
        failsWithNoSuchFileException(instance.build("localhost",
                                                    new StreamSSTableComponentRequest("ks1",
                                                                                      "table1",
                                                                                      "non_existent",
                                                                                      "component.db")),
                                     "Component 'component.db' does not exist for snapshot 'non_existent'");
    }

    @Test
    void failsWhenSnapshotIsNotADirectory()
    {
        failsWithNoSuchFileException(instance.build("localhost",
                                                    new StreamSSTableComponentRequest("ks1",
                                                                                      "table1",
                                                                                      "not_a_snapshot_dir",
                                                                                      "component.db")),
                                     "Component 'component.db' does not exist for snapshot 'not_a_snapshot_dir'");
    }

    @Test
    void failsWhenComponentFileDoesNotExist()
    {
        String errMsg = "Component 'does-not-exist-TOC.txt' does not exist for snapshot 'backup.2022-03-17-04-PDT'";
        failsWithNoSuchFileException(instance.build("localhost",
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
        failsWithNoSuchFileException(instance.build("localhost",
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
            Path basePath = Paths.get(dataDir1.getAbsolutePath() + "/data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1/snapshots/this_is_a_valid_snapshot_name_i_❤_u");
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("data.db"), 0, 0, "a72c8740a57611ec935db766a70c44a1"));
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("index.db"), 0, 0, "a72c8740a57611ec935db766a70c44a1"));
            expectedSnapshotFiles.add(new SnapshotPathBuilder.SnapshotFile(basePath.resolve("nb-203-big-TOC.txt"), 0, 0, "a72c8740a57611ec935db766a70c44a1"));
            assertThat(snapshotFiles).containsExactlyInAnyOrderElementsOf(expectedSnapshotFiles);
        });
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void testTableWithUUIDPicked(@TempDir File tempDir) throws IOException
    {
        File dataDir = new File(tempDir, "data");
        dataDir.mkdirs();

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
        SnapshotPathBuilder newBuilder = new SnapshotPathBuilder(vertx, mockInstancesConfig, validator, executorPools);
        expectedPath = atableWithUUID.getAbsolutePath() + "/snapshots/a_snapshot/data.db";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks1",
                                                                        "a_table",
                                                                        "a_snapshot",
                                                                        "data.db")),
                               expectedPath);

        expectedPath = atableWithUUID.getAbsolutePath() + "/snapshots/a_snapshot/index.db";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks1",
                                                                        "a_table",
                                                                        "a_snapshot",
                                                                        "index.db")),
                               expectedPath);

        expectedPath = atableWithUUID.getAbsolutePath() + "/snapshots/a_snapshot/nb-203-big-TOC.txt";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks1",
                                                                        "a_table",
                                                                        "a_snapshot",
                                                                        "nb-203-big-TOC.txt")),
                               expectedPath);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    void testLastModifiedTablePicked(@TempDir File tempDir) throws IOException
    {
        File dataDir = new File(tempDir, "data");
        dataDir.mkdirs();

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
        SnapshotPathBuilder newBuilder = new SnapshotPathBuilder(vertx, mockInstancesConfig, validator, executorPools);
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
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db";
        succeedsWhenPathExists(newBuilder
                               .build("localhost",
                                      new StreamSSTableComponentRequest("ks4",
                                                                        "table4",
                                                                        "this_is_a_valid_snapshot_name_i_❤_u",
                                                                        "index.db")),
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

    protected void failsWithNoSuchFileException(Future<?> future, String expectedMessage)
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
        assertThat(testContext.causeOfFailure()).isInstanceOf(NoSuchFileException.class)
                                                .returns(expectedMessage, from(Throwable::getMessage));
    }
}
