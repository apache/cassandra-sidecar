package org.apache.cassandra.sidecar.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
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
        when(mockInvalidDataDirInstanceMeta.dataDirs()).thenReturn(Arrays.asList(invalidDirPath));

        when(mockInstancesConfig.instanceFromHost("emptyDataDirInstance")).thenReturn(mockEmptyDataDirInstanceMeta);
        when(mockEmptyDataDirInstanceMeta.dataDirs()).thenReturn(Arrays.asList());

        // Create some files and directories
        assertThat(new File(dataDir0, "not_a_keyspace_dir").createNewFile());
        assertThat(new File(dataDir0, "ks1/table1/snapshots/backup.2022-03-17-04-PDT/not_a_file.db").mkdirs());
        assertThat(new File(dataDir0, "ks1/not_a_table_dir").createNewFile());
        assertThat(new File(dataDir0, "ks1/table1/snapshots/not_a_snapshot_dir").createNewFile());
        assertThat(new File(dataDir0, "data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd").mkdirs());

        assertThat(new File(dataDir1, "ks3/table3/snapshots/snapshot1").mkdirs());

        // this is a different table with the same "table4" prefix
        assertThat(new File(dataDir1, "data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1" +
                                      "/snapshots/this_is_a_valid_snapshot_name_i_❤_u").mkdirs());

        // table && table-<TABLE_UUID>
        assertThat(new File(dataDir0, "ks1/a_table/snapshots/a_snapshot/").mkdirs());
        assertThat(new File(dataDir0, "ks1/a_table-a72c8740a57611ec935db766a70c44a1/snapshots/a_snapshot/").mkdirs());

        // create some files inside snapshot backup.2022-03-17-04-PDT
        assertThat(new File(dataDir0, "ks1/table1/snapshots/backup.2022-03-17-04-PDT/data.db").createNewFile());
        assertThat(new File(dataDir0, "ks1/table1/snapshots/backup.2022-03-17-04-PDT/index.db").createNewFile());
        assertThat(new File(dataDir0, "ks1/table1/snapshots/backup.2022-03-17-04-PDT/nb-203-big-TOC.txt")
                   .createNewFile());

        // create some files inside snapshot ea823202-a62c-4603-bb6a-4e15d79091cd
        assertThat(new File(dataDir0, "data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd/data.db")
                   .createNewFile());
        assertThat(new File(dataDir0, "data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd/index.db")
                   .createNewFile());
        assertThat(
        new File(dataDir0, "data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd/nb-203-big-TOC.txt")
        .createNewFile());

        // create some files inside snapshot snapshot1 in dataDir1
        assertThat(new File(dataDir1, "ks3/table3/snapshots/snapshot1/data.db").createNewFile());
        assertThat(new File(dataDir1, "ks3/table3/snapshots/snapshot1/index.db").createNewFile());
        assertThat(new File(dataDir1, "ks3/table3/snapshots/snapshot1/nb-203-big-TOC.txt").createNewFile());

        assertThat(new File(dataDir1, "data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1" +
                                      "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db").createNewFile());
        assertThat(new File(dataDir1, "data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1" +
                                      "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db").createNewFile());
        assertThat(new File(dataDir1, "data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1" +
                                      "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/nb-203-big-TOC.txt")
                   .createNewFile());

        vertx = Vertx.vertx();
        instance = initialize(vertx, mockInstancesConfig);
    }

    @AfterEach
    void clear()
    {
        assertThat(dataDir0.delete());
        assertThat(dataDir1.delete());
    }

    abstract SnapshotPathBuilder initialize(Vertx vertx, InstancesConfig instancesConfig);

    @Test
    void failsWhenKeyspaceIsNull()
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest(null, "table",
                                                                                  "snapshot", "component")))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("keyspace must not be null");
    }

    @Test
    void failsWhenKeyspaceContainsInvalidCharacters()
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest("i_❤_u", "table",
                                                                                  "snapshot", "component")))
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
        .returns("Invalid characters in keyspace: ../../../etc/passwd", from(t -> ((HttpException) t)
                                                                                  .getPayload()));
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
        .returns("Invalid characters in table name: ../../../etc/passwd", from(t -> ((HttpException) t)
                                                                                    .getPayload()));
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
        .returns("Invalid characters in snapshot name: " + invalidFileName, from(t -> ((HttpException) t)
                                                                                      .getPayload()));
    }

    @Test
    void failsWhenComponentNameIsNull()
    {
        assertThatThrownBy(() -> instance.build("localhost",
                                                new StreamSSTableComponentRequest("ks",
                                                                                  "table",
                                                                                  "snapshot",
                                                                                  null)))
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

    @Test
    void failsWhenDataDirsAreEmpty() throws InterruptedException
    {
        failsWithFileNotFoundException(instance.build("emptyDataDirInstance",
                                                      new StreamSSTableComponentRequest("ks",
                                                                                        "table",
                                                                                        "snapshot",
                                                                                        "component.db")),
                                       "No data directories are available for host 'emptyDataDirInstance'");
    }

    @Test
    void failsWhenInvalidDataDirectory() throws InterruptedException
    {
        failsWithFileNotFoundException(instance.build("invalidDataDirInstance",
                                                      new StreamSSTableComponentRequest("ks",
                                                                                        "table",
                                                                                        "snapshot",
                                                                                        "component.db")),
                                       "Keyspace 'ks' does not exist");
    }

    @Test
    void failsWhenKeyspaceDirectoryDoesNotExist() throws InterruptedException
    {
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("non_existent",
                                                                                        "table",
                                                                                        "snapshot",
                                                                                        "component.db")),
                                       "Keyspace 'non_existent' does not exist");
    }

    @Test
    void failsWhenKeyspaceIsNotADirectory() throws InterruptedException
    {
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("not_a_keyspace_dir",
                                                                                        "table",
                                                                                        "snapshot",
                                                                                        "component.db")),
                                       "Keyspace 'not_a_keyspace_dir' does not exist");
    }

    @Test
    void failsWhenTableDoesNotExist() throws InterruptedException
    {
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("ks1",
                                                                                        "non_existent",
                                                                                        "snapshot",
                                                                                        "component.db")),
                                       "Table 'non_existent' does not exist");
    }

    @Test
    void failsWhenTableDoesNotExistWithSimilarPrefix() throws InterruptedException
    {
        // In this scenario, we have other tables with the "table" prefix (i.e table4)
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("ks1",
                                                                                        "table",
                                                                                        "snapshot",
                                                                                        "component.db")),
                                       "Table 'table' does not exist");
    }

    @Test
    void failsWhenTableNameIsNotADirectory() throws InterruptedException
    {
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("ks1",
                                                                                        "not_a_table_dir",
                                                                                        "snapshot",
                                                                                        "component.db")),
                                       "Table 'not_a_table_dir' does not exist");
    }

    @Test
    void failsWhenSnapshotDirectoryDoesNotExist() throws InterruptedException
    {
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("ks1",
                                                                                        "table1",
                                                                                        "non_existent",
                                                                                        "component.db")),
                                       "Component 'component.db' does not exist for snapshot 'non_existent'");
    }

    @Test
    void failsWhenSnapshotIsNotADirectory() throws InterruptedException
    {
        failsWithFileNotFoundException(instance.build("localhost",
                                                      new StreamSSTableComponentRequest("ks1",
                                                                                        "table1",
                                                                                        "not_a_snapshot_dir",
                                                                                        "component.db")),
                                       "Component 'component.db' does not exist for snapshot 'not_a_snapshot_dir'");
    }

    @Test
    void failsWhenComponentFileDoesNotExist() throws InterruptedException
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
    void failsWhenComponentIsNotAFile() throws InterruptedException
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
    void succeedsWhenComponentExists() throws Exception
    {
        String expectedPath;
        expectedPath = dataDir0.getAbsolutePath() + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT/data.db";
        succeedsWhenComponentExists(instance.build("localhost",
                                                   new StreamSSTableComponentRequest("ks1",
                                                                                     "table1",
                                                                                     "backup.2022-03-17-04-PDT",
                                                                                     "data.db")),
                                    expectedPath);
        expectedPath = dataDir0.getAbsolutePath() + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT/index.db";
        succeedsWhenComponentExists(instance.build("localhost",
                                                   new StreamSSTableComponentRequest("ks1",
                                                                                     "table1",
                                                                                     "backup.2022-03-17-04-PDT",
                                                                                     "index.db")),
                                    expectedPath);
        expectedPath = dataDir0.getAbsolutePath() + "/ks1/table1/snapshots/backup.2022-03-17-04-PDT/nb-203-big-TOC.txt";
        succeedsWhenComponentExists(instance.build("localhost",
                                                   new StreamSSTableComponentRequest("ks1",
                                                                                     "table1",
                                                                                     "backup.2022-03-17-04-PDT",
                                                                                     "nb-203-big-TOC.txt")),
                                    expectedPath);
        expectedPath = dataDir0.getAbsolutePath()
                       + "/data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd/data.db";
        succeedsWhenComponentExists(instance
                                    .build("localhost",
                                           new StreamSSTableComponentRequest("ks2",
                                                                             "table2",
                                                                             "ea823202-a62c-4603-bb6a-4e15d79091cd",
                                                                             "data.db")),
                                    expectedPath);
        expectedPath = dataDir0.getAbsolutePath()
                       + "/data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd/index.db";
        succeedsWhenComponentExists(instance
                                    .build("localhost",
                                           new StreamSSTableComponentRequest("ks2",
                                                                             "table2",
                                                                             "ea823202-a62c-4603-bb6a-4e15d79091cd",
                                                                             "index.db")),
                                    expectedPath);
        expectedPath = dataDir0.getAbsolutePath()
                       + "/data/ks2/table2/snapshots/ea823202-a62c-4603-bb6a-4e15d79091cd/nb-203-big-TOC.txt";
        succeedsWhenComponentExists(instance
                                    .build("localhost",
                                           new StreamSSTableComponentRequest("ks2",
                                                                             "table2",
                                                                             "ea823202-a62c-4603-bb6a-4e15d79091cd",
                                                                             "nb-203-big-TOC.txt")),
                                    expectedPath);
        expectedPath = dataDir1.getAbsolutePath() + "/ks3/table3/snapshots/snapshot1/data.db";
        succeedsWhenComponentExists(instance.build("localhost",
                                                   new StreamSSTableComponentRequest("ks3",
                                                                                     "table3",
                                                                                     "snapshot1",
                                                                                     "data.db")),
                                    expectedPath);
        expectedPath = dataDir1.getAbsolutePath() + "/ks3/table3/snapshots/snapshot1/index.db";
        succeedsWhenComponentExists(instance.build("localhost",
                                                   new StreamSSTableComponentRequest("ks3",
                                                                                     "table3",
                                                                                     "snapshot1",
                                                                                     "index.db")),
                                    expectedPath);
        expectedPath = dataDir1.getAbsolutePath() + "/ks3/table3/snapshots/snapshot1/nb-203-big-TOC.txt";
        succeedsWhenComponentExists(instance.build("localhost",
                                                   new StreamSSTableComponentRequest("ks3",
                                                                                     "table3",
                                                                                     "snapshot1",
                                                                                     "nb-203-big-TOC.txt")),
                                    expectedPath);



        // table table4 shares the prefix with table table4abc
        expectedPath = dataDir1.getAbsolutePath()
                       + "/data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1"
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db";
        succeedsWhenComponentExists(instance
                                    .build("localhost",
                                           new StreamSSTableComponentRequest("ks4",
                                                                             "table4abc",
                                                                             "this_is_a_valid_snapshot_name_i_❤_u",
                                                                             "data.db")),
                                    expectedPath);
        expectedPath = dataDir1.getAbsolutePath()
                       + "/data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1"
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db";
        succeedsWhenComponentExists(instance
                                    .build("localhost",
                                           new StreamSSTableComponentRequest("ks4",
                                                                             "table4abc",
                                                                             "this_is_a_valid_snapshot_name_i_❤_u",
                                                                             "index.db")),
                                    expectedPath);
        expectedPath = dataDir1.getAbsolutePath()
                       + "/data/ks4/table4abc-a72c8740a57611ec935db766a70c44a1"
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/nb-203-big-TOC.txt";
        succeedsWhenComponentExists(instance
                                    .build("localhost",
                                           new StreamSSTableComponentRequest("ks4",
                                                                             "table4abc",
                                                                             "this_is_a_valid_snapshot_name_i_❤_u",
                                                                             "nb-203-big-TOC.txt")),
                                    expectedPath);


    }

    @Test
    void testTableWithUUIDPicked() throws IOException, InterruptedException
    {
        TemporaryFolder tempFolder = new TemporaryFolder();
        tempFolder.create();
        File dataDir = tempFolder.newFolder("data");

        InstancesConfig mockInstancesConfig = mock(InstancesConfig.class);
        InstanceMetadata mockInstanceMeta = mock(InstanceMetadata.class);

        when(mockInstancesConfig.instanceFromHost("localhost")).thenReturn(mockInstanceMeta);
        when(mockInstanceMeta.dataDirs()).thenReturn(Arrays.asList(dataDir.getAbsolutePath()));

        File atable = new File(dataDir, "data/ks1/a_table");
        assertThat(atable.mkdirs());
        File atableSnapshot = new File(atable, "snapshots/a_snapshot");
        assertThat(atableSnapshot.mkdirs());
        assertThat(new File(atable, "snapshots/a_snapshot/data.db").createNewFile());
        assertThat(new File(atable, "snapshots/a_snapshot/index.db").createNewFile());
        assertThat(new File(atable, "snapshots/a_snapshot/nb-203-big-TOC.txt").createNewFile());

        File atableWithUUID = new File(dataDir, "data/ks1/a_table-a72c8740a57611ec935db766a70c44a1");
        assertThat(atableWithUUID.mkdirs());
        File atableWithUUIDSnapshot = new File(atableWithUUID, "snapshots/a_snapshot");
        assertThat(atableWithUUIDSnapshot.mkdirs());

        assertThat(new File(atableWithUUID, "snapshots/a_snapshot/data.db").createNewFile());
        assertThat(new File(atableWithUUID, "snapshots/a_snapshot/index.db").createNewFile());
        assertThat(new File(atableWithUUID, "snapshots/a_snapshot/nb-203-big-TOC.txt").createNewFile());
        assertThat(atableWithUUID.setLastModified(System.currentTimeMillis() + 2000000));

        String expectedPath;
        // a_table and a_table-<TABLE_UUID> - the latter should be picked
        SnapshotPathBuilder newBuilder = new SnapshotPathBuilder(vertx.fileSystem(), mockInstancesConfig);
        expectedPath = atableWithUUID.getAbsolutePath() + "/snapshots/a_snapshot/data.db";
        succeedsWhenComponentExists(newBuilder
                                    .build("localhost",
                                           new StreamSSTableComponentRequest("ks1",
                                                                             "a_table",
                                                                             "a_snapshot",
                                                                             "data.db")),
                                    expectedPath);
        expectedPath = atableWithUUID.getAbsolutePath() + "/snapshots/a_snapshot/index.db";
        succeedsWhenComponentExists(newBuilder
                                    .build("localhost",
                                           new StreamSSTableComponentRequest("ks1",
                                                                             "a_table",
                                                                             "a_snapshot",
                                                                             "index.db")),
                                    expectedPath);
        expectedPath = atableWithUUID.getAbsolutePath() + "/snapshots/a_snapshot/nb-203-big-TOC.txt";
        succeedsWhenComponentExists(newBuilder
                                    .build("localhost",
                                           new StreamSSTableComponentRequest("ks1",
                                                                             "a_table",
                                                                             "a_snapshot",
                                                                             "nb-203-big-TOC.txt")),
                                    expectedPath);
    }

    @Test
    void testLastModifiedTablePicked() throws IOException, InterruptedException
    {
        TemporaryFolder tempFolder = new TemporaryFolder();
        tempFolder.create();
        File dataDir = tempFolder.newFolder("data");

        InstancesConfig mockInstancesConfig = mock(InstancesConfig.class);
        InstanceMetadata mockInstanceMeta = mock(InstanceMetadata.class);

        when(mockInstancesConfig.instanceFromHost("localhost")).thenReturn(mockInstanceMeta);
        when(mockInstanceMeta.dataDirs()).thenReturn(Arrays.asList(dataDir.getAbsolutePath()));

        File table4Old = new File(dataDir, "data/ks4/table4-a6442310a57611ec8b980b0b2009844e1");
        assertThat(table4Old.mkdirs());

        // table was dropped and recreated. The table gets a new uuid
        File table4OldSnapshot = new File(table4Old, "snapshots/this_is_a_valid_snapshot_name_i_❤_u");
        assertThat(table4OldSnapshot.mkdirs());
        // create some files inside snapshot this_is_a_valid_snapshot_name_i_❤_u in dataDir1
        assertThat(new File(table4Old, "snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db").createNewFile());
        assertThat(new File(table4Old, "snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db").createNewFile());
        assertThat(new File(table4Old, "snapshots/this_is_a_valid_snapshot_name_i_❤_u/nb-203-big-TOC.txt")
                   .createNewFile());

        File table4New = new File(dataDir, "data/ks4/table4-a72c8740a57611ec935db766a70c44a11");
        assertThat(table4New.mkdirs());

        File table4NewSnapshot = new File(table4New, "snapshots/this_is_a_valid_snapshot_name_i_❤_u");
        assertThat(table4NewSnapshot.mkdirs());

        assertThat(new File(table4New, "snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db").createNewFile());
        assertThat(new File(table4New, "snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db").createNewFile());
        assertThat(new File(table4New, "snapshots/this_is_a_valid_snapshot_name_i_❤_u/nb-203-big-TOC.txt")
                   .createNewFile());
        assertThat(table4New.setLastModified(System.currentTimeMillis() + 2000000));

        String expectedPath;
        SnapshotPathBuilder newBuilder = new SnapshotPathBuilder(vertx.fileSystem(), mockInstancesConfig);
        // table4-a72c8740a57611ec935db766a70c44a1 is the last modified, so it is the correct directory
        expectedPath = table4New.getAbsolutePath()
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/data.db";
        succeedsWhenComponentExists(newBuilder
                                    .build("localhost",
                                           new StreamSSTableComponentRequest("ks4",
                                                                             "table4",
                                                                             "this_is_a_valid_snapshot_name_i_❤_u",
                                                                             "data.db")),
                                    expectedPath);
        expectedPath = table4New.getAbsolutePath()
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/index.db";
        succeedsWhenComponentExists(newBuilder
                                    .build("localhost",
                                           new StreamSSTableComponentRequest("ks4",
                                                                             "table4",
                                                                             "this_is_a_valid_snapshot_name_i_❤_u",
                                                                             "index.db")),
                                    expectedPath);
        expectedPath = table4New.getAbsolutePath()
                       + "/snapshots/this_is_a_valid_snapshot_name_i_❤_u/nb-203-big-TOC.txt";
        succeedsWhenComponentExists(newBuilder
                                    .build("localhost",
                                           new StreamSSTableComponentRequest("ks4",
                                                                             "table4",
                                                                             "this_is_a_valid_snapshot_name_i_❤_u",
                                                                             "nb-203-big-TOC.txt")),
                                    expectedPath);
    }

    protected void succeedsWhenComponentExists(Future<String> future, String expectedPath)
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
