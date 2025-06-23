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

package org.apache.cassandra.sidecar.livemigration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.InstanceFileInfo;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;

import static org.apache.cassandra.sidecar.common.response.InstanceFileInfo.FileType.DIRECTORY;
import static org.apache.cassandra.sidecar.livemigration.InstanceFileInfoTestUtil.findInstanceFileInfo;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.LIVE_MIGRATION_CDC_RAW_DIR_PATH;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.LIVE_MIGRATION_COMMITLOG_DIR_PATH;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.LIVE_MIGRATION_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.LIVE_MIGRATION_HINTS_DIR_PATH;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.LIVE_MIGRATION_SAVED_CACHES_DIR_PATH;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.createFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CassandraInstanceFilesImplTest
{
    private static final String DATA_DIR_1 = "data1";
    private static final String CDC_DIR = "cdc_raw";
    private static final String COMMITLOG_DIR = "commitlog";
    private static final String HINTS_DIR = "hints";
    private static final String SAVED_CACHES_DIR = "saved_caches";
    private static final String LOCAL_SYSTEM_DATA_FILES_DIR = "local_system";
    private static final String STAGING_DIR = "sstable-staging";

    @TempDir
    Path tempDir;

    @Test
    public void testFiles() throws IOException
    {
        String cassandraHomeDir = tempDir.resolve("testGetFiles").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        LiveMigrationConfiguration liveMigrationConfig = mock(LiveMigrationConfiguration.class);
        when(liveMigrationConfig.filesToExclude()).thenReturn(Collections.emptySet());
        when(liveMigrationConfig.directoriesToExclude()).thenReturn(Collections.emptySet());

        String content = "abcdefg";
        createFile(content, instanceMetadata.dataDirs().get(0), "ks1", "t1", "file1.db");
        createFile(content, instanceMetadata.cdcDir(), "cdc_raw1.log");
        createFile(content, instanceMetadata.commitlogDir(), "commitlog0");
        createFile(content, instanceMetadata.hintsDir(), "hints1");
        createFile(content, instanceMetadata.savedCachesDir(), "cache1.db");
        createFile(content, instanceMetadata.localSystemDataFileDir(), "system", "local", "data1.db");
        createFile(content, instanceMetadata.stagingDir(), "stage-data.db");

        CassandraInstanceFilesImpl instanceFiles = new CassandraInstanceFilesImpl(instanceMetadata, liveMigrationConfig);
        List<InstanceFileInfo> instanceFileInfoList = instanceFiles.files();

        assertThat(instanceFileInfoList).isNotEmpty();
        InstanceFileInfo ks1FileInfo = findInstanceFileInfo(instanceFileInfoList,
                                                            LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks1");
        assertThat(ks1FileInfo).isNotNull();
        assertThat(ks1FileInfo.fileType).isEqualTo(DIRECTORY);

        InstanceFileInfo ks1T1FileInfo = findInstanceFileInfo(instanceFileInfoList,
                                                              LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks1/t1");
        assertThat(ks1T1FileInfo).isNotNull();
        assertThat(ks1T1FileInfo.fileType).isEqualTo(DIRECTORY);

        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks1/t1/file1.db")).isNotNull();
        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_CDC_RAW_DIR_PATH + "/0/cdc_raw1.log")).isNotNull();
        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_COMMITLOG_DIR_PATH + "/0/commitlog0")).isNotNull();
        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_HINTS_DIR_PATH + "/0/hints1")).isNotNull();
        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_SAVED_CACHES_DIR_PATH + "/0/cache1.db")).isNotNull();
        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH
                                                              + "/0/system/local/data1.db")).isNotNull();
    }

    @Test
    public void testFilesNoExclusionsSpecified() throws IOException
    {
        String cassandraHomeDir = tempDir.resolve("testGetFilesNoExclusionsSpecified").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        LiveMigrationConfiguration liveMigrationConfig = mock(LiveMigrationConfiguration.class);
        when(liveMigrationConfig.filesToExclude()).thenReturn(null);
        when(liveMigrationConfig.directoriesToExclude()).thenReturn(null);

        String content = "abcdefg";
        createFile(content, instanceMetadata.dataDirs().get(0), "ks1", "t1", "file1.db");

        CassandraInstanceFilesImpl instanceFiles = new CassandraInstanceFilesImpl(instanceMetadata, liveMigrationConfig);
        List<InstanceFileInfo> instanceFileInfoList = instanceFiles.files();

        assertThat(instanceFileInfoList).isNotEmpty();
        InstanceFileInfo ks1FileInfo = findInstanceFileInfo(instanceFileInfoList,
                                                            LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks1");
        assertThat(ks1FileInfo).isNotNull();
        assertThat(ks1FileInfo.fileType).isEqualTo(DIRECTORY);

        InstanceFileInfo ks1T1FileInfo = findInstanceFileInfo(instanceFileInfoList,
                                                              LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks1/t1");
        assertThat(ks1T1FileInfo).isNotNull();
        assertThat(ks1T1FileInfo.fileType).isEqualTo(DIRECTORY);

        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks1/t1/file1.db")).isNotNull();
    }

    @Test
    public void testFilesAndDirsExcluded() throws IOException
    {
        String cassandraHomeDir = tempDir.resolve("testGetFilesWithFilesAndDirsExcluded").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);

        Set<String> dirsToExclude = new HashSet<>();
        Set<String> filesToExclude = new HashSet<>();

        LiveMigrationConfiguration liveMigrationConfig = mock(LiveMigrationConfiguration.class);
        when(liveMigrationConfig.filesToExclude()).thenReturn(filesToExclude);
        when(liveMigrationConfig.directoriesToExclude()).thenReturn(dirsToExclude);

        String content = "abcdefg";
        createFile(content, instanceMetadata.dataDirs().get(0), "ks1", "t1", "file1.db");
        createFile(content, instanceMetadata.cdcDir(), "cdc_raw1.log");
        createFile(content, instanceMetadata.commitlogDir(), "commitlog0");
        createFile(content, instanceMetadata.hintsDir(), "hints1");
        createFile(content, instanceMetadata.savedCachesDir(), "cache1.db");
        createFile(content, instanceMetadata.localSystemDataFileDir(), "system", "local", "data1.db");
        createFile(content, instanceMetadata.stagingDir(), "stage-data.db");

        dirsToExclude.add("glob:" + instanceMetadata.hintsDir()); // dir excluded without placeholder
        dirsToExclude.add("glob:${COMMITLOG_DIR}"); // dir excluded using placeholder

        filesToExclude.add("glob:" + instanceMetadata.dataDirs().get(0) + "/ks1/t1/file1.db");
        filesToExclude.add("glob:${CDC_RAW_DIR}" + "/*.log");


        CassandraInstanceFilesImpl instanceFiles = new CassandraInstanceFilesImpl(instanceMetadata, liveMigrationConfig);
        List<InstanceFileInfo> instanceFileInfoList = instanceFiles.files();

        assertThat(instanceFileInfoList).isNotEmpty();
        InstanceFileInfo ks1FileInfo = findInstanceFileInfo(instanceFileInfoList,
                                                            LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks1");
        assertThat(ks1FileInfo).isNotNull();
        assertThat(ks1FileInfo.fileType).isEqualTo(DIRECTORY);

        InstanceFileInfo ks1T1FileInfo = findInstanceFileInfo(instanceFileInfoList,
                                                              LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks1/t1");
        assertThat(ks1T1FileInfo).isNotNull();
        assertThat(ks1T1FileInfo.fileType).isEqualTo(DIRECTORY);

        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks1/t1/file1.db")).isNull();
        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_CDC_RAW_DIR_PATH + "/0/cdc_raw1.log")).isNull();
        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_COMMITLOG_DIR_PATH)).isNull();
        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_COMMITLOG_DIR_PATH + "/0")).isNull();
        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_HINTS_DIR_PATH)).isNull();
        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_HINTS_DIR_PATH + "/0")).isNull();
        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_SAVED_CACHES_DIR_PATH + "/0/cache1.db")).isNotNull();
        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH
                                                              + "/0/system/local/data1.db")).isNotNull();
    }

    @Test
    public void testFilesWhenFewDirectoriesDoesNotExist() throws IOException
    {
        //cdc_dir, saved_caches_dir & local_system_data_files_dir not present
        String cassandraHomeDir = tempDir.resolve("testGetFiles").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        when(instanceMetadata.cdcDir()).thenReturn(null); // cdc dir is null
        when(instanceMetadata.savedCachesDir()).thenReturn(null); // saved caches dir is null
        when(instanceMetadata.localSystemDataFileDir()).thenReturn(null);
        LiveMigrationConfiguration liveMigrationConfig = mock(LiveMigrationConfiguration.class);
        when(liveMigrationConfig.filesToExclude()).thenReturn(Collections.emptySet());
        when(liveMigrationConfig.directoriesToExclude()).thenReturn(Collections.emptySet());

        String content = "abcdefg";
        createFile(content, instanceMetadata.dataDirs().get(0), "ks1", "t1", "file1.db");

        CassandraInstanceFilesImpl instanceFiles = new CassandraInstanceFilesImpl(instanceMetadata, liveMigrationConfig);
        List<InstanceFileInfo> instanceFileInfoList = instanceFiles.files();
        assertThat(instanceFileInfoList).isNotEmpty();

        InstanceFileInfo ks1FileInfo = findInstanceFileInfo(instanceFileInfoList,
                                                            LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks1");
        assertThat(ks1FileInfo).isNotNull();
        assertThat(ks1FileInfo.fileType).isEqualTo(DIRECTORY);

        InstanceFileInfo ks1T1FileInfo = findInstanceFileInfo(instanceFileInfoList,
                                                              LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks1/t1");
        assertThat(ks1T1FileInfo).isNotNull();
        assertThat(ks1T1FileInfo.fileType).isEqualTo(DIRECTORY);

        assertThat(findInstanceFileInfo(instanceFileInfoList, LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/ks1/t1/file1.db")).isNotNull();
    }


    InstanceMetadata getInstanceMetadata(String cassandraHomeDir)
    {
        InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
        List<String> dataDirs = new ArrayList<>(1);
        dataDirs.add(cassandraHomeDir + "/" + DATA_DIR_1);
        when(instanceMetadata.dataDirs()).thenReturn(dataDirs);
        when(instanceMetadata.cdcDir()).thenReturn(cassandraHomeDir + "/" + CDC_DIR);
        when(instanceMetadata.commitlogDir()).thenReturn(cassandraHomeDir + "/" + COMMITLOG_DIR);
        when(instanceMetadata.hintsDir()).thenReturn(cassandraHomeDir + "/" + HINTS_DIR);
        when(instanceMetadata.savedCachesDir()).thenReturn(cassandraHomeDir + "/" + SAVED_CACHES_DIR);
        when(instanceMetadata.localSystemDataFileDir())
        .thenReturn(cassandraHomeDir + "/" + LOCAL_SYSTEM_DATA_FILES_DIR);
        when(instanceMetadata.stagingDir()).thenReturn(cassandraHomeDir + "/" + STAGING_DIR);

        return instanceMetadata;
    }
}
