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

package org.apache.cassandra.sidecar.cluster.instance;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.codahale.metrics.MetricRegistry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class InstanceMetadataImplTest
{
    private static final int ID = 123;
    private static final String HOST = "testhost";
    private static final int PORT = 12345;
    private static final String DATA_DIR_1 = "test/data/data1";
    private static final String DATA_DIR_2 = "test/data/data2";
    private static final String CDC_DIR = "cdc_dir";
    private static final String STAGING_DIR = "staging_dir";
    private static final String COMMITLOG_DIR = "commitlog_dir";
    private static final String HINTS_DIR = "hints_dir";
    private static final String SAVED_CACHES_DIR = "saved_caches_dir";
    private static final String LOCAL_SYSTEM_DATA_FILE_DIR = "local_system_data_dir";
    private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    @TempDir
    Path tempDir;

    @Test
    void testConstructor()
    {
        String rootDir = tempDir.toString();

        InstanceMetadataImpl metadata = getInstanceMetadataBuilder(rootDir).build();

        assertThat(metadata.id()).isEqualTo(ID);
        assertThat(metadata.host()).isEqualTo(HOST);
        assertThat(metadata.port()).isEqualTo(PORT);
        assertThat(metadata.dataDirs()).contains(rootDir + "/" + DATA_DIR_1, rootDir + "/" + DATA_DIR_2);
        assertThat(metadata.cdcDir()).isEqualTo(rootDir + "/" + CDC_DIR);
        assertThat(metadata.stagingDir()).isEqualTo(rootDir + "/" + STAGING_DIR);
        assertThat(metadata.commitlogDir()).isEqualTo(rootDir + "/" + COMMITLOG_DIR);
        assertThat(metadata.hintsDir()).isEqualTo(rootDir + "/" + HINTS_DIR);
        assertThat(metadata.savedCachesDir()).isEqualTo(rootDir + "/" + SAVED_CACHES_DIR);
        assertThat(metadata.localSystemDataFileDir()).isEqualTo(rootDir + "/" + LOCAL_SYSTEM_DATA_FILE_DIR);
    }

    @Test
    void testConstructorWithHomeDirPaths()
    {
        String rootDir = "~";
        String homeDir = System.getProperty("user.home");

        InstanceMetadataImpl metadata = getInstanceMetadataBuilder(rootDir).build();

        assertThat(metadata.dataDirs()).contains(homeDir + "/" + DATA_DIR_1, homeDir + "/" + DATA_DIR_2);
        assertThat(metadata.cdcDir()).isEqualTo(homeDir + "/" + CDC_DIR);
        assertThat(metadata.stagingDir()).isEqualTo(homeDir + "/" + STAGING_DIR);
        assertThat(metadata.commitlogDir()).isEqualTo(homeDir + "/" + COMMITLOG_DIR);
        assertThat(metadata.hintsDir()).isEqualTo(homeDir + "/" + HINTS_DIR);
        assertThat(metadata.savedCachesDir()).isEqualTo(homeDir + "/" + SAVED_CACHES_DIR);
        assertThat(metadata.localSystemDataFileDir()).isEqualTo(homeDir + "/" + LOCAL_SYSTEM_DATA_FILE_DIR);
    }

    @Test
    void testConstructorWithStorageDir()
    {
        String rootDir = tempDir.toString();

        InstanceMetadataImpl metadata = InstanceMetadataImpl.builder()
                                                            .id(ID)
                                                            .host(HOST)
                                                            .port(PORT)
                                                            .metricRegistry(METRIC_REGISTRY)
                                                            .dataDirs(Collections.singletonList(rootDir + "/" + DATA_DIR_1))
                                                            .storageDir(rootDir)
                                                            .build();

        assertThat(metadata.dataDirs()).contains(rootDir + "/" + DATA_DIR_1);
        assertThat(metadata.cdcDir()).isEqualTo(rootDir + "/cdc_raw");
        assertThat(metadata.commitlogDir()).isEqualTo(rootDir + "/commitlog");
        assertThat(metadata.hintsDir()).isEqualTo(rootDir + "/hints");
        assertThat(metadata.savedCachesDir()).isEqualTo(rootDir + "/saved_caches");
        assertThat(metadata.stagingDir()).isNull();
        assertThat(metadata.localSystemDataFileDir()).isNull();
    }

    @Test
    void failsWhenDataDirsAreNotConfigured()
    {
        assertThatNullPointerException().isThrownBy(() -> InstanceMetadataImpl.builder()
                                                                              .id(ID)
                                                                              .host(HOST)
                                                                              .port(PORT)
                                                                              .metricRegistry(METRIC_REGISTRY)
                                                                              .build())
                                        .withMessageContaining("dataDirs are required when storageDir is not configured");
    }

    @Test
    void failsWhenDataDirsAreEmpty()
    {
        assertThatIllegalArgumentException().isThrownBy(() -> InstanceMetadataImpl.builder()
                                                                                  .id(ID)
                                                                                  .host(HOST)
                                                                                  .port(PORT)
                                                                                  .metricRegistry(METRIC_REGISTRY)
                                                                                  .dataDirs(Collections.emptyList())
                                                                                  .build())
                                            .withMessageContaining("dataDirs are required when storageDir is not configured");
    }

    @Test
    void failsWhenCommitLogDirIsNotConfigured()
    {
        assertThatNullPointerException().isThrownBy(() -> InstanceMetadataImpl.builder()
                                                                              .id(ID)
                                                                              .host(HOST)
                                                                              .port(PORT)
                                                                              .metricRegistry(METRIC_REGISTRY)
                                                                              .dataDirs(Collections.singletonList("/tmp/data"))
                                                                              .build())
                                        .withMessageContaining("commitlogDir is required when storageDir is not configured");
    }

    @Test
    void failsWhenHintsDirIsNotConfigured()
    {
        assertThatNullPointerException().isThrownBy(() -> InstanceMetadataImpl.builder()
                                                                              .id(ID)
                                                                              .host(HOST)
                                                                              .port(PORT)
                                                                              .metricRegistry(METRIC_REGISTRY)
                                                                              .dataDirs(Collections.singletonList("/tmp/data"))
                                                                              .commitlogDir("/tmp/commitlog")
                                                                              .build())
                                        .withMessageContaining("hintsDir is required when storageDir is not configured");
    }

    @Test
    void failsWhenSavedCachesDirIsNotConfigured()
    {
        assertThatNullPointerException().isThrownBy(() -> InstanceMetadataImpl.builder()
                                                                              .id(ID)
                                                                              .host(HOST)
                                                                              .port(PORT)
                                                                              .metricRegistry(METRIC_REGISTRY)
                                                                              .dataDirs(Collections.singletonList("/tmp/data"))
                                                                              .commitlogDir("/tmp/commitlog")
                                                                              .hintsDir("/tmp/hints")
                                                                              .build())
                                        .withMessageContaining("savedCachesDir is required when storageDir is not configured");
    }

    @Test
    void testResolvesDefaultDirectories()
    {
        String rootDir = tempDir.toString();
        InstanceMetadata metadata = InstanceMetadataImpl.builder()
                                                        .id(ID)
                                                        .host(HOST)
                                                        .port(PORT)
                                                        .metricRegistry(METRIC_REGISTRY)
                                                        .storageDir(rootDir)
                                                        .build();
        assertThat(metadata.dataDirs()).containsExactly(rootDir + "/data");
        assertThat(metadata.cdcDir()).isEqualTo(rootDir + "/cdc_raw");
        assertThat(metadata.commitlogDir()).isEqualTo(rootDir + "/commitlog");
        assertThat(metadata.hintsDir()).isEqualTo(rootDir + "/hints");
        assertThat(metadata.savedCachesDir()).isEqualTo(rootDir + "/saved_caches");
    }

    @Test
    void testCustomDirectories()
    {
        String rootDir = tempDir.toString();
        InstanceMetadata metadata = InstanceMetadataImpl.builder()
                                                        .id(ID)
                                                        .host(HOST)
                                                        .port(PORT)
                                                        .metricRegistry(METRIC_REGISTRY)
                                                        .storageDir(rootDir)
                                                        .cdcDir(rootDir + "/customcdcdir")
                                                        .build();
        assertThat(metadata.cdcDir()).isEqualTo(rootDir + "/customcdcdir");
        assertThat(metadata.dataDirs()).containsExactly(rootDir + "/data");
        assertThat(metadata.commitlogDir()).isEqualTo(rootDir + "/commitlog");
        assertThat(metadata.hintsDir()).isEqualTo(rootDir + "/hints");
        assertThat(metadata.savedCachesDir()).isEqualTo(rootDir + "/saved_caches");

        metadata = InstanceMetadataImpl.builder()
                                       .id(ID)
                                       .host(HOST)
                                       .port(PORT)
                                       .metricRegistry(METRIC_REGISTRY)
                                       .storageDir(rootDir)
                                       .hintsDir(rootDir + "/customhints")
                                       .build();
        assertThat(metadata.hintsDir()).isEqualTo(rootDir + "/customhints");
        assertThat(metadata.dataDirs()).containsExactly(rootDir + "/data");
        assertThat(metadata.cdcDir()).isEqualTo(rootDir + "/cdc_raw");
        assertThat(metadata.commitlogDir()).isEqualTo(rootDir + "/commitlog");
        assertThat(metadata.savedCachesDir()).isEqualTo(rootDir + "/saved_caches");

        metadata = InstanceMetadataImpl.builder()
                                       .id(ID)
                                       .host(HOST)
                                       .port(PORT)
                                       .metricRegistry(METRIC_REGISTRY)
                                       .storageDir(rootDir)
                                       .commitlogDir(rootDir + "/customcommitlog")
                                       .build();
        assertThat(metadata.hintsDir()).isEqualTo(rootDir + "/hints");
        assertThat(metadata.dataDirs()).containsExactly(rootDir + "/data");
        assertThat(metadata.cdcDir()).isEqualTo(rootDir + "/cdc_raw");
        assertThat(metadata.commitlogDir()).isEqualTo(rootDir + "/customcommitlog");
        assertThat(metadata.savedCachesDir()).isEqualTo(rootDir + "/saved_caches");

        metadata = InstanceMetadataImpl.builder()
                                       .id(ID)
                                       .host(HOST)
                                       .port(PORT)
                                       .metricRegistry(METRIC_REGISTRY)
                                       .storageDir(rootDir)
                                       .savedCachesDir(rootDir + "/customsaved")
                                       .build();
        assertThat(metadata.hintsDir()).isEqualTo(rootDir + "/hints");
        assertThat(metadata.dataDirs()).containsExactly(rootDir + "/data");
        assertThat(metadata.cdcDir()).isEqualTo(rootDir + "/cdc_raw");
        assertThat(metadata.commitlogDir()).isEqualTo(rootDir + "/commitlog");
        assertThat(metadata.savedCachesDir()).isEqualTo(rootDir + "/customsaved");

        metadata = InstanceMetadataImpl.builder()
                                       .id(ID)
                                       .host(HOST)
                                       .port(PORT)
                                       .metricRegistry(METRIC_REGISTRY)
                                       .storageDir(rootDir)
                                       .dataDirs(Arrays.asList("/tmp/data/dir_1", "/tmp/data/dir_3"))
                                       .build();
        assertThat(metadata.hintsDir()).isEqualTo(rootDir + "/hints");
        assertThat(metadata.dataDirs()).containsExactly("/tmp/data/dir_1", "/tmp/data/dir_3");
        assertThat(metadata.cdcDir()).isEqualTo(rootDir + "/cdc_raw");
        assertThat(metadata.commitlogDir()).isEqualTo(rootDir + "/commitlog");
        assertThat(metadata.savedCachesDir()).isEqualTo(rootDir + "/saved_caches");
    }

    InstanceMetadataImpl.Builder getInstanceMetadataBuilder(String rootDir)
    {
        List<String> dataDirs = new ArrayList<>();
        dataDirs.add(rootDir + "/" + DATA_DIR_1);
        dataDirs.add(rootDir + "/" + DATA_DIR_2);

        return InstanceMetadataImpl.builder()
                                   .id(ID)
                                   .host(HOST)
                                   .port(PORT)
                                   .dataDirs(dataDirs)
                                   .cdcDir(rootDir + "/" + CDC_DIR)
                                   .stagingDir(rootDir + "/" + STAGING_DIR)
                                   .commitlogDir(rootDir + "/" + COMMITLOG_DIR)
                                   .hintsDir(rootDir + "/" + HINTS_DIR)
                                   .savedCachesDir(rootDir + "/" + SAVED_CACHES_DIR)
                                   .localSystemDataFileDir(rootDir + "/" + LOCAL_SYSTEM_DATA_FILE_DIR)
                                   .metricRegistry(METRIC_REGISTRY);
    }
}
