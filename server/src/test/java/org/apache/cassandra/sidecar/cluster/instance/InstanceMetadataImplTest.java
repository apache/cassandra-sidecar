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
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.codahale.metrics.MetricRegistry;

import static org.assertj.core.api.Assertions.assertThat;

class InstanceMetadataImplTest
{

    private static final int ID = 123;
    private static final String HOST = "testhost";
    private static final int PORT = 12345;
    private static final String DATA_DIR_1 = "test/data/data1";
    private static final String DATA_DIR_2 = "test/data/data2";
    private static final String CDC_DIR = "cdc_dir";
    private static final String STAGING_DIR = "staging_dir";
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
                                   .metricRegistry(METRIC_REGISTRY);
    }
}
