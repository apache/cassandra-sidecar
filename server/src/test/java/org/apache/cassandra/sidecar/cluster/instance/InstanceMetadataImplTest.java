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

import static org.junit.jupiter.api.Assertions.assertEquals;

class InstanceMetadataImplTest
{

    @TempDir
    Path tempDir;


    @Test
    void testConstructor()
    {
        int id = 123;
        String host = "testhost";
        int port = 12345;
        String rootDir = tempDir.toString();
        List<String> dataDirs = new ArrayList<>();
        dataDirs.add(rootDir + "/test/data/data1");
        dataDirs.add(rootDir + "/test/data/data2");
        String cdcDir = rootDir + "/cdc_dir";
        String stagingDir = rootDir + "/staging_dir";
        MetricRegistry metricRegistry = new MetricRegistry();

        InstanceMetadataImpl metadata = InstanceMetadataImpl.builder()
                                                            .id(id)
                                                            .host(host)
                                                            .port(port)
                                                            .dataDirs(dataDirs)
                                                            .cdcDir(cdcDir)
                                                            .stagingDir(stagingDir)
                                                            .metricRegistry(metricRegistry)
                                                            .build();

        assertEquals(id, metadata.id());
        assertEquals(host, metadata.host());
        assertEquals(port, metadata.port());
        assertEquals(dataDirs, metadata.dataDirs());
        assertEquals(cdcDir, metadata.cdcDir());
        assertEquals(stagingDir, metadata.stagingDir());
    }

    @Test
    void testConstructorWithHomeDirPaths()
    {
        int id = 123;
        String host = "testhost";
        int port = 12345;
        String rootDir = "~";
        String dataDir1 = "test/data/data1";
        String dataDir2 = "test/data/data2";
        List<String> dataDirs = new ArrayList<>();
        dataDirs.add(rootDir + "/" + dataDir1);
        dataDirs.add(rootDir + "/" + dataDir2);
        String cdcDir = "cdc_dir";
        String stagingDir = "staging_dir";
        MetricRegistry metricRegistry = new MetricRegistry();

        String homeDir = System.getProperty("user.home");

        InstanceMetadataImpl metadata = InstanceMetadataImpl.builder()
                                                            .id(id)
                                                            .host(host)
                                                            .port(port)
                                                            .dataDirs(dataDirs)
                                                            .cdcDir(rootDir + "/" + cdcDir)
                                                            .stagingDir(rootDir + "/" + stagingDir)
                                                            .metricRegistry(metricRegistry)
                                                            .build();

        List<String> expectedDataDirs = new ArrayList<>();
        expectedDataDirs.add(homeDir + "/" + dataDir1);
        expectedDataDirs.add(homeDir + "/" + dataDir2);
        assertEquals(expectedDataDirs, metadata.dataDirs());
        assertEquals(homeDir + "/" + cdcDir, metadata.cdcDir());
        assertEquals(homeDir + "/" + stagingDir, metadata.stagingDir());
    }
}
