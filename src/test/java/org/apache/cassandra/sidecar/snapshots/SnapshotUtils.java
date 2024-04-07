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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.CQLSessionProviderImpl;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.InstancesConfigImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.MockCassandraFactory;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.utils.DriverUtils;
import org.apache.cassandra.sidecar.metrics.MetricFilter;
import org.apache.cassandra.sidecar.metrics.MetricRegistryProvider;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Utilities for testing snapshot related features
 */
public class SnapshotUtils
{
    public static final String STAGING_DIR = "staging";
    public static final List<MetricFilter> INCLUDE_ALL = Collections.singletonList(new MetricFilter.Regex(".*"));
    public static final MetricRegistryProvider METRIC_REGISTRY_PROVIDER = new MetricRegistryProvider("cassandra_sidecar",
                                                                                                     INCLUDE_ALL,
                                                                                                     Collections.emptyList());

    public static String makeStagingDir(String rootPath)
    {
        return rootPath + File.separatorChar + STAGING_DIR;
    }

    public static void initializeTmpDirectory(File temporaryFolder) throws IOException
    {
        for (final String[] folderPath : mockSnapshotDirectories())
        {
            assertThat(new File(temporaryFolder, String.join("/", folderPath)).mkdirs()).isTrue();
        }
        for (final String[] folderPath : mockNonSnapshotDirectories())
        {
            assertThat(new File(temporaryFolder, String.join("/", folderPath)).mkdirs()).isTrue();
        }
        for (final String fileName : mockSnapshotFiles())
        {
            File snapshotFile = new File(temporaryFolder, fileName);
            Files.write(snapshotFile.toPath(), "hello world".getBytes(StandardCharsets.UTF_8));
        }
        for (final String fileName : mockNonSnapshotFiles())
        {
            File nonSnapshotFile = new File(temporaryFolder, fileName);
            Files.write(nonSnapshotFile.toPath(), new byte[0]);
        }
        // adding secondary index files
        assertThat(new File(temporaryFolder, "d1/data/keyspace1/table1-1234/snapshots/snapshot1/.index/")
                   .mkdirs()).isTrue();
        Path path = temporaryFolder.toPath().resolve("d1")
                                   .resolve("data")
                                   .resolve("keyspace1")
                                   .resolve("table1-1234")
                                   .resolve("snapshots")
                                   .resolve("snapshot1")
                                   .resolve(".index")
                                   .resolve("secondary.db");
        Files.write(path, new byte[0]);

        assertThat(new File(temporaryFolder, "d1/data/keyspace1/table1-1234")
                   .setLastModified(System.currentTimeMillis() + 2_000_000)).isTrue();
    }

    public static InstancesConfig mockInstancesConfig(Vertx vertx, String rootPath)
    {
        CQLSessionProvider mockSession1 = mock(CQLSessionProviderImpl.class);
        return mockInstancesConfig(vertx, rootPath, null, mockSession1);
    }

    public static InstancesConfig mockInstancesConfig(Vertx vertx,
                                                      String rootPath,
                                                      CassandraAdapterDelegate delegate,
                                                      CQLSessionProvider cqlSessionProvider1)
    {
        CassandraVersionProvider.Builder versionProviderBuilder = new CassandraVersionProvider.Builder();
        versionProviderBuilder.add(new MockCassandraFactory());
        CassandraVersionProvider versionProvider = versionProviderBuilder.build();
        String stagingDir = makeStagingDir(rootPath);

        if (delegate == null)
        {
            JmxClient mockJmxClient = mock(JmxClient.class);
            delegate = new CassandraAdapterDelegate(vertx, 1, versionProvider, cqlSessionProvider1, mockJmxClient,
                                                    new DriverUtils(), null, "localhost1", 9042);
        }

        InstanceMetadataImpl localhost = InstanceMetadataImpl.builder()
                                                             .id(1)
                                                             .host("localhost")
                                                             .port(9043)
                                                             .dataDirs(Collections.singletonList(rootPath + "/d1"))
                                                             .stagingDir(stagingDir)
                                                             .delegate(delegate)
                                                             .metricRegistry(METRIC_REGISTRY_PROVIDER.registry(1))
                                                             .build();
        InstanceMetadataImpl localhost2 = InstanceMetadataImpl.builder()
                                                              .id(2)
                                                              .host("localhost2")
                                                              .port(9043)
                                                              .dataDirs(Collections.singletonList(rootPath + "/d2"))
                                                              .stagingDir(stagingDir)
                                                              .delegate(delegate)
                                                              .metricRegistry(METRIC_REGISTRY_PROVIDER.registry(2))
                                                              .build();
        List<InstanceMetadata> instanceMetas = Arrays.asList(localhost, localhost2);
        return new InstancesConfigImpl(instanceMetas, DnsResolver.DEFAULT);
    }

    public static List<String[]> mockSnapshotDirectories()
    {
        return Arrays.asList(new String[]{ "d1", "data", "keyspace1", "table1-1234", "snapshots", "snapshot1" },
                             new String[]{ "d1", "data", "keyspace1", "table1-1234", "snapshots", "snapshot2" },
                             new String[]{ "d1", "data", "keyspace1", "table2-1234", "snapshots", "snapshot1" },
                             new String[]{ "d1", "data", "keyspace1", "table2-1234", "snapshots", "snapshot2" },
                             new String[]{ "d2", "data", "keyspace1", "table1-1234", "snapshots", "snapshot1" },
                             new String[]{ "d2", "data", "keyspace1", "table1-1234", "snapshots", "snapshot2" },
                             new String[]{ "d2", "data", "keyspace1", "table2-1234", "snapshots", "snapshot1" },
                             new String[]{ "d2", "data", "keyspace1", "table2-1234", "snapshots", "snapshot2" });
    }

    public static List<String[]> mockNonSnapshotDirectories()
    {
        return Arrays.asList(new String[]{ "d1", "data", "keyspace1", "table1", "nonsnapshots", "snapshot1" },
                             new String[]{ "d1", "data", "keyspace1", "table2", "nonsnapshots", "snapshot1" },
                             new String[]{ "d2", "data", "keyspace1", "table1", "nonsnapshots", "snapshot1" },
                             new String[]{ "d2", "data", "keyspace1", "table2", "nonsnapshots", "snapshot1" });
    }

    public static List<String> mockSnapshotFiles()
    {
        List<String> snapshotFiles = new ArrayList<>();
        snapshotFiles.addAll(snapshot1Files());
        snapshotFiles.addAll(snapshot2Files());
        Collections.sort(snapshotFiles);
        return snapshotFiles;
    }

    public static List<String> snapshot1Files()
    {
        final List<String> snapshotFiles = new ArrayList<>();
        snapshotFiles.addAll(snapshot1Instance1Files());
        snapshotFiles.addAll(snapshot1Instance2Files());
        Collections.sort(snapshotFiles);
        return snapshotFiles;
    }

    public static List<String> snapshot1Instance1Files()
    {
        return Arrays.asList("d1/data/keyspace1/table1-1234/snapshots/snapshot1/1.db",
                             "d1/data/keyspace1/table2-1234/snapshots/snapshot1/3.db");
    }

    public static List<String> snapshot1Instance2Files()
    {
        return Arrays.asList("d2/data/keyspace1/table1-1234/snapshots/snapshot1/5.db",
                             "d2/data/keyspace1/table2-1234/snapshots/snapshot1/7.db");
    }

    public static List<String> snapshot2Files()
    {
        List<String> snapshotFiles = Arrays.asList("d1/data/keyspace1/table1-1234/snapshots/snapshot2/2.db",
                                                   "d1/data/keyspace1/table2-1234/snapshots/snapshot2/4.db",
                                                   "d2/data/keyspace1/table1-1234/snapshots/snapshot2/6.db",
                                                   "d2/data/keyspace1/table2-1234/snapshots/snapshot2/8.db");
        Collections.sort(snapshotFiles);
        return snapshotFiles;
    }

    public static List<String> mockNonSnapshotFiles()
    {
        List<String> nonSnapshotFiles = Arrays.asList("d1/data/keyspace1/table1/11.db",
                                                      "d1/data/keyspace1/table2/12.db",
                                                      "d2/data/keyspace1/table1/13.db",
                                                      "d2/data/keyspace1/table2/14.db");
        Collections.sort(nonSnapshotFiles);
        return nonSnapshotFiles;
    }
}
