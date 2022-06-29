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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;

import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.InstancesConfigImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.MockCassandraFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Utilities for testing snapshot related features
 */
public class SnapshotUtils
{
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void initializeTmpDirectory(File temporaryFolder) throws IOException
    {
        for (final String[] folderPath : getMockSnapshotDirectories())
        {
            assertThat(new File(temporaryFolder, String.join("/", folderPath)).mkdirs()).isTrue();
        }
        for (final String[] folderPath : getMockNonSnapshotDirectories())
        {
            assertThat(new File(temporaryFolder, String.join("/", folderPath)).mkdirs()).isTrue();
        }
        for (final String fileName : getMockSnapshotFiles())
        {
            File snapshotFile = new File(temporaryFolder, fileName);
            FileUtils.writeStringToFile(snapshotFile, "hello world", Charset.defaultCharset());
        }
        for (final String fileName : getMockNonSnapshotFiles())
        {
            File nonSnapshotFile = new File(temporaryFolder, fileName);
            FileUtils.touch(nonSnapshotFile);
        }
        // adding secondary index files
        assertThat(new File(temporaryFolder, "d1/data/keyspace1/table1-1234/snapshots/snapshot1/.index/")
                   .mkdirs()).isTrue();
        FileUtils.touch(new File(temporaryFolder,
                                 "d1/data/keyspace1/table1-1234/snapshots/snapshot1/.index/secondary.db"));

        assertThat(new File(temporaryFolder, "d1/data/keyspace1/table1-1234")
                   .setLastModified(System.currentTimeMillis() + 2_000_000)).isTrue();
    }

    public static InstancesConfig mockInstancesConfig(String rootPath)
    {
        CassandraVersionProvider.Builder versionProviderBuilder = new CassandraVersionProvider.Builder();
        versionProviderBuilder.add(new MockCassandraFactory());
        CassandraVersionProvider versionProvider = versionProviderBuilder.build();
        InstanceMetadataImpl localhost = new InstanceMetadataImpl(1,
                                                                  "localhost",
                                                                  9043,
                                                                  Collections.singletonList(rootPath + "/d1/data"),
                                                                  null,
                                                                  versionProvider,
                                                                  1000);
        InstanceMetadataImpl localhost2 = new InstanceMetadataImpl(2,
                                                                   "localhost2",
                                                                   9043,
                                                                   Collections.singletonList(rootPath + "/d2/data"),
                                                                   null,
                                                                   versionProvider,
                                                                   1000);
        List<InstanceMetadata> instanceMetas = Arrays.asList(localhost, localhost2);
        return new InstancesConfigImpl(instanceMetas);
    }

    public static List<String[]> getMockSnapshotDirectories()
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

    public static List<String[]> getMockNonSnapshotDirectories()
    {
        return Arrays.asList(new String[]{ "d1", "data", "keyspace1", "table1", "nonsnapshots", "snapshot1" },
                             new String[]{ "d1", "data", "keyspace1", "table2", "nonsnapshots", "snapshot1" },
                             new String[]{ "d2", "data", "keyspace1", "table1", "nonsnapshots", "snapshot1" },
                             new String[]{ "d2", "data", "keyspace1", "table2", "nonsnapshots", "snapshot1" });
    }

    public static List<String> getMockSnapshotFiles()
    {
        List<String> snapshotFiles = new ArrayList<>();
        snapshotFiles.addAll(getSnapshot1Files());
        snapshotFiles.addAll(getSnapshot2Files());
        Collections.sort(snapshotFiles);
        return snapshotFiles;
    }

    public static List<String> getSnapshot1Files()
    {
        final List<String> snapshotFiles = new ArrayList<>();
        snapshotFiles.addAll(getSnapshot1Instance1Files());
        snapshotFiles.addAll(getSnapshot1Instance2Files());
        Collections.sort(snapshotFiles);
        return snapshotFiles;
    }

    public static List<String> getSnapshot1Instance1Files()
    {
        return Arrays.asList("d1/data/keyspace1/table1-1234/snapshots/snapshot1/1.db",
                             "d1/data/keyspace1/table2-1234/snapshots/snapshot1/3.db");
    }

    public static List<String> getSnapshot1Instance2Files()
    {
        return Arrays.asList("d2/data/keyspace1/table1-1234/snapshots/snapshot1/5.db",
                             "d2/data/keyspace1/table2-1234/snapshots/snapshot1/7.db");
    }

    public static List<String> getSnapshot2Files()
    {
        List<String> snapshotFiles = Arrays.asList("d1/data/keyspace1/table1-1234/snapshots/snapshot2/2.db",
                                                   "d1/data/keyspace1/table2-1234/snapshots/snapshot2/4.db",
                                                   "d2/data/keyspace1/table1-1234/snapshots/snapshot2/6.db",
                                                   "d2/data/keyspace1/table2-1234/snapshots/snapshot2/8.db");
        Collections.sort(snapshotFiles);
        return snapshotFiles;
    }

    public static List<String> getMockNonSnapshotFiles()
    {
        List<String> nonSnapshotFiles = Arrays.asList("d1/data/keyspace1/table1/11.db",
                                                      "d1/data/keyspace1/table2/12.db",
                                                      "d2/data/keyspace1/table1/13.db",
                                                      "d2/data/keyspace1/table2/14.db");
        Collections.sort(nonSnapshotFiles);
        return nonSnapshotFiles;
    }
}
