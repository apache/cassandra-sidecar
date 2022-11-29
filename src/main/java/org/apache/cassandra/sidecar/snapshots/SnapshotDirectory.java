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
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.base.Preconditions;

import static org.apache.cassandra.sidecar.snapshots.SnapshotPathBuilder.SNAPSHOTS_DIR_NAME;

/**
 * An object that encapsulates the parts of a snapshot directory
 */
public class SnapshotDirectory
{
    public final String dataDirectory;
    public final String keyspace;
    public final String tableName;
    public final String snapshotName;

    SnapshotDirectory(String dataDirectory, String keyspace, String tableName, String snapshotName)
    {
        this.dataDirectory = dataDirectory;
        this.keyspace = keyspace;
        this.tableName = tableName;
        this.snapshotName = snapshotName;
    }

    /**
     * Parses a snapshot directory string into a {@link SnapshotDirectory} object. The snapshot directory
     * has the following structure {@code /&lt;data_dir&gt;/&lt;ks&gt;/&lt;table&gt;/snapshots/&lt;snapshot_name&gt;}.
     *
     * @param snapshotDirectory the absolute path to the snapshot directory
     * @return the {@link SnapshotDirectory} object representing the provided {@code snapshotDirectory}
     */
    public static SnapshotDirectory of(String snapshotDirectory)
    {
        Path snapshotDirectoryPath = Paths.get(snapshotDirectory);
        int nameCount = snapshotDirectoryPath.getNameCount();
        Preconditions.checkArgument(nameCount >= 5, "Invalid snapshotDirectory. " +
                                                    "Expected at least 5 parts but found " + nameCount);
        String snapshotName = snapshotDirectoryPath.getName(nameCount - 1).toString();
        String snapshotDirName = snapshotDirectoryPath.getName(nameCount - 2).toString();
        String tableName = snapshotDirectoryPath.getName(nameCount - 3).toString();
        String keyspace = snapshotDirectoryPath.getName(nameCount - 4).toString();
        String dataDirectory = File.separator + snapshotDirectoryPath.subpath(0, nameCount - 4);

        Preconditions.checkArgument(SNAPSHOTS_DIR_NAME.equalsIgnoreCase(snapshotDirName),
                                    "Invalid snapshotDirectory. The expected directory structure is " +
                                    "'/<data_dir>/<ks>/<table>/snapshots/<snapshot_name>'");

        return new SnapshotDirectory(dataDirectory, keyspace, tableName, snapshotName);
    }
}
