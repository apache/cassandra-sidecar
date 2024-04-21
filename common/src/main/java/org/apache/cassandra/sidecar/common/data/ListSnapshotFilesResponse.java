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

package org.apache.cassandra.sidecar.common.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.common.annotations.Beta;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;

/**
 * A class representing a response for the {@code SnapshotRequest}.
 * This class is expected to evolve and has been mark with the {@link Beta} annotation.
 */
@Beta
public class ListSnapshotFilesResponse
{
    private final List<FileInfo> snapshotFilesInfo;

    public ListSnapshotFilesResponse()
    {
        this.snapshotFilesInfo = new ArrayList<>();
    }

    public void addSnapshotFile(FileInfo fileInfo)
    {
        snapshotFilesInfo.add(fileInfo);
    }

    @JsonProperty("snapshotFilesInfo")
    public List<FileInfo> snapshotFilesInfo()
    {
        return snapshotFilesInfo;
    }

    /**
     * Json data model of file attributes
     */
    public static class FileInfo
    {
        public final long size;
        public final String host;
        public final int port;
        public final int dataDirIndex;
        public final String snapshotName;
        public final String keySpaceName;
        public final String tableName;
        public final String tableId;
        public final String fileName;
        private String componentDownloadUrl;

        public FileInfo(@JsonProperty("size") long size,
                        @JsonProperty("host") String host,
                        @JsonProperty("port") int port,
                        @JsonProperty("dataDirIndex") int dataDirIndex,
                        @JsonProperty("snapshotName") String snapshotName,
                        @JsonProperty("keySpaceName") String keySpaceName,
                        @JsonProperty("tableName") String tableName,
                        @JsonProperty("tableId") String tableId,
                        @JsonProperty("fileName") String fileName)
        {
            this.size = size;
            this.host = host;
            this.port = port;
            this.dataDirIndex = dataDirIndex;
            this.snapshotName = snapshotName;
            this.keySpaceName = keySpaceName;
            this.tableName = tableName;
            this.tableId = tableId;
            this.fileName = fileName;
        }

        public String componentDownloadUrl()
        {
            if (componentDownloadUrl == null)
            {
                String tableName = this.tableId != null
                                   ? this.tableName + "-" + this.tableId
                                   : this.tableName;

                componentDownloadUrl = ApiEndpointsV1.COMPONENTS_ROUTE
                                       .replaceAll(ApiEndpointsV1.KEYSPACE_PATH_PARAM, keySpaceName)
                                       .replaceAll(ApiEndpointsV1.TABLE_PATH_PARAM, tableName)
                                       .replaceAll(ApiEndpointsV1.SNAPSHOT_PATH_PARAM, snapshotName)
                                       .replaceAll(ApiEndpointsV1.COMPONENT_PATH_PARAM, fileName)
                                       + "?dataDirectoryIndex=" + dataDirIndex;
            }
            return componentDownloadUrl;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FileInfo fileInfo = (FileInfo) o;
            return size == fileInfo.size &&
                   port == fileInfo.port &&
                   dataDirIndex == fileInfo.dataDirIndex &&
                   Objects.equals(host, fileInfo.host) &&
                   Objects.equals(snapshotName, fileInfo.snapshotName) &&
                   Objects.equals(keySpaceName, fileInfo.keySpaceName) &&
                   Objects.equals(tableName, fileInfo.tableName) &&
                   Objects.equals(fileName, fileInfo.fileName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(size, host, port, dataDirIndex, snapshotName, keySpaceName, tableName, fileName);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ListSnapshotFilesResponse that = (ListSnapshotFilesResponse) o;
        return Objects.equals(snapshotFilesInfo, that.snapshotFilesInfo);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(snapshotFilesInfo);
    }
}
