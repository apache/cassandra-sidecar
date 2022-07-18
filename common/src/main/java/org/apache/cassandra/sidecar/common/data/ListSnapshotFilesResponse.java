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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A class representing a response for the {@link ListSnapshotFilesRequest}
 */
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

    public List<FileInfo> getSnapshotFilesInfo()
    {
        return snapshotFilesInfo;
    }

    /**
     * Json data model of file attributes
     */
    public static class FileInfo
    {
        private final long size;
        private final String host;
        private final int port;
        private final int dataDirIndex;
        private final String snapshotName;
        private final String keySpaceName;
        private final String tableName;
        private final String fileName;

        public FileInfo(@JsonProperty("size") long size,
                        @JsonProperty("host") String host,
                        @JsonProperty("port") int port,
                        @JsonProperty("dataDirIndex") int dataDirIndex,
                        @JsonProperty("snapshotName") String snapshotName,
                        @JsonProperty("keySpaceName") String keySpaceName,
                        @JsonProperty("tableName") String tableName,
                        @JsonProperty("fileName") String fileName)
        {
            this.size = size;
            this.host = host;
            this.port = port;
            this.dataDirIndex = dataDirIndex;
            this.snapshotName = snapshotName;
            this.keySpaceName = keySpaceName;
            this.tableName = tableName;
            this.fileName = fileName;
        }

        public String ssTableComponentPath()
        {
            return Paths.get(keySpaceName, tableName, fileName).toString();
        }

        /**
         * @return the size of the file
         */
        public long getSize()
        {
            return size;
        }

        /**
         * @return the host where the file is hosted
         */
        public String getHost()
        {
            return host;
        }

        /**
         * @return the port of the service hosting the file
         */
        public int getPort()
        {
            return port;
        }

        /**
         * @return the index to the data directory
         */
        public int getDataDirIndex()
        {
            return dataDirIndex;
        }

        /**
         * @return the name of the snapshot
         */
        public String getSnapshotName()
        {
            return snapshotName;
        }

        /**
         * @return the name of the keyspace this file belongs to
         */
        public String getKeySpaceName()
        {
            return keySpaceName;
        }

        /**
         * @return the name of the table this file belongs to
         */
        public String getTableName()
        {
            return tableName;
        }

        /**
         * @return the name of the file
         */
        public String getFileName()
        {
            return fileName;
        }

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

        public int hashCode()
        {
            return Objects.hash(size, host, port, dataDirIndex, snapshotName, keySpaceName, tableName, fileName);
        }
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ListSnapshotFilesResponse that = (ListSnapshotFilesResponse) o;
        return Objects.equals(snapshotFilesInfo, that.snapshotFilesInfo);
    }

    public int hashCode()
    {
        return Objects.hash(snapshotFilesInfo);
    }
}
