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

package org.apache.cassandra.sidecar.common.response;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.jetbrains.annotations.NotNull;

/**
 * Class representing response for {@link ApiEndpointsV1#LIVE_MIGRATION_FILES_ROUTE}.
 * Holds list of file urls (with their individual sizes) + total size of files
 * <p>
 * Sample format:
 * <pre>
 * {
 *   "totalSize": 123456789,
 *   "files" : [
 *     {
 *       "fileUrl": "/api/v1/live-migration/files/{type_of_directory}/{directory_index}/{relative_file_path}",
 *       "size": 123456789,
 *       "fileType": "FILE",
 *       "lastModifiedTime": 1707193655406
 *     },
 *
 *     {
 *       "fileUrl": "/api/v1/live-migration/files/{type_of_directory}/{directory_index}/{relative_file_path}",
 *       "size": -1,
 *       "fileType": "DIRECTORY",
 *       "lastModifiedTime": 1707798915999
 *     }
 *   ]
 * }
 * </pre>
 */
public class InstanceFilesListResponse
{
    private final List<InstanceFileInfo> files;
    private final long totalSize;

    public InstanceFilesListResponse(@NotNull List<InstanceFileInfo> files)
    {
        this.files = Collections.unmodifiableList(files);
        long sum = 0L;
        for (InstanceFileInfo file : files)
        {
            if (file.fileType.equals(InstanceFileInfo.FileType.FILE))
            {
                long size = file.size;
                sum += size;
            }
        }
        totalSize = sum;
    }

    @JsonCreator
    public InstanceFilesListResponse(@JsonProperty("files") List<InstanceFileInfo> files,
                                     @JsonProperty("totalSize") long totalSize)
    {
        this.files = Collections.unmodifiableList(files);
        this.totalSize = totalSize;
    }

    public List<InstanceFileInfo> getFiles()
    {
        return files;
    }

    public long getTotalSize()
    {
        return totalSize;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        InstanceFilesListResponse that = (InstanceFilesListResponse) o;
        return totalSize == that.totalSize && Objects.equals(files, that.files);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(files, totalSize);
    }
}
