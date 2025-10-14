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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class for holding information of a downloadable file during Live Migration.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class InstanceFileInfo
{
    public final String fileUrl;
    // size is only relevant when the fileType is 'FILE'.
    // It is set to -1 for directories.
    public final long size;
    public final FileType fileType;

    // The latest timestamp at which the file/dir was modified represented in milliseconds.
    public final long lastModifiedTime;

    public InstanceFileInfo(@JsonProperty("fileUrl") String fileUrl,
                            @JsonProperty("size") long size,
                            @JsonProperty("fileType") FileType fileType,
                            @JsonProperty("lastModifiedTime") long lastModifiedTime)
    {
        this.fileUrl = fileUrl;
        this.size = size;
        this.fileType = fileType;
        this.lastModifiedTime = lastModifiedTime;
    }

    /**
     * Enum for file type
     */
    public enum FileType
    {
        FILE,
        DIRECTORY
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        InstanceFileInfo that = (InstanceFileInfo) o;
        return size == that.size
               && lastModifiedTime == that.lastModifiedTime
               && Objects.equals(fileUrl, that.fileUrl)
               && fileType == that.fileType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fileUrl, size, fileType, lastModifiedTime);
    }

    @Override
    public String toString()
    {
        return "InstanceFileInfo{" +
               "fileUrl='" + fileUrl + '\'' +
               ", size=" + size +
               ", fileType=" + fileType +
               ", lastModifiedTime=" + lastModifiedTime +
               '}';
    }
}
