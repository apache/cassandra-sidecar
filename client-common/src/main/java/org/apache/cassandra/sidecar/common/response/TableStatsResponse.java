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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class response for the TableStats API
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableStatsResponse
{
    private final String keyspace;
    private final String table;
    private final long sstableCount;
    private final long diskSpaceUsedBytes;
    private final long totalDiskSpaceUsedBytes;
    private final long snapshotsSizeBytes;

    /**
     * Constructs a new {@link TableStatsResponse}.
     *
     * @param keyspace the keyspace in Cassandra
     * @param table the table in Cassandra
     * @param sstableCount total count of the sstables
     * @param diskSpaceUsedBytes disk space used in MB for the table
     * @param totalDiskSpaceUsedBytes disk space used in MB for the table
     * @param snapshotsSizeBytes size on disk in MB for the snapshots of the table
     */
    @JsonCreator
    public TableStatsResponse(@JsonProperty("keyspace") String keyspace,
                              @JsonProperty("table") String table,
                              @JsonProperty("sstableCount") long sstableCount,
                              @JsonProperty("diskSpaceUsedBytes") long diskSpaceUsedBytes,
                              @JsonProperty("totalDiskSpaceUsedBytes") long totalDiskSpaceUsedBytes,
                              @JsonProperty("snapshotsSizeBytes") long snapshotsSizeBytes)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.sstableCount = sstableCount;
        this.diskSpaceUsedBytes = diskSpaceUsedBytes;
        this.totalDiskSpaceUsedBytes = totalDiskSpaceUsedBytes;
        this.snapshotsSizeBytes = snapshotsSizeBytes;
    }

    @JsonProperty("keyspace")
    public String keyspace()
    {
        return keyspace;
    }

    @JsonProperty("table")
    public String table()
    {
        return table;
    }

    @JsonProperty("sstableCount")
    public long sstableCount()
    {
        return sstableCount;
    }

    @JsonProperty("diskSpaceUsedBytes")
    public long diskSpaceUsedBytes()
    {
        return diskSpaceUsedBytes;
    }

    @JsonProperty("totalDiskSpaceUsedBytes")
    public long totalDiskSpaceUsedBytes()
    {
        return totalDiskSpaceUsedBytes;
    }

    @JsonProperty("snapshotsSizeBytes")
    public long snapshotsSizeBytes()
    {
        return snapshotsSizeBytes;
    }

}
