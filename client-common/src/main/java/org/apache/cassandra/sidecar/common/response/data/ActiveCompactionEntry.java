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

package org.apache.cassandra.sidecar.common.response.data;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents an active compaction entry
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ActiveCompactionEntry
{
    private final String id;
    private final String keyspace;
    private final String columnFamily;
    private final String taskType;
    private final long completedBytes;
    private final long totalBytes;
    private final double percentCompleted;
    private final List<String> ssTables;
    private final String targetDirectory;

    /**
     * Constructs a new {@link ActiveCompactionEntry}.
     *
     * @param id               compaction ID
     * @param keyspace         keyspace name
     * @param columnFamily     table/column family name
     * @param taskType         type of compaction task
     * @param completedBytes   completed compaction in bytes
     * @param totalBytes       total compaction in bytes
     * @param percentCompleted percentage of completed compactions
     * @param ssTables         list of SSTables being compacted
     * @param targetDirectory  target directory for output
     */
    @JsonCreator
    public ActiveCompactionEntry(@JsonProperty("id") final String id,
                                 @JsonProperty("keyspace") final String keyspace,
                                 @JsonProperty("columnFamily") final String columnFamily,
                                 @JsonProperty("taskType") final String taskType,
                                 @JsonProperty("completedBytes") final long completedBytes,
                                 @JsonProperty("totalBytes") final long totalBytes,
                                 @JsonProperty("percentCompleted") final double percentCompleted,
                                 @JsonProperty("ssTables") final List<String> ssTables,
                                 @JsonProperty("targetDirectory") final String targetDirectory)
    {
        this.id = id;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.taskType = taskType;
        this.completedBytes = completedBytes;
        this.totalBytes = totalBytes;
        this.percentCompleted = percentCompleted;
        this.ssTables = ssTables;
        this.targetDirectory = targetDirectory;
    }

    @JsonProperty("id")
    public String id()
    {
        return id;
    }

    @JsonProperty("keyspace")
    public String keyspace()
    {
        return keyspace;
    }

    @JsonProperty("columnFamily")
    public String columnFamily()
    {
        return columnFamily;
    }

    @JsonProperty("taskType")
    public String taskType()
    {
        return taskType;
    }

    @JsonProperty("completedBytes")
    public long completedBytes()
    {
        return completedBytes;
    }

    @JsonProperty("totalBytes")
    public long totalBytes()
    {
        return totalBytes;
    }

    @JsonProperty("percentCompleted")
    public double percentCompleted()
    {
        return percentCompleted;
    }

    @JsonProperty("ssTables")
    public List<String> ssTables()
    {
        return ssTables;
    }

    @JsonProperty("targetDirectory")
    public String targetDirectory()
    {
        return targetDirectory;
    }
}