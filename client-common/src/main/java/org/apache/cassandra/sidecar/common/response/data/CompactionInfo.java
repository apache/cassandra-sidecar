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
import org.apache.cassandra.sidecar.common.DataObjectBuilder;

/**
 * Represents an active compaction entry
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CompactionInfo
{
    public static final String ID = "id";
    public static final String KEYSPACE = "keyspace";
    public static final String TABLE = "table";
    public static final String TASK_TYPE = "taskType";
    public static final String COMPLETED_BYTES = "completedBytes";
    public static final String TOTAL_BYTES = "totalBytes";
    public static final String PERCENT_COMPLETED = "percentCompleted";
    public static final String SS_TABLES = "sstables";
    public static final String TARGET_DIRECTORY = "targetDirectory";

    private final String id;
    private final String keyspace;
    private final String table;
    private final String taskType;
    private final long completedBytes;
    private final long totalBytes;
    private final double percentCompleted;
    private final List<String> sstables;
    private final String targetDirectory;

    private CompactionInfo(Builder builder)
    {
        this.id = builder.id;
        this.keyspace = builder.keyspace;
        this.table = builder.table;
        this.taskType = builder.taskType;
        this.completedBytes = builder.completedBytes;
        this.totalBytes = builder.totalBytes;
        this.percentCompleted = builder.percentCompleted;
        this.sstables = builder.sstables;
        this.targetDirectory = builder.targetDirectory;
    }

    /**
     * Constructs a new {@link CompactionInfo}.
     *
     * @param id               compaction ID
     * @param keyspace         keyspace name
     * @param table            table name
     * @param taskType         type of compaction task
     * @param completedBytes   completed compaction in bytes
     * @param totalBytes       total compaction in bytes
     * @param percentCompleted percentage of completed compactions
     * @param sstables         list of SSTables being compacted
     * @param targetDirectory  target directory for output
     */
    @JsonCreator
    public CompactionInfo(@JsonProperty(ID) String id,
                          @JsonProperty(KEYSPACE) String keyspace,
                          @JsonProperty(TABLE) String table,
                          @JsonProperty(TASK_TYPE) String taskType,
                          @JsonProperty(COMPLETED_BYTES) long completedBytes,
                          @JsonProperty(TOTAL_BYTES) long totalBytes,
                          @JsonProperty(PERCENT_COMPLETED) double percentCompleted,
                          @JsonProperty(SS_TABLES) List<String> sstables,
                          @JsonProperty(TARGET_DIRECTORY) String targetDirectory)
    {
        this.id = id;
        this.keyspace = keyspace;
        this.table = table;
        this.taskType = taskType;
        this.completedBytes = completedBytes;
        this.totalBytes = totalBytes;
        this.percentCompleted = percentCompleted;
        this.sstables = sstables;
        this.targetDirectory = targetDirectory;
    }

    @JsonProperty(ID)
    public String id()
    {
        return id;
    }

    @JsonProperty(KEYSPACE)
    public String keyspace()
    {
        return keyspace;
    }

    @JsonProperty(TABLE)
    public String table()
    {
        return table;
    }

    @JsonProperty(TASK_TYPE)
    public String taskType()
    {
        return taskType;
    }

    @JsonProperty(COMPLETED_BYTES)
    public long completedBytes()
    {
        return completedBytes;
    }

    @JsonProperty(TOTAL_BYTES)
    public long totalBytes()
    {
        return totalBytes;
    }

    @JsonProperty(PERCENT_COMPLETED)
    public double percentCompleted()
    {
        return percentCompleted;
    }

    @JsonProperty(SS_TABLES)
    public List<String> ssTables()
    {
        return sstables;
    }

    @JsonProperty(TARGET_DIRECTORY)
    public String targetDirectory()
    {
        return targetDirectory;
    }

    @Override
    public String toString()
    {
        return String.format("CompactionInfo{" +
                           "id='%s', " +
                           "keyspace='%s', " +
                           "table='%s', " +
                           "taskType='%s', " +
                           "completedBytes=%d, " +
                           "totalBytes=%d, " +
                           "percentCompleted=%.2f, " +
                           "sstables=%s, " +
                           "targetDirectory='%s'}",
                           id, keyspace, table, taskType, completedBytes, totalBytes, percentCompleted, sstables, targetDirectory);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code ActiveCompactionEntry} builder static inner class.
     */
    public static final class Builder implements DataObjectBuilder<Builder, CompactionInfo>
    {
        private String id;
        private String keyspace;
        private String table;
        private String taskType;
        private long completedBytes;
        private long totalBytes;
        private double percentCompleted;
        private List<String> sstables;
        private String targetDirectory;

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code id} and returns a reference to this Builder enabling method chaining.
         *
         * @param id the {@code id} to set
         * @return a reference to this Builder
         */
        public Builder id(String id)
        {
            return update(b -> b.id = id);
        }

        /**
         * Sets the {@code keyspace} and returns a reference to this Builder enabling method chaining.
         *
         * @param keyspace the {@code keyspace} to set
         * @return a reference to this Builder
         */
        public Builder keyspace(String keyspace)
        {
            return update(b -> b.keyspace = keyspace);
        }

        /**
         * Sets the {@code table} and returns a reference to this Builder enabling method chaining.
         *
         * @param table the {@code table} to set
         * @return a reference to this Builder
         */
        public Builder table(String table)
        {
            return update(b -> b.table = table);
        }

        /**
         * Sets the {@code taskType} and returns a reference to this Builder enabling method chaining.
         *
         * @param taskType the {@code taskType} to set
         * @return a reference to this Builder
         */
        public Builder taskType(String taskType)
        {
            return update(b -> b.taskType = taskType);
        }

        /**
         * Sets the {@code completedBytes} and returns a reference to this Builder enabling method chaining.
         *
         * @param completedBytes the {@code completedBytes} to set
         * @return a reference to this Builder
         */
        public Builder completedBytes(long completedBytes)
        {
            return update(b -> b.completedBytes = completedBytes);
        }

        /**
         * Sets the {@code totalBytes} and returns a reference to this Builder enabling method chaining.
         *
         * @param totalBytes the {@code totalBytes} to set
         * @return a reference to this Builder
         */
        public Builder totalBytes(long totalBytes)
        {
            return update(b -> b.totalBytes = totalBytes);
        }

        /**
         * Sets the {@code percentCompleted} and returns a reference to this Builder enabling method chaining.
         *
         * @param percentCompleted the {@code percentCompleted} to set
         * @return a reference to this Builder
         */
        public Builder percentCompleted(double percentCompleted)
        {
            return update(b -> b.percentCompleted = percentCompleted);
        }

        /**
         * Sets the {@code ssTables} and returns a reference to this Builder enabling method chaining.
         *
         * @param sstables the {@code sstables} to set
         * @return a reference to this Builder
         */
        public Builder sstables(List<String> sstables)
        {
            return update(b -> b.sstables = sstables);
        }

        /**
         * Sets the {@code targetDirectory} and returns a reference to this Builder enabling method chaining.
         *
         * @param targetDirectory the {@code targetDirectory} to set
         * @return a reference to this Builder
         */
        public Builder targetDirectory(String targetDirectory)
        {
            return update(b -> b.targetDirectory = targetDirectory);
        }

        /**
         * Returns a {@code ActiveCompactionEntry} built from the parameters previously set.
         *
         * @return a {@code ActiveCompactionEntry} built with parameters of this {@code ActiveCompactionEntry.Builder}
         */
        @Override
        public CompactionInfo build()
        {
            return new CompactionInfo(this);
        }
    }
}
