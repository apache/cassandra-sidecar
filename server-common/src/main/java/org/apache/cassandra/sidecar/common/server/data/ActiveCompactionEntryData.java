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

package org.apache.cassandra.sidecar.common.server.data;

import java.util.List;

import org.apache.cassandra.sidecar.common.DataObjectBuilder;

/**
 * Data object representing an active compaction entry
 */
public class ActiveCompactionEntryData
{
    public final String id;
    public final String keyspace;
    public final String table;
    public final String taskType;
    public final long completedBytes;
    public final long totalBytes;
    public final double percentCompleted;
    public final List<String> sstables;
    public final String targetDirectory;

    private ActiveCompactionEntryData(Builder builder)
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


    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code ActiveCompactionEntryData} builder static inner class.
     */
    public static final class Builder implements DataObjectBuilder<Builder, ActiveCompactionEntryData>
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
         * Sets the {@code sstables} and returns a reference to this Builder enabling method chaining.
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
         * Returns a {@code ActiveCompactionEntryData} built from the parameters previously set.
         *
         * @return a {@code ActiveCompactionEntryData} built with parameters of this {@code ActiveCompactionEntryData.Builder}
         */
        @Override
        public ActiveCompactionEntryData build()
        {
            return new ActiveCompactionEntryData(this);
        }
    }
}
