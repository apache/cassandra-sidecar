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
package org.apache.cassandra.sidecar.data;

import java.util.Objects;

import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.routes.snapshots.ListSnapshotHandler;

/**
 * Holder class for the {@link ListSnapshotHandler}
 * request parameters
 */
public class SnapshotRequest
{
    private final String snapshotName;
    private final boolean includeSecondaryIndexFiles;
    private final QualifiedTableName qualifiedTableName;
    private final String ttl;

    /**
     * Constructs a new object from the configured builder.
     *
     * @param builder the builder object used to construct this object
     */
    private SnapshotRequest(Builder builder)
    {
        snapshotName = Objects.requireNonNull(builder.snapshotName, "snapshotName must not be null");
        qualifiedTableName = Objects.requireNonNull(builder.qualifiedTableName, "qualifiedTableName must be not null");
        includeSecondaryIndexFiles = builder.includeSecondaryIndexFiles;
        ttl = builder.ttl;
    }

    /**
     * @return the {@link QualifiedTableName}
     */
    public QualifiedTableName qualifiedTableName()
    {
        return qualifiedTableName;
    }

    /**
     * @return the keyspace in Cassandra
     */
    public String keyspace()
    {
        return qualifiedTableName.keyspace();
    }

    /**
     * @return the table name in Cassandra
     */
    public String tableName()
    {
        return qualifiedTableName.tableName();
    }

    /**
     * @return the name of the snapshot
     */
    public String snapshotName()
    {
        return snapshotName;
    }

    /**
     * @return true if secondary index files should be included, false otherwise
     */
    public boolean includeSecondaryIndexFiles()
    {
        return includeSecondaryIndexFiles;
    }

    /**
     * @return the TTL for snapshot creation if provided
     */
    public String ttl()
    {
        return ttl;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "SnapshotRequest{" +
               "keyspace='" + keyspace() + '\'' +
               ", tableName='" + tableName() + '\'' +
               ", snapshotName='" + snapshotName + '\'' +
               ", includeSecondaryIndexFiles=" + includeSecondaryIndexFiles +
               ", ttl=" + ttl +
               '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code SnapshotRequest} builder static inner class.
     */
    public static final class Builder implements DataObjectBuilder<Builder, SnapshotRequest>
    {
        private String snapshotName;
        private boolean includeSecondaryIndexFiles = false;
        private QualifiedTableName qualifiedTableName;
        private String ttl;

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code snapshotName} and returns a reference to this Builder enabling method chaining.
         *
         * @param snapshotName the {@code snapshotName} to set
         * @return a reference to this Builder
         */
        public Builder snapshotName(String snapshotName)
        {
            return update(b -> b.snapshotName = snapshotName);
        }

        /**
         * Sets the {@code includeSecondaryIndexFiles} and returns a reference to this Builder enabling method chaining.
         *
         * @param includeSecondaryIndexFiles the {@code includeSecondaryIndexFiles} to set
         * @return a reference to this Builder
         */
        public Builder includeSecondaryIndexFiles(boolean includeSecondaryIndexFiles)
        {
            return update(b -> b.includeSecondaryIndexFiles = includeSecondaryIndexFiles);
        }

        /**
         * Sets the {@code qualifiedTableName} and returns a reference to this Builder enabling method chaining.
         *
         * @param keyspace the Cassandra keyspace
         * @param table    the Cassandra table
         * @return a reference to this Builder
         */
        public Builder qualifiedTableName(String keyspace, String table)
        {
            return update(b -> b.qualifiedTableName = new QualifiedTableName(keyspace, table));
        }

        /**
         * Sets the {@code qualifiedTableName} and returns a reference to this Builder enabling method chaining.
         *
         * @param qualifiedTableName the {@code qualifiedTableName} to set
         * @return a reference to this Builder
         */
        public Builder qualifiedTableName(QualifiedTableName qualifiedTableName)
        {
            return update(b -> b.qualifiedTableName = qualifiedTableName);
        }

        /**
         * Sets the {@code ttl} and returns a reference to this Builder enabling method chaining.
         *
         * @param ttl the {@code ttl} to set
         * @return a reference to this Builder
         */
        public Builder ttl(String ttl)
        {
            return update(b -> b.ttl = ttl);
        }

        /**
         * Returns a {@code SnapshotRequest} built from the parameters previously set.
         *
         * @return a {@code SnapshotRequest} built with parameters of this {@code SnapshotRequest.Builder}
         */
        @Override
        public SnapshotRequest build()
        {
            return new SnapshotRequest(this);
        }
    }
}
