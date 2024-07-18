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

package org.apache.cassandra.sidecar.common.server;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An interface that defines interactions with the storage system in Cassandra.
 * TODO: relocate to server-common
 */
public interface StorageOperations
{
    /**
     * Takes the snapshot of a multiple column family from different keyspaces. A snapshot name must be specified.
     *
     * @param tag      the tag given to the snapshot; may not be null or empty
     * @param keyspace the keyspace in the Cassandra database to use for the snapshot
     * @param table    the table in the Cassandra database to use for the snapshot
     * @param options  map of options, for example ttl, skipFlush
     */
    void takeSnapshot(@NotNull String tag, @NotNull String keyspace, @NotNull String table,
                      @Nullable Map<String, String> options);

    /**
     * Remove the snapshot with the given {@code tag} from the given {@code keyspace}/{@code table}.
     *
     * @param tag      the tag used to create the snapshot (name of the snapshot)
     * @param keyspace the keyspace in the Cassandra database to use for the snapshot
     * @param table    the table in the Cassandra database to use for the snapshot
     */
    void clearSnapshot(@NotNull String tag, @NotNull String keyspace, @NotNull String table);

    /**
     * Get the ring view of the cluster
     *
     * @param keyspace keyspace to check the data ownership; Cassandra selects a keyspace if null value is passed.
     * @return ring view
     * @throws UnknownHostException when hostname of peer Cassandra nodes cannot be resolved
     */
    RingResponse ring(@Nullable Name keyspace) throws UnknownHostException;

    /**
     * Get the token ranges and the corresponding read and write replicas by datacenter
     *
     * @param keyspace    the keyspace in the Cassandra database
     * @param partitioner token partitioner used for token assignment
     * @return token range to read and write replica mappings
     */
    TokenRangeReplicasResponse tokenRangeReplicas(@NotNull Name keyspace,
                                                  @NotNull String partitioner);

    /**
     * @return the list of all data file locations for the Cassandra instance
     */
    List<String> dataFileLocations();

    /**
     * Clean up the data of the specified and remove the keys no longer belongs to the Cassandra node.
     *
     * @param keyspace keyspace of the table to clean
     * @param table table to clean
     * @param concurrency concurrency of the cleanup (compaction) job.
     *                    Note that it cannot exceed the configured `concurrent_compactors` in Cassandra
     * @throws IOException i/o exception during cleanup
     * @throws ExecutionException it does not really throw but declared in MBean
     * @throws InterruptedException it does not really throw but declared in MBean
     */
    void outOfRangeDataCleanup(@NotNull String keyspace, @NotNull String table, int concurrency)
    throws IOException, ExecutionException, InterruptedException;

    /**
     * Similar to {@link #outOfRangeDataCleanup(String, String, int)}, but use 1 for concurrency
     */
    default void outOfRangeDataCleanup(@NotNull String keyspace, @NotNull String table)
    throws IOException, ExecutionException, InterruptedException
    {
        outOfRangeDataCleanup(keyspace, table, 1);
    }
}
