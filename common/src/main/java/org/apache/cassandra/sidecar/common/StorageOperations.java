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

package org.apache.cassandra.sidecar.common;

import java.net.UnknownHostException;
import java.util.Map;

import org.apache.cassandra.sidecar.common.data.RingResponse;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An interface that defines interactions with the storage system in Cassandra.
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
    RingResponse ring(@Nullable String keyspace) throws UnknownHostException;

    /**
     * Get the token ranges and the corresponding read and write replicas by datacenter
     * @param keyspace the keyspace in the Cassandra database
     * @param partitioner token partitioner used for token assignment
     * @return token range to read and write replica mappings
     */
    TokenRangeReplicasResponse tokenRangeReplicas(@NotNull String keyspace,
                                                  @NotNull String partitioner);
}
