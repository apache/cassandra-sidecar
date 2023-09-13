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

package org.apache.cassandra.sidecar.adapters.base;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * An interface that pulls methods from the Cassandra Storage Service Proxy
 */
public interface StorageJmxOperations
{
    String STORAGE_SERVICE_OBJ_NAME = "org.apache.cassandra.db:type=StorageService";

    /**
     * Retrieve the list of live nodes in the cluster, where "liveness" is
     * determined by the failure detector of the node being queried.
     *
     * @return set of IP addresses, as Strings
     */
    List<String> getLiveNodesWithPort();

    /**
     * Retrieve the list of unreachable nodes in the cluster, as determined
     * by this node's failure detector.
     *
     * @return set of IP addresses, as Strings
     */
    List<String> getUnreachableNodesWithPort();

    /**
     * Retrieve the list of nodes currently bootstrapping into the ring.
     *
     * @return set of IP addresses, as Strings
     */
    List<String> getJoiningNodesWithPort();

    /**
     * Retrieve the list of nodes currently leaving the ring.
     *
     * @return set of IP addresses, as Strings
     */
    List<String> getLeavingNodesWithPort();

    /**
     * Retrieve the list of nodes currently moving in the ring.
     *
     * @return set of IP addresses, as Strings
     */
    List<String> getMovingNodesWithPort();

    /**
     * Human-readable load value.  Keys are IP addresses.
     */
    Map<String, String> getLoadMapWithPort();

    /**
     * Retrieve a map of tokens to endpoints, including the bootstrapping
     * ones.
     *
     * @return a map of tokens to endpoints in ascending order
     */
    Map<String, String> getTokenToEndpointWithPortMap();

    /**
     * Effective ownership is % of the data each node owns given the keyspace
     * we calculate the percentage using replication factor.
     * If Keyspace == null, this method will try to verify if all the keyspaces
     * in the cluster have the same replication strategies and if yes then we will
     * use the first else an empty Map is returned.
     */
    Map<String, Float> effectiveOwnershipWithPort(String keyspace) throws IllegalStateException;

    /**
     * given a list of tokens (representing the nodes in the cluster), returns
     * a mapping from {@code "token -> %age of cluster owned by that token"}
     */
    Map<String, Float> getOwnershipWithPort();

    /**
     * Retrieve the mapping of endpoint to host ID
     */
    Map<String, String> getEndpointWithPortToHostId();

    /**
     * Takes the snapshot of a multiple column family from different keyspaces. A snapshot name must be specified.
     *
     * @param tag      the tag given to the snapshot; may not be null or empty
     * @param options  map of options, for example ttl, skipFlush
     * @param entities list of keyspaces / tables in the form of empty | ks1 ks2 ... | ks1.cf1,ks2.cf2,...
     * @throws IOException in certain versions of Cassandra.
     */
    void takeSnapshot(String tag, Map<String, String> options, String... entities) throws IOException;

    /**
     * Remove the snapshot with the given name from the given keyspaces.
     *
     * @param tag           the tag used to create the snapshot (name of the snapshot)
     * @param keyspaceNames an optional list of keyspaces
     */
    void clearSnapshot(String tag, String... keyspaceNames);

    /**
     * Retrieve the list of node endpoints by token range for the given keyspace
     *
     * @param keyspace the keyspace in Cassandra
     * @return Returns a mapping of token range (represented by the first two entries in the key) to
     * a list of endpoints
     */
    Map<List<String>, List<String>> getRangeToEndpointWithPortMap(String keyspace);

    /**
     * Retrieve the list of pending node endpoints by token range for the given keyspace
     *
     * @param keyspace the keyspace in Cassandra
     * @return Returns a mapping of token range (represented by the first two entries in the key) to
     * a list of endpoints
     */
    Map<List<String>, List<String>> getPendingRangeToEndpointWithPortMap(String keyspace);
}
