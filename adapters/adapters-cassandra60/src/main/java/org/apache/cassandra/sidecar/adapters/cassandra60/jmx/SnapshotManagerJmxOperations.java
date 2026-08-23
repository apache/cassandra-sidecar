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

package org.apache.cassandra.sidecar.adapters.cassandra60.jmx;

import java.io.IOException;
import java.util.Map;

/**
 * An interface that pulls methods from the Cassandra Snapshot Manager MBean. Cassandra 6.0 introduced this MBean
 * and deprecated the snapshot methods on the Storage Service MBean.
 */
public interface SnapshotManagerJmxOperations
{
    String SNAPSHOT_MANAGER_OBJ_NAME = "org.apache.cassandra.service.snapshot:type=SnapshotManager";

    /**
     * Takes the snapshot of tables in one or more keyspaces.
     *
     * @param tag      the tag given to the snapshot; may not be null or empty
     * @param options  map of options, for example skipFlush
     * @param entities an empty list, a list of keyspaces, or a list of {@code keyspace.table} pairs
     * @throws IOException when the snapshot operation fails
     */
    void takeSnapshot(String tag, Map<String, String> options, String... entities) throws IOException;

    /**
     * Deletes the snapshot with the given tag from the listed keyspaces. A null or empty tag clears every snapshot.
     *
     * @param tag           the tag of the snapshot to clear
     * @param options       map of options; Cassandra 6.0 accepts {@code older_than} and {@code older_than_timestamp}
     * @param keyspaceNames the keyspaces to clear the snapshot from
     * @throws IOException when the clear snapshot operation fails
     */
    void clearSnapshot(String tag, Map<String, Object> options, String... keyspaceNames) throws IOException;
}
