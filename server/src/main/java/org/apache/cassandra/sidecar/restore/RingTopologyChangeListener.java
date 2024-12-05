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

package org.apache.cassandra.sidecar.restore;

import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * To receive notifications on ring topology changed
 */
public interface RingTopologyChangeListener
{
    /**
     * Notification on ring topology changed
     * @param keyspace keyspace (which defines replication factor) to derive the topology from
     * @param oldTopology old topology. The value is nullable. When the ring topology of the keyspace is just learned, the value is null.
     * @param newTopology new topology. The value is always non-null
     */
    void onRingTopologyChanged(String keyspace,
                               @Nullable TokenRangeReplicasResponse oldTopology,
                               @NotNull TokenRangeReplicasResponse newTopology);
}
