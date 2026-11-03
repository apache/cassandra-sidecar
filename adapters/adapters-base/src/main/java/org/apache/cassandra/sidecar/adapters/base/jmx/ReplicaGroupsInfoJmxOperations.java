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

package org.apache.cassandra.sidecar.adapters.base.jmx;

import javax.management.openmbean.CompositeData;

/**
 * An interface that pulls methods from Cassandra's ReplicaGroupsInfo MBean, which reports replica placement,
 * including which replicas are witnesses. Only Cassandra versions that support witness replicas register it.
 */
public interface ReplicaGroupsInfoJmxOperations
{
    String REPLICA_GROUPS_INFO_OBJ_NAME = "org.apache.cassandra.locator:type=ReplicaGroupsInfo";

    /**
     * @param keyspace the keyspace in Cassandra
     * @return the read and write placement of the keyspace, and the peers in it, as a single {@link CompositeData}
     * of type {@code ReplicaGroupsByDatacenter}
     */
    CompositeData replicaGroupsForKeyspaceByDatacenter(String keyspace);
}