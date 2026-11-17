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

package org.apache.cassandra.sidecar.job.restart;

/**
 * Describes the data replication scope across a cluster. 
 */
public enum ReplicaScope
{
    /**
     * All keyspaces use NetworkTopologyStrategy with max RF less than number of racks, meaning separate data replicas 
     * are never hosted within the same rack. Same-rack parallelism for operations is safe. 
     */
    RACK,

    /**
     * All keyspaces use NetworkTopologyStrategy, but at least one keyspace's RF exceeds the number of 
     * racks, so a rack can hold more than one replica of a set of data. Co-replicas are confined to the datacenter 
     * but not to distinct racks.
     */
    DATACENTER,

    /**
     * At least one keyspace does not use NetworkTopologyStrategy (e.g. SimpleStrategy), meaning replicas are hosted by 
     * nodes regardless of rack or DC. A co-replica can be any node in the cluster.
     */
    CLUSTER
}
