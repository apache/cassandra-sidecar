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

import java.util.List;

/**
 * Core Cassandra Adapter interface
 * For now, this is just a placeholder.  We will most likely want to define the interface to returns bits such as
 * getCompaction(), getClusterMembership, etc, which return interfaces such as ICompaction, IClusterMembership.
 * We will need different implementations due to the slow move away from JMX towards CQL for some, but not all, actions.
 */
public interface ICassandraAdapter
{
    /**
     * @return a list of {@link NodeStatus} for the Cassandra cluster
     */
    List<NodeStatus> getStatus();

    /**
     * @return the {@link StorageOperations} implementation for the Cassandra cluster
     */
    StorageOperations storageOperations();

}
