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

package org.apache.cassandra.sidecar.config;

/**
 * Configuration for sidecar schema creation
 */
public interface SchemaKeyspaceConfiguration
{
    /**
     * @return boolean indicating if schema creation is enabled
     */
    boolean isEnabled();

    /**
     * @return keyspace name for sidecar schema
     */
    String keyspace();

    /**
     * @return replication strategy for sidecar schema
     */
    String replicationStrategy();

    /**
     * @return replication factor for sidecar schema
     */
    int replicationFactor();

    default String createReplicationStrategyString()
    {
        return String.format("{'class':'%s', 'replication_factor':'%s'}", replicationStrategy(), replicationFactor());
    }
}
