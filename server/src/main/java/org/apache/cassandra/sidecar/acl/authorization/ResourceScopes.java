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

package org.apache.cassandra.sidecar.acl.authorization;

import java.util.Collections;
import java.util.Set;

/**
 * Possible resource scopes of sidecar permissions. This list is not exhaustive.
 */
public class ResourceScopes
{
    /**
     * Signifies the Cassandra cluster scope. For example, to determine whether you have access to
     * retrieve basic Cassandra ring, gossip, or other Cassandra-related information. Currently cluster scope
     * does not contain any resource variables.
     */
    public static final ResourceScope CLUSTER = new ResourceScope()
    {
        public String variableAwareResource()
        {
            return "CLUSTER";
        }

        public String resolveWithResource(String resource)
        {
            return "CLUSTER";
        }

        public Set<String> expandedResources()
        {
            return Collections.emptySet();
        }
    };

    /**
     * Signifies the Cassandra Sidecar operations scope. Whether Sidecar operator has sufficient permissions to
     * run operations against both Cassandra Sidecar and Cassandra clusters that Sidecar is managing. Currently
     * operation scope does not contain any resource variables.
     */
    public static final ResourceScope OPERATION = new ResourceScope()
    {
        public String variableAwareResource()
        {
            return "OPERATION";
        }

        public String resolveWithResource(String resource)
        {
            return "OPERATION";
        }

        public Set<String> expandedResources()
        {
            return Collections.emptySet();
        }
    };

    /**
     * Signifies Cassandra data scope.
     */
    public static final ResourceScope DATA = new DataResourceScope();

    /**
     * Signifies Cassandra data scope at a keyspace level.
     */
    public static final ResourceScope KEYSPACE = new DataResourceScope(true);

    /**
     * Signifies Cassandra data scope at a table level.
     */
    public static final ResourceScope TABLE = new DataResourceScope(true, true);
}
