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

import java.util.Set;

import static org.apache.cassandra.sidecar.common.utils.StringUtils.isNullOrEmpty;

/**
 * Possible resource scopes of sidecar permissions. This list is not exhaustive.
 */
public class ResourceScopes
{
    /**
     * Default scope used when resource scope is not defined for {@link Permission}.
     */
    public static final ResourceScope NO_SCOPE = new ResourceScope()
    {
        @Override
        public String variableAwareResource()
        {
            return null;
        }

        @Override
        public String resolveWithResource(String resource)
        {
            return resource;
        }

        @Override
        public Set<String> expandedResources()
        {
            return Set.of();
        }
    };

    /**
     * Signifies the Cassandra cluster scope. For example, to determine whether you have access to
     * retrieve basic Cassandra ring, gossip, or other Cassandra-related information. Currently, cluster scope
     * does not contain any resource variables.
     */
    public static final ResourceScope CLUSTER_SCOPE = new ResourceScope()
    {
        @Override
        public String variableAwareResource()
        {
            return "cluster";
        }

        @Override
        public String resolveWithResource(String resource)
        {
            validate(resource);
            return "cluster";
        }

        @Override
        public Set<String> expandedResources()
        {
            return Set.of(variableAwareResource());
        }
    };

    /**
     * Signifies the Cassandra Sidecar operations scope. Whether Sidecar operator has sufficient permissions to
     * run operations against both Cassandra Sidecar and Cassandra clusters that Sidecar is managing. Currently
     * operation scope does not contain any resource variables.
     */
    public static final ResourceScope OPERATION_SCOPE = new ResourceScope()
    {
        @Override
        public String variableAwareResource()
        {
            return "operation";
        }

        @Override
        public String resolveWithResource(String resource)
        {
            validate(resource);
            return "operation";
        }

        @Override
        public Set<String> expandedResources()
        {
            return Set.of(variableAwareResource());
        }
    };

    /**
     * Signifies Cassandra data scope.
     */
    public static final ResourceScope DATA_SCOPE = DataResourceScope.DATA_SCOPE;

    /**
     * Signifies Cassandra data scope at a keyspace level.
     */
    public static final ResourceScope KEYSPACE_SCOPE = DataResourceScope.KEYSPACE_SCOPE;

    /**
     * Signifies Cassandra data scope at a table level.
     */
    public static final ResourceScope TABLE_SCOPE = DataResourceScope.TABLE_SCOPE;

    private static void validate(String resource)
    {
        if (isNullOrEmpty(resource))
        {
            throw new IllegalArgumentException("Resource expected for resolving");
        }
    }
}
