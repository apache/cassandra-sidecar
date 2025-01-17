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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.KEYSPACE;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.TABLE;

/**
 * Resources sidecar can expect permissions for. This list is not exhaustive.
 */
public enum VariableAwareResource
{
    /**
     * Signifies the Cassandra cluster resource. For example, to determine whether you have access to retrieve
     * basic Cassandra ring, gossip, or other Cassandra-related information.
     */
    CLUSTER("cluster"),

    /**
     * Signifies the Cassandra Sidecar operations resource which allows a Sidecar operator with sufficient permissions
     * run operations against both Cassandra Sidecar and Cassandra clusters that Sidecar is managing.
     */
    OPERATION("operation"),

    // data resources

    /**
     * Cassandra stores data resource in the format data, data/keyspace or data/keyspace_name/table_name within
     * the role_permissions table. A similar format is followed for storing data resources in sidecar permissions
     * table role_permissions_v1. Hence, sidecar endpoints expect data resources to be provided in format
     * data/keyspace_name/table_name.
     * <p>
     * In this context, curly braces are used to denote variable parts of the resource. For e.g., when permissions are
     * checked for resource data/{keyspace} in an endpoint, the part within the curly braces ({keyspace})
     * represents a placeholder for the actual keyspace name provided as a path parameter. For more context refer to
     * io.vertx.ext.auth.authorization.impl.VariableAwareExpression
     * <p>
     * During the permission matching process, the placeholder {keyspace} is resolved to the actual keyspace
     * being accessed by the endpoint. For e.g. data/{keyspace} resolves to data/university if the keyspace is
     * "university".
     * <p>
     * User permissions are then extracted from both Cassandra and sidecar role permissions tables for
     * the resolved resource and are matched against the expected permissions set defined in the endpoint's handler.
     */
    DATA("data"),

    DATA_WITH_KEYSPACE(String.format("data/{%s}", KEYSPACE),
                       // can expand to DATA
                       DATA),

    // TODO remove this hack once VariableAwareExpression bug is fixed
    // VariableAwareExpression in vertx-auth-common package has a bug during String.substring() call, hence
    // we cannot set resources that do not end in curly braces (e.g. data/keyspace/*) in
    // PermissionBasedAuthorizationImpl or WildcardPermissionBasedAuthorizationImpl. data/{%s}/{TABLE_WILDCARD} treats
    // TABLE_WILDCARD as a variable. This hack allows to read resource level permissions that could be set for all
    // tables through data/<keyspace_name>/*. Bug should be fixed in 4.5.12
    // Note: DATA_WITH_KEYSPACE_ALL_TABLES resource comprises all tables under the keyspace excluding the keyspace itself
    DATA_WITH_KEYSPACE_ALL_TABLES(String.format("data/{%s}/{TABLE_WILDCARD}", KEYSPACE),
                                  // can expand to DATA, DATA_WITH_KEYSPACE
                                  DATA, DATA_WITH_KEYSPACE),

    DATA_WITH_KEYSPACE_TABLE(String.format("data/{%s}/{%s}", KEYSPACE, TABLE),
                             // can expand to DATA, DATA_WITH_KEYSPACE and DATA_WITH_KEYSPACE_ALL_TABLES
                             DATA, DATA_WITH_KEYSPACE, DATA_WITH_KEYSPACE_ALL_TABLES);

    private final String resource;
    private final List<String> expandedResources;

    VariableAwareResource(String resource, VariableAwareResource... expansions)
    {
        this.resource = resource;
        List<String> expandedResources = Arrays.stream(expansions).map(r -> r.resource).collect(Collectors.toList());
        expandedResources.add(resource);
        this.expandedResources = Collections.unmodifiableList(expandedResources);
    }

    public String resource()
    {
        return resource;
    }

    /**
     * @return the list of expanded resources eligible to match the resources a user holds
     */
    public List<String> expandedResources()
    {
        return expandedResources;
    }
}
