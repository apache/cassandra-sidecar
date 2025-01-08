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

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.KEYSPACE;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.TABLE;

/**
 * Resources sidecar can expect permissions for. This list is not exhaustive.
 */
public enum VariableAwareResource
{
    CLUSTER("cluster"),

    SIDECAR("sidecar"),

    /**
     * Cassandra stores data resource in the format data/keyspace_name/table_name within the role_permissions table.
     * A similar format is followed for storing data resource in sidecar permissions table role_permissions_v1. hence
     * sidecar endpoints expect data resources to be provided in format data/keyspace_name/table_name.
     * <p>
     * In this context, curly braces are used to denote variable parts of the resource. For e.g., when permissions are
     * checked for resource data/{keyspace} in an endpoint, the part within the curly braces ({keyspace})
     * represents a placeholder for the actual keyspace. For more context refer to
     * {@link io.vertx.ext.auth.authorization.impl.VariableAwareExpression}
     * <p>
     * During the permission matching process, the placeholder {keyspace} is resolved to the actual keyspace
     * being accessed by the endpoint. For e.g. data/{keyspace} resolves to data/university if the keyspace is
     * "university".
     * <p>
     * User permissions are then extracted from both Cassandra and sidecar role permissions tables for
     * the resolved resource and are matched against the expected permissions set defined in the endpoint's handler.
     */
    DATA_WITH_KEYSPACE(String.format("data/{%s}", KEYSPACE)),
    DATA_WITH_KEYSPACE_TABLE(String.format("data/{%s}/{%s}", KEYSPACE, TABLE));

    private final String resource;

    VariableAwareResource(String resource)
    {
        this.resource = resource;
    }

    public String resource()
    {
        return resource;
    }
}
