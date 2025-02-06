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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.KEYSPACE;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.TABLE;
import static org.apache.cassandra.sidecar.common.utils.StringUtils.isNullOrEmpty;

/**
 * Signifies scope of Cassandra data resource. {@code keyspaceScoped} can be set to true to create data scope
 * restricted to a keyspace. {@code tableScoped} can be set to true to create data scope restricted to a table.
 */
public class DataResourceScope implements ResourceScope
{
    public static final Pattern DATA_RESOURCE_PATTERN = Pattern.compile("^data(?:/([^/]+))?(?:/([^/]+))?$");

    public static final String DATA = "data";

    /**
     * Cassandra stores data resource in the format data, data/keyspace or data/keyspace_name/table_name within
     * role_permissions table. A similar format is followed for storing data resources in sidecar permissions
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
    public static final String DATA_WITH_KEYSPACE = String.format("data/{%s}", KEYSPACE);

    // TODO remove this hack once VariableAwareExpression bug is fixed
    // VariableAwareExpression in vertx-auth-common package has a bug during String.substring() call, hence
    // we cannot set resources that do not end in curly braces (e.g. data/keyspace/*) in
    // PermissionBasedAuthorizationImpl or WildcardPermissionBasedAuthorizationImpl. data/{%s}/{TABLE_WILDCARD} treats
    // TABLE_WILDCARD as a variable. This hack allows to read resource level permissions that could be set for all
    // tables through data/<keyspace_name>/*. Bug should be fixed in 4.5.12
    // Note: DATA_WITH_KEYSPACE_ALL_TABLES authorizes for all tables under the keyspace excluding the keyspace itself
    public static final String DATA_WITH_KEYSPACE_ALL_TABLES = String.format("data/{%s}/{TABLE_WILDCARD}", KEYSPACE);

    public static final String DATA_WITH_KEYSPACE_TABLE = String.format("data/{%s}/{%s}", KEYSPACE, TABLE);

    // scoped at data level including all keyspaces and tables
    public static final DataResourceScope DATA_SCOPE = new DataResourceScope(false, false);
    // scoped at keyspace level within data
    public static final DataResourceScope KEYSPACE_SCOPE = new DataResourceScope(true, false);
    // scoped at table level within a keyspace
    public static final DataResourceScope TABLE_SCOPE = new DataResourceScope(true, true);

    private final boolean keyspaceScoped;
    private final boolean tableScoped;
    private final Set<String> expandedResources;

    private DataResourceScope(boolean keyspaceScoped, boolean tableScoped)
    {
        this.keyspaceScoped = keyspaceScoped;
        this.tableScoped = tableScoped;
        this.expandedResources = initializeExpandedResources();
    }

    private Set<String> initializeExpandedResources()
    {
        if (tableScoped)
        {
            // can expand to DATA, DATA_WITH_KEYSPACE and DATA_WITH_KEYSPACE_ALL_TABLES
            return Set.of(DATA, DATA_WITH_KEYSPACE, DATA_WITH_KEYSPACE_ALL_TABLES, DATA_WITH_KEYSPACE_TABLE);
        }
        if (keyspaceScoped)
        {
            // can expand to DATA
            return Set.of(DATA, DATA_WITH_KEYSPACE);
        }
        return Collections.singleton(DATA);
    }

    @Override
    public String variableAwareResource()
    {
        if (tableScoped)
        {
            return DATA_WITH_KEYSPACE_TABLE;
        }
        if (keyspaceScoped)
        {
            return DATA_WITH_KEYSPACE;
        }
        return DATA;
    }

    @Override
    public String resolveWithResource(String resource)
    {
        Matcher matcher = validate(resource);
        if (tableScoped)
        {
            return resource;
        }
        else if (keyspaceScoped)
        {
            // if table is present, we create resource with just keyspace
            return matcher.group(2) != null ? DATA + "/" + matcher.group(1) : resource;
        }
        return DATA;
    }

    @Override
    public Set<String> expandedResources()
    {
        return expandedResources;
    }

    /**
     * Verifies resource is valid and returns a matcher if resource is valid.
     *
     * @param resource resource
     * @return matcher matching {@link #DATA_RESOURCE_PATTERN}
     */
    private Matcher validate(String resource)
    {
        if (isNullOrEmpty(resource))
        {
            throw new IllegalArgumentException("Resource expected for resolving");
        }

        Matcher matcher = DATA_RESOURCE_PATTERN.matcher(resource);
        if (!matcher.matches())
        {
            String errMsg = String.format("Resource %s does not match expected data resource scope format %s",
                                          resource, DATA_RESOURCE_PATTERN.pattern());
            throw new IllegalArgumentException(errMsg);
        }
        return matcher;
    }
}
