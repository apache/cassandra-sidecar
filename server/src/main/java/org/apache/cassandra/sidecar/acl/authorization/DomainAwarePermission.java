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

import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.WildcardPermissionBasedAuthorization;
import io.vertx.ext.auth.authorization.impl.WildcardPermissionBasedAuthorizationImpl;

import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.NO_SCOPE;
import static org.apache.cassandra.sidecar.common.utils.StringUtils.isNotEmpty;

/**
 * Domain aware permissions allow grouping allowed permissions for a domain. They can be represented with
 * ':' wildcard parts divider. Wildcard sub parts divider token ',' can be used to group actions for a domain.
 * Wildcard token '*' is restricted in {@link DomainAwarePermission} to avoid unpredictable behaviour. Majority
 * of sidecar permissions are represented in format {@code domain}:{@code action}.
 * <p>
 * Example, with SNAPSHOT:CREATE permission, CREATE action is allowed for the SNAPSHOT domain. Sample actions are
 * CREATE, READ, EDIT, UPDATE, DELETE, IMPORT, UPLOAD, START, ABORT etc.
 * <p>
 * Some examples of wildcard permissions are:
 * - SNAPSHOT:CREATE,READ,DELETE allows SNAPSHOT:CREATE, SNAPSHOT:READ and SNAPSHOT:DELETE.
 */
public class DomainAwarePermission extends StandardPermission
{
    public static final String WILDCARD_TOKEN = "*";
    public static final String WILDCARD_PART_DIVIDER_TOKEN = ":";

    public DomainAwarePermission(String name)
    {
        this(name, NO_SCOPE);
    }

    /**
     * Creates an instance of {@link DomainAwarePermission} with given permission name and resource scope.
     *
     * @param name      permission name
     * @param scope     resource scope for permission
     */
    public DomainAwarePermission(String name, ResourceScope scope)
    {
        super(name, scope);
        validate(name);
    }

    private void validate(String name)
    {
        if (name.contains(WILDCARD_TOKEN))
        {
            throw new IllegalArgumentException("DomainAwarePermission can not have " + WILDCARD_TOKEN +
                                               " to avoid unpredictable behavior");
        }
        if (!name.contains(WILDCARD_PART_DIVIDER_TOKEN))
        {
            throw new IllegalArgumentException("DomainAwarePermission must have " + WILDCARD_PART_DIVIDER_TOKEN +
                                               " to divide domain and action");
        }
        String[] wildcardParts = name.split(WILDCARD_PART_DIVIDER_TOKEN);
        boolean hasEmptyParts = Arrays.stream(wildcardParts).anyMatch(String::isEmpty);
        if (wildcardParts.length == 0 || hasEmptyParts)
        {
            throw new IllegalArgumentException("DomainAwarePermission parts can not be empty");
        }
    }

    @Override
    public Authorization toAuthorization(String resource)
    {
        WildcardPermissionBasedAuthorization authorization = new WildcardPermissionBasedAuthorizationImpl(name);
        if (isNotEmpty(resource))
        {
            authorization.setResource(resourceScope.resolveWithResource(resource));
        }
        return authorization;
    }
}
