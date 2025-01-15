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

import static org.apache.cassandra.sidecar.common.utils.StringUtils.isNotEmpty;

/**
 * Wildcard permissions allow grouping allowed permissions. They can be represented with ':' wildcard parts divider
 * to divide wildcard parts and '*' wildcard token for matching wildcard parts. Majority of sidecar permissions are
 * represented in format {@code domain}:{@code action}.
 * <p>
 * Example, with SNAPSHOT:CREATE permission, the CREATE action is allowed for the SNAPSHOT domain. Sample actions are
 * CREATE, READ, EDIT, UPDATE, DELETE, IMPORT, UPLOAD, START, ABORT etc.
 * <p>
 * Some examples of wildcard permissions are:
 * - SNAPSHOT:* allows SNAPSHOT:CREATE, SNAPSHOT:VIEW and SNAPSHOT:DELETE.
 * - *:CREATE allows the CREATE action on all possible targets.
 * - *:* allows all possible permissions for specified all domains
 */
public class WildcardPermission extends StandardPermission
{
    public static final String WILDCARD_TOKEN = "*";
    public static final String WILDCARD_PART_DIVIDER_TOKEN = ":";

    public WildcardPermission(String name)
    {
        super(name);
        if (!name.contains(WILDCARD_TOKEN) && !name.contains(WILDCARD_PART_DIVIDER_TOKEN))
        {
            throw new IllegalArgumentException("Wildcard permissions must either have wildcard token " + WILDCARD_TOKEN
                                               + " or must be divided into wildcard parts");
        }
        validate(name);
    }

    private void validate(String name)
    {
        String[] wildcardParts = name.split(WILDCARD_PART_DIVIDER_TOKEN);
        boolean hasEmptyParts = Arrays.stream(wildcardParts).anyMatch(String::isEmpty);
        if (wildcardParts.length == 0 || hasEmptyParts)
        {
            throw new IllegalArgumentException("Wildcard permission parts can not be empty");
        }
    }

    @Override
    public Authorization toAuthorization(String resource)
    {
        WildcardPermissionBasedAuthorization authorization = new WildcardPermissionBasedAuthorizationImpl(name);
        if (isNotEmpty(resource))
        {
            authorization.setResource(resource);
        }
        return authorization;
    }
}
