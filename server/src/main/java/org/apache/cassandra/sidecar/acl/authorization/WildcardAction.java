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

/**
 * Wildcard actions allow grouping allowed actions
 */
public class WildcardAction extends StandardAction
{
    public static final String WILDCARD_TOKEN = "*";
    public static final String WILDCARD_PART_DIVIDER_TOKEN = ":";

    public WildcardAction(String name)
    {
        super(name);
        if (!name.contains(WILDCARD_TOKEN) && !name.contains(WILDCARD_PART_DIVIDER_TOKEN))
        {
            throw new IllegalArgumentException("Wildcard actions must either have wildcard token " + WILDCARD_TOKEN
                                               + " or must have wildcard parts");
        }
        validate(name);
    }

    private void validate(String name)
    {
        String[] wildcardParts = name.split(WILDCARD_PART_DIVIDER_TOKEN);
        boolean hasEmptyParts = Arrays.stream(wildcardParts).anyMatch(String::isEmpty);
        if (wildcardParts.length == 0 || hasEmptyParts)
        {
            throw new IllegalArgumentException("Wildcard action parts can not be empty");
        }
    }

    @Override
    public Authorization toAuthorization(String resource)
    {
        WildcardPermissionBasedAuthorization authorization = new WildcardPermissionBasedAuthorizationImpl(name);
        if (resource != null && !resource.isEmpty())
        {
            authorization.setResource(resource);
        }
        return authorization;
    }
}
