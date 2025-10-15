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

import java.util.List;
import java.util.Objects;

import io.vertx.core.MultiMap;
import io.vertx.ext.auth.User;

import static org.apache.cassandra.sidecar.utils.AuthUtils.extractCassandraRoles;

/**
 * Implementation of {@link AuthorizationCacheKey}, uniquely represents an authorization request with user and resource
 * context.
 */
public class AuthorizationCacheKeyImpl implements AuthorizationCacheKey
{
    private final List<String> roles;
    private final MultiMap variables;

    public AuthorizationCacheKeyImpl(User user, MultiMap variables)
    {
        this.roles = extractCassandraRoles(user);
        // Store a copy, otherwise Cache is not able to identity 2 keys with same values in MultiMap as same.
        // If MultiMap is modifiable the equality behaviour changes.
        this.variables = MultiMap.caseInsensitiveMultiMap();
        if (variables != null)
        {
            variables.forEach(entry -> this.variables.add(entry.getKey(), entry.getValue()));
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        AuthorizationCacheKeyImpl that = (AuthorizationCacheKeyImpl) o;
        return roles.equals(that.roles) && variablesEqual(variables, that.variables);
    }

    private boolean variablesEqual(MultiMap map1, MultiMap map2)
    {
        if (map1 == map2)
        {
            return true;
        }
        if (map1 == null || map2 == null)
        {
            return false;
        }
        if (map1.size() != map2.size())
        {
            return false;
        }

        for (String name : map1.names())
        {
            if (!map2.contains(name) || !map1.getAll(name).equals(map2.getAll(name)))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(roles, variablesHashCode(variables));
    }

    private int variablesHashCode(MultiMap variables)
    {
        if (variables == null)
        {
            return 0;
        }
        int hash = 0;
        for (String name : variables.names())
        {
            hash += Objects.hash(name, variables.getAll(name));
        }
        return hash;
    }
}
