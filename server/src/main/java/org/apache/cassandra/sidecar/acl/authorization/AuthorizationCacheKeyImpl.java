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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.vertx.ext.auth.User;

import static org.apache.cassandra.sidecar.utils.AuthUtils.extractCassandraRoles;

/**
 * Implementation of {@link AuthorizationCacheKey}, uniquely represents an authorization request with user and resource
 * context.
 */
public class AuthorizationCacheKeyImpl implements AuthorizationCacheKey
{
    private final List<String> roles;
    private final Set<String> variables;
    private final int hashCode;

    public AuthorizationCacheKeyImpl(User user, Iterable<Map.Entry<String, String>> variables)
    {
        this.roles = extractCassandraRoles(user);
        // Vert.x HeadersMultimap and HeadersMultimap.MapEntry does not implement equals or hashCode,
        // hence we store flattened variables in a Set
        this.variables = new HashSet<>();
        if (variables == null || !variables.iterator().hasNext())
        {
            this.hashCode = Objects.hash(this.roles, this.variables);
            return;
        }

        for (Map.Entry<String, String> entry : variables)
        {
            // We convert to lower case, since Vert.x Multimap representation is case insensitive for variables stored
            this.variables.add(entry.getKey().toLowerCase() + ":" +entry.getValue());
        }
        this.hashCode = Objects.hash(this.roles, this.variables);
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
        return roles.equals(that.roles) && variables.equals(that.variables);
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }
}
