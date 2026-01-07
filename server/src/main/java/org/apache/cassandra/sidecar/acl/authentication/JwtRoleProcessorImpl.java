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

package org.apache.cassandra.sidecar.acl.authentication;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;

/**
 * {@link JwtRoleProcessor} implementation.
 */
public class JwtRoleProcessorImpl implements JwtRoleProcessor
{
    private static final String SUB_KEY = "sub";
    private static final String IDENTITY_KEY = "identity";
    private static final String IDENTITIES_KEY = "identities";
    private final IdentityToRoleCache identityToRoleCache;

    public JwtRoleProcessorImpl(IdentityToRoleCache identityToRoleCache)
    {
        this.identityToRoleCache = identityToRoleCache;
    }

    @Override
    public Future<List<String>> processRoles(JsonObject decodedToken)
    {
        String identity;
        if ((identity = decodedToken.getString(IDENTITY_KEY)) != null || (identity = decodedToken.getString(SUB_KEY)) != null)
        {
            return identityToRoleCache.get(identity).map(role -> role != null ? List.of(role) : List.of());
        }

        JsonArray identityKeyArray = decodedToken.getJsonArray(IDENTITIES_KEY);
        if (identityKeyArray == null)
        {
            return Future.succeededFuture(List.of());
        }
        List<Future<String>> roleFutures = new ArrayList<>();
        // noinspection unchecked
        for (String i : (List<String>) identityKeyArray.getList())
        {
            roleFutures.add(identityToRoleCache.get(i));
        }
        return Future.all(roleFutures)
                     .map(compositeFuture -> {
                         List<String> roles = new ArrayList<>(identityKeyArray.size());
                         for (int i = 0; i < compositeFuture.size(); i++)
                         {
                             String roleFromIdentity = compositeFuture.resultAt(i);
                             if (roleFromIdentity != null)
                             {
                                 roles.add(roleFromIdentity);
                             }
                         }
                         return List.copyOf(roles);
                     });
    }
}
