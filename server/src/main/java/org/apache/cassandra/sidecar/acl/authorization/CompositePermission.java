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

import io.vertx.ext.auth.authorization.AndAuthorization;
import io.vertx.ext.auth.authorization.Authorization;
import org.apache.cassandra.sidecar.common.utils.Preconditions;

/**
 * {@link CompositePermission} is composed of basic permissions or other composite permissions. It is to represent
 * feature level permissions such as BULK_READ, BULK_WRITE etc. that could contain basic permissions such as
 * CREATE_SNAPSHOT, DELETE_SNAPSHOT etc.
 */
public class CompositePermission extends WildcardPermission
{
    private final Set<Permission> permissions;

    public CompositePermission(String name, Set<Permission> permissions)
    {
        super(name);
        Preconditions.checkArgument(permissions != null && !permissions.isEmpty(),
                                    "CompositePermission can not be created with null or empty permissions");
        this.permissions = Collections.unmodifiableSet(permissions);
    }

    public Set<Permission> permissions()
    {
        return permissions;
    }

    @Override
    public Authorization toAuthorization(String resource)
    {
        AndAuthorization authorization = AndAuthorization.create();
        for (Permission permission : permissions)
        {
            authorization.addAuthorization(permission.toAuthorization(resource));
        }
        return authorization;
    }
}