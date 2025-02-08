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
import java.util.List;

import io.vertx.ext.auth.authorization.AndAuthorization;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.WildcardPermissionBasedAuthorization;
import io.vertx.ext.auth.authorization.impl.WildcardPermissionBasedAuthorizationImpl;

/**
 * Represents a collection of permissions that can be combined and assigned together. This is mainly used to expand
 * feature level permissions granted for users. Feature level permissions grant access to a set of related features.
 * Features may involve accessing multiple handlers each requiring individual permissions. By grouping permissions,
 * feature level permissions simplify assigning access control for features.
 */
public class CompositePermission extends StandardPermission
{
    private final WildcardPermissionBasedAuthorization nameAuthorization;
    private final List<Permission> permissions;

    /**
     * Creates a {@link CompositePermission} with given permission name and list of child permissions.
     *
     * @param name          permission name
     * @param permissions   list of child permissions
     */
    public CompositePermission(String name, List<Permission> permissions)
    {
        super(name);
        if (permissions == null || permissions.isEmpty())
        {
            throw new IllegalArgumentException("CompositePermission can not be created with null or empty permissions");
        }
        this.nameAuthorization = new WildcardPermissionBasedAuthorizationImpl(name);
        this.permissions = Collections.unmodifiableList(permissions);
    }

    /**
     * @return {@link WildcardPermissionBasedAuthorization} created from permission name. {@link #nameAuthorization}
     * can be used for finding match between {@link CompositePermission}
     */
    public WildcardPermissionBasedAuthorization nameAuthorization()
    {
        return nameAuthorization;
    }

    /**
     * @return {@code List} of child permissions composed within {@link CompositePermission}
     */
    public List<Permission> childPermissions()
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
