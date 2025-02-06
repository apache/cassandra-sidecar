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

import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.PermissionBasedAuthorization;

import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.NO_SCOPE;
import static org.apache.cassandra.sidecar.common.utils.StringUtils.isNotEmpty;
import static org.apache.cassandra.sidecar.common.utils.StringUtils.isNullOrEmpty;

/**
 * {@link StandardPermission} needs an exact match between permission. If resource is set, exact match between
 * resources if also required.
 */
public class StandardPermission implements Permission
{
    protected final String name;
    protected final ResourceScope resourceScope;

    public StandardPermission(String name)
    {
        this(name, NO_SCOPE);
    }

    /**
     * Creates an instance of {@link StandardPermission} with given permission name and resource scope.
     *
     * @param name      permission name
     * @param scope     resource scope for permission
     */
    public StandardPermission(String name, ResourceScope scope)
    {
        if (isNullOrEmpty(name))
        {
            throw new IllegalArgumentException("Permission name can not be null or empty");
        }
        this.name = name;
        this.resourceScope = scope;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public ResourceScope resourceScope()
    {
        return resourceScope;
    }

    @Override
    public Authorization toAuthorization(String resource)
    {
        PermissionBasedAuthorization authorization = PermissionBasedAuthorization.create(name);
        if (isNotEmpty(resource))
        {
            authorization.setResource(resourceScope.resolveWithResource(resource));
        }
        return authorization;
    }
}
