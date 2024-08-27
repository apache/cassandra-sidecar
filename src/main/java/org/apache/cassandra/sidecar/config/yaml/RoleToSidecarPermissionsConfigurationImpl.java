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

package org.apache.cassandra.sidecar.config.yaml;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.ext.auth.authorization.AndAuthorization;
import io.vertx.ext.auth.authorization.RoleBasedAuthorization;
import org.apache.cassandra.sidecar.auth.authorization.MutualTlsPermissions;
import org.apache.cassandra.sidecar.config.RoleToSidecarPermissionsConfiguration;

/**
 * {@inheritDoc}
 */
public class RoleToSidecarPermissionsConfigurationImpl implements RoleToSidecarPermissionsConfiguration
{
    private static final String DEFAULT_ROLE = "";
    private static final List<Map<MutualTlsPermissions, List<String>>> DEFAULT_PERMISSIONS = new ArrayList<>();

    @JsonProperty(value = "role")
    protected final String role;
    @JsonProperty(value = "permissions")
    protected final List<Map<MutualTlsPermissions, List<String>>> permissions;

    public RoleToSidecarPermissionsConfigurationImpl()
    {
        this(DEFAULT_ROLE, DEFAULT_PERMISSIONS);
    }

    public RoleToSidecarPermissionsConfigurationImpl(String role, List<Map<MutualTlsPermissions, List<String>>> permissions)
    {
        this.role = role;
        this.permissions = permissions;
    }

    /**
     * {@inheritDoc}
     */
    public String role()
    {
        return role;
    }

    /**
     * {@inheritDoc}
     */
    public AndAuthorization permissions()
    {
        AndAuthorization permission = AndAuthorization.create();

        for (Map<MutualTlsPermissions, List<String>> permissionToResources : permissions)
        {
            for (Map.Entry<MutualTlsPermissions, List<String>> permissionsToResources : permissionToResources.entrySet())
            {
                for (String resource : permissionsToResources.getValue())
                {
                    RoleBasedAuthorization authorization = RoleBasedAuthorization.create(permissionsToResources.getKey().name())
                                                                                 .setResource(resource);
                    permission.addAuthorization(authorization);
                }
            }
        }

        return permission;
    }
}
