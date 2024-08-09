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

package org.apache.cassandra.sidecar.auth.authorization;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vertx.ext.auth.authorization.AndAuthorization;
import io.vertx.ext.auth.authorization.OrAuthorization;
import io.vertx.ext.auth.authorization.RoleBasedAuthorization;

/**
 * Required Permissions Provider where we get the required permissions from the configuration. This
 * implementation uses a mapping from generic endpoints to an {@code AndAuthorization} and sets the resource
 * with the given parameters.
 */
public class RequiredPermissionsProvider
{
    private final Map<String, List<MutualTlsPermissions>> endpointsToPermissions;

    public RequiredPermissionsProvider()
    {
        endpointsToPermissions = new HashMap<>();
    }

    public void putPermissionsMapping(String endpoint, List<MutualTlsPermissions> permissions)
    {
        endpointsToPermissions.put(endpoint, permissions);
    }

    public AndAuthorization requiredPermissions(String endpoint, Map<String, String> params)
    {
        List<MutualTlsPermissions> permissions = endpointsToPermissions.get(endpoint);
        if (permissions == null) return AndAuthorization.create();
        String resource = "";
        AndAuthorization requiredAuthorizations = AndAuthorization.create();
        if (params.get("keyspace") != null && params.get("table") != null)
        {
            resource = "<table " + params.get("keyspace") + "." + params.get("table") + ">";
        }
        else if (params.get("keyspace") != null)
        {
            resource = "<keyspace " + params.get("keyspace") + ">";
        }

        for (MutualTlsPermissions permission : permissions)
        {
            OrAuthorization requiredResourceOrAll = OrAuthorization.create();
            if (!resource.isEmpty())
            {
                requiredResourceOrAll.addAuthorization(RoleBasedAuthorization.create(permission.name()).setResource(resource));
            }
            requiredResourceOrAll.addAuthorization(RoleBasedAuthorization.create(permission.name()).setResource("<ALL KEYSPACES>"));
            requiredAuthorizations.addAuthorization(requiredResourceOrAll);
        }
        return requiredAuthorizations;
    }
}
