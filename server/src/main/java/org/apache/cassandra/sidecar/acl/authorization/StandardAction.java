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
import io.vertx.ext.auth.authorization.impl.PermissionBasedAuthorizationImpl;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

import static org.apache.cassandra.sidecar.acl.authorization.WildcardAction.WILDCARD_TOKEN;

/**
 * Standard actions need an exact match between allowed actions.
 */
public class StandardAction implements Action
{
    protected final String name;

    public StandardAction(String name)
    {
        if (name == null || name.isEmpty())
        {
            throw new IllegalArgumentException("Action name can not be null or empty. To allow wildcard action across "
                                               + "resources, use " + WILDCARD_TOKEN);
        }
        this.name = name;
    }

    @Override
    public Authorization toAuthorization(String resource)
    {
        PermissionBasedAuthorization authorization = new PermissionBasedAuthorizationImpl(name);
        if (resource != null && !resource.isEmpty())
        {
            authorization.setResource(resource);
        }
        return authorization;
    }
}
