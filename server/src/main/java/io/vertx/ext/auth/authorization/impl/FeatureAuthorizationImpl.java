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

package io.vertx.ext.auth.authorization.impl;

import java.util.List;
import java.util.Objects;

import io.vertx.core.MultiMap;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.AndAuthorization;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.AuthorizationContext;
import io.vertx.ext.auth.authorization.FeatureAuthorization;
import io.vertx.ext.auth.authorization.PermissionBasedAuthorization;
import io.vertx.ext.auth.authorization.WildcardPermissionBasedAuthorization;

public class FeatureAuthorizationImpl extends AndAuthorizationImpl implements FeatureAuthorization
{
    private static final String RESOURCE_SEPARATOR = ",";
    private static final String RESOURCE_KEY_VALUE_SEPARATOR = "=";
    private String resource;

    public FeatureAuthorizationImpl(List<Authorization> authorizations)
    {
        super();
        for (Authorization authorization : authorizations)
        {
            boolean isResourceBasedAuthorization = authorization instanceof PermissionBasedAuthorization
                                                   || authorization instanceof WildcardPermissionBasedAuthorization
                                                   || authorization instanceof FeatureAuthorization;
            if (!isResourceBasedAuthorization)
            {
                throw new UnsupportedOperationException("FeatureAuthorization does not support composing non resource" +
                                                        " based authorizations");
            }
            super.addAuthorization(authorization);
        }
    }

    @Override
    public AndAuthorization addAuthorization(Authorization authorization)
    {
        throw new UnsupportedOperationException("All authorizations for FeatureAuthorization must be set during initialization");
    }

    @Override
    public String getResource()
    {
        return resource;
    }

    @Override
    public FeatureAuthorization setResource(String resource)
    {
        Objects.requireNonNull(resource);
        this.resource = resource;

        AuthorizationContext context = createContext(resource);
        resolveResourceForComposedAuthorizations(context);
        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        FeatureAuthorizationImpl that = (FeatureAuthorizationImpl) o;
        return Objects.equals(authorizations, that.authorizations) && Objects.equals(resource, that.resource);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), authorizations, resource);
    }

    private AuthorizationContext createContext(String composedResource)
    {
        String[] resources = composedResource.split(RESOURCE_SEPARATOR);
        MultiMap variables = MultiMap.caseInsensitiveMultiMap();
        for (String resource : resources)
        {
            String[] parts = resource.split(RESOURCE_KEY_VALUE_SEPARATOR);
            if (parts.length == 2)
            {
                variables.add(parts[0], parts[1]);
            }
        }
        return new AuthorizationContext()
        {
            public User user()
            {
                return null;
            }

            public MultiMap variables()
            {
                return variables;
            }
        };
    }

    private void resolveResourceForComposedAuthorizations(AuthorizationContext context)
    {
        for (Authorization authorization : authorizations)
        {
            resolveResourceForAuthorization(authorization, context);
        }
    }

    private <T extends Authorization> void resolveResourceForAuthorization(T authorization,
                                                                           AuthorizationContext context)
    {
        if (authorization instanceof PermissionBasedAuthorization)
        {
            PermissionBasedAuthorization permissionBasedAuthorization = (PermissionBasedAuthorization) authorization;
            VariableAwareExpression awareResource
            = new VariableAwareExpression(permissionBasedAuthorization.getResource());
            String resolvedResource = awareResource.resolve(context);
            permissionBasedAuthorization.setResource(resolvedResource);
        }
        else if (authorization instanceof WildcardPermissionBasedAuthorization)
        {
            WildcardPermissionBasedAuthorization wildcardAuthorization
            = (WildcardPermissionBasedAuthorization) authorization;
            VariableAwareExpression awareResource = new VariableAwareExpression(wildcardAuthorization.getResource());
            String resolvedResource = awareResource.resolve(context);
            wildcardAuthorization.setResource(resolvedResource);
        }
        else if (authorization instanceof FeatureAuthorization)
        {
            FeatureAuthorization featureAuthorization = (FeatureAuthorization) authorization;
            featureAuthorization.setResource(resource);
        }
    }
}
