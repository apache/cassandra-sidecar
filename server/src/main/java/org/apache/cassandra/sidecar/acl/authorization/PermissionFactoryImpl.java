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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.vertx.ext.auth.authorization.WildcardPermissionBasedAuthorization;
import io.vertx.ext.auth.authorization.impl.WildcardPermissionBasedAuthorizationImpl;

import static org.apache.cassandra.sidecar.acl.authorization.DomainAwarePermission.WILDCARD_PART_DIVIDER_TOKEN;
import static org.apache.cassandra.sidecar.acl.authorization.FeaturePermission.ALL_FEATURE_PERMISSIONS;

/**
 * Implementation of {@link PermissionFactory}
 */
public class PermissionFactoryImpl implements PermissionFactory
{
    private static final String FORBIDDEN_PERMISSION_PREFIX = "*:";
    static final String FORBIDDEN_PREFIX_ERR_MSG
    = String.format("Permission with prefix %s are forbidden", FORBIDDEN_PERMISSION_PREFIX);
    private final List<CompositePermission> supportedFeaturePermissions;

    public PermissionFactoryImpl()
    {
        this(Collections.unmodifiableList(ALL_FEATURE_PERMISSIONS));
    }

    /**
     * Creates a {@link PermissionFactoryImpl} given a list of supported feature permissions.
     *
     * @param supportedFeaturePermissions list of feature permissions sidecar recognizes and honors
     */
    public PermissionFactoryImpl(List<CompositePermission> supportedFeaturePermissions)
    {
        this.supportedFeaturePermissions = supportedFeaturePermissions;
    }

    /**
     * @return list of supported feature permissions.
     */
    public List<CompositePermission> supportedFeaturePermissions()
    {
        return supportedFeaturePermissions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Permission createPermission(String name)
    {
        Objects.requireNonNull(name, "name cannot be null");
        if (name.startsWith(FORBIDDEN_PERMISSION_PREFIX))
        {
            throw new IllegalArgumentException(FORBIDDEN_PREFIX_ERR_MSG);
        }
        Permission permission = createFeaturePermission(name);
        if (permission != null)
        {
            return permission;
        }
        boolean isDomainAware = name.contains(WILDCARD_PART_DIVIDER_TOKEN);
        return isDomainAware ? new DomainAwarePermission(name) : new StandardPermission(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompositePermission createFeaturePermission(String name)
    {
        List<Permission> combinedPermissions = new ArrayList<>();
        WildcardPermissionBasedAuthorization requestedAuthorization = new WildcardPermissionBasedAuthorizationImpl(name);
        for (CompositePermission featurePermission : supportedFeaturePermissions())
        {
            if (requestedAuthorization.verify(featurePermission.nameAuthorization()))
            {
                combinedPermissions.addAll(featurePermission.childPermissions());
            }
        }
        return combinedPermissions.isEmpty() ? null : new CompositePermission(name, combinedPermissions);
    }
}
