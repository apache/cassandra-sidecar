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

import java.util.Set;

import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.OrAuthorization;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a permission that can be granted to a user
 */
public interface Permission
{
    /**
     * @return name of permission
     */
    String name();

    /**
     * @return resource scope this permission can act for.
     */
    @NotNull
    ResourceScope resourceScope();

    /**
     * User authorization created with resource
     *
     * @param resource resource set for authorization matching
     * @return {@link Authorization} created from permission for a resource.
     */
    Authorization toAuthorization(String resource);

    /**
     * User authorization created with expanded resources of given resource scope. When no resource scope is set,
     * {@link Authorization} is created with just permission {@link #name}
     *
     * @return {@link Authorization} created with expanded resources of associated resource scope, when expanded
     * resources are empty, {@link Authorization} is created with permission {@link #name}
     */
    default Authorization toAuthorization()
    {
        return toAuthorization(resourceScope().expandedResources());
    }

    /**
     * User authorization created with eligible resources.
     *
     * @param eligibleResources authorization is created with all the eligible resources, so that if user holds grant
     *                          for <b>any</b> of the eligibleResources, then they are granted access
     * @return {@link Authorization} created with given eligibleResources, when empty list is passed
     * {@link Authorization} is created with just permission {@link #name}.
     */
    default Authorization toAuthorization(Set<String> eligibleResources)
    {
        if (eligibleResources == null || eligibleResources.isEmpty())
        {
            return toAuthorization((String) null);
        }

        OrAuthorization orAuthorization = OrAuthorization.create();
        for (String resource : eligibleResources)
        {
            orAuthorization.addAuthorization(toAuthorization(resource));
        }
        return orAuthorization;
    }
}
