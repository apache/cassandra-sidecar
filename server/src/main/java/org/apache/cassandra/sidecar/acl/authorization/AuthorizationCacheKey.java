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

import io.vertx.ext.auth.authorization.AuthorizationContext;

/**
 * {@link AuthorizationCacheKey} is used to uniquely represent an authorization request using both user context
 * and resource context.
 */
public interface AuthorizationCacheKey
{
    /**
     * Creates an instance of {@link AuthorizationCacheKey} using Vert.x's {@link AuthorizationContext}.
     * (Note: {@link AuthorizationCacheKey} is required because {@link AuthorizationContext} does not have equals
     * comparison)
     *
     * @param handlerId            handlerId uniquely represents the authorization handler used for a route
     * @param authorizationContext instance of {@link AuthorizationContext}. Used by Vert.x to represent an
     *                             authorization request.
     * @return {@link AuthorizationCacheKey} uniquely represents an authorization request using user and resource
     * context.
     */
    static AuthorizationCacheKey create(int handlerId, AuthorizationContext authorizationContext)
    {
        return new AuthorizationCacheKeyImpl(handlerId, authorizationContext.user(), authorizationContext.variables());
    }
}
