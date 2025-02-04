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

/**
 * Resource scope that can be set in permissions.
 */
public interface ResourceScope
{
    /**
     * @return variable aware resource built with this scope, to be used in Handlers for building required
     * authorizations. Variables in a resource can be denoted by enclosing them in curly braces. For e.g. for data
     * resource scoped at keyspace level, variableAwareResource returned would be data/{keyspace}. These variables are
     * populated when a request is received using request parameters and authorizations are extracted for resolved
     * resource.
     */
    String variableAwareResource();

    /**
     * Given a resource builds resolved resource for this scope.
     *
     * @param resource resource set
     * @return resolved resource built with set scope and given resource. Mainly used for feature level
     * permissions to build resource for child permissions with child permission's scope and parent
     * permission's resource.
     */
    String resolveWithResource(String resource);

    /**
     * @return {@code Set} of expanded resources this resource scope can accept authorization for.
     */
    Set<String> expandedResources();
}
