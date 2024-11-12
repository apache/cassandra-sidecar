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

package org.apache.cassandra.sidecar.config;

import java.util.List;
import java.util.Set;

/**
 * Configuration for request authentication and authorization to Sidecar API
 */
public interface AccessControlConfiguration
{
    /**
     * @return whether access control is enabled, if {@code true} requests will be authenticated and authorized
     * before allowed
     */
    boolean enabled();

    /**
     * @return configuration needed for setting up authenticators in Sidecar
     */
    List<ParameterizedClassConfiguration> authenticatorsConfiguration();

    /**
     * @return configuration needed for setting up authorizer in Sidecar
     */
    ParameterizedClassConfiguration authorizerConfiguration();

    /**
     * @return A {@code Set<String>} of administrative identities that are always authenticated and authorized
     */
    Set<String> adminIdentities();

    /**
     * @return the configuration used for creating permissions related caches
     */
    CacheConfiguration permissionCacheConfiguration();
}
