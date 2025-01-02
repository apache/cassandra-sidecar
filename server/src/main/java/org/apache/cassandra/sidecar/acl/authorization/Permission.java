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

/**
 * Represents a permission that can be granted to a user on a resource.
 */
public interface Permission
{
    /**
     * @return name of permission
     */
    String name();

    /**
     * @return {@link Authorization}. Most sidecar endpoints require a resource. This method is used in testing
     */
    default Authorization toAuthorization()
    {
        return toAuthorization(null);
    }

    /**
     * @return {@link Authorization} created for a resource
     */
    Authorization toAuthorization(String resource);
}
