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

package org.apache.cassandra.sidecar.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;

/**
 * Class with utility methods for Authentication and Authorization.
 */
public class AuthUtils
{
    /**
     * Extracts a list of identities a user holds from their principal.
     *
     * @param user User object in Vertx
     * @return extracted identities of user
     */
    public static List<String> extractIdentities(User user)
    {
        JsonObject principal = user.principal();

        if (principal == null)
        {
            return Collections.emptyList();
        }

        List<String> identities = new ArrayList<>();

        String identity = principal.getString("identity");
        if (identity != null)
        {
            identities.add(identity);
        }

        String identitiesString = user.principal().getString("identities");
        if (identitiesString != null)
        {
            String[] parts = identitiesString.split(",");
            identities.addAll(Arrays.asList(parts));
        }
        return Collections.unmodifiableList(identities);
    }
}
