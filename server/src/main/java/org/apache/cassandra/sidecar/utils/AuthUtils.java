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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.acl.authorization.Action;
import org.apache.cassandra.sidecar.acl.authorization.StandardAction;
import org.apache.cassandra.sidecar.acl.authorization.WildcardAction;

import static org.apache.cassandra.sidecar.acl.authorization.WildcardAction.WILDCARD_PART_DIVIDER_TOKEN;
import static org.apache.cassandra.sidecar.acl.authorization.WildcardAction.WILDCARD_TOKEN;

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
        validatePrincipal(user);

        return Optional.ofNullable(user.principal().getString("identity"))
                       .map(Collections::singletonList)
                       .orElseGet(() -> Arrays.asList(user.principal()
                                                          .getString("identities")
                                                          .split(",")));
    }

    /**
     * @return an instance of {@link Action} given the name
     */
    public static Action actionFromName(String name)
    {
        if (name == null)
        {
            throw new IllegalArgumentException("Name can not be null");
        }

        boolean isWildCard = name.equals(WILDCARD_TOKEN) || name.contains(WILDCARD_PART_DIVIDER_TOKEN);
        if (isWildCard)
        {
            return new WildcardAction(name);
        }
        return new StandardAction(name);
    }

    private static void validatePrincipal(User user)
    {
        if (user.principal() == null)
        {
            throw new HttpException(HttpResponseStatus.FORBIDDEN.code(), "User principal empty");
        }

        if (!user.principal().containsKey("identity") && !user.principal().containsKey("identities"))
        {
            throw new HttpException(HttpResponseStatus.FORBIDDEN.code(), "No valid identity found for authorizing");
        }
    }
}
