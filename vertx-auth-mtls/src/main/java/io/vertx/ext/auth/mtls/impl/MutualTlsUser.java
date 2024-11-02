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

package io.vertx.ext.auth.mtls.impl;

import java.util.List;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.impl.UserImpl;

/**
 * {@link io.vertx.ext.auth.User} implementation that represents a user authenticated with mutual TLS.
 */
public class MutualTlsUser extends UserImpl
{
    private final List<String> identities;

    public MutualTlsUser(List<String> identities)
    {
        super(new JsonObject().put("identities", String.join(",", identities)), new JsonObject());
        this.identities = identities;
    }

    /**
     * @return valid identities extracted from user certificate
     */
    public List<String> identities()
    {
        return identities;
    }

    public static MutualTlsUser fromIdentities(List<String> identities)
    {
        return new MutualTlsUser(identities);
    }
}
