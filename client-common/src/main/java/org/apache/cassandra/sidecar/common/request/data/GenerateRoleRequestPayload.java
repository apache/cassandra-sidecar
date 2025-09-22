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

package org.apache.cassandra.sidecar.common.request.data;

import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class GenerateRoleRequestPayload
{
    public static final String ROLE_NAME_OPTIONS_KEY_NAME = "role_name_options";
    public static final String SIDECAR_GENERATION_KEY_NAME = "sidecar_generation";
    public static final String PASSWORD_GENERATION_KEY_NAME = "password_generation";
    public static final String LOGIN_KEY_NAME = "login";
    public static final String SUPERUSER_KEY_NAME = "superuser";

    @JsonProperty(ROLE_NAME_OPTIONS_KEY_NAME)
    public final Map<String, String> roleNameOptions;

    @JsonProperty(SIDECAR_GENERATION_KEY_NAME)
    public final boolean sidecarGeneration;

    @JsonProperty(PASSWORD_GENERATION_KEY_NAME)
    public final boolean passwordGeneration;

    @JsonProperty(LOGIN_KEY_NAME)
    public final boolean login;

    @JsonProperty(SUPERUSER_KEY_NAME)
    public final boolean superuser;

    @JsonCreator
    public GenerateRoleRequestPayload(Map<String, String> config,
                                      boolean sidecarGeneration,
                                      boolean passwordGeneration,
                                      boolean login,
                                      boolean superuser)
    {
        this.roleNameOptions = config;
        this.sidecarGeneration = sidecarGeneration;
        this.passwordGeneration = passwordGeneration;
        this.login = login;
        this.superuser = superuser;
    }

    public Map<String, String> roleNameOptions()
    {
        return roleNameOptions;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GenerateRoleRequestPayload payload = (GenerateRoleRequestPayload) o;
        return sidecarGeneration == payload.sidecarGeneration
               && passwordGeneration == payload.passwordGeneration
               && login == payload.login
               && superuser == payload.superuser
               && Objects.equals(roleNameOptions, payload.roleNameOptions);
    }

    public int hashCode()
    {
        return Objects.hash(roleNameOptions, sidecarGeneration, passwordGeneration, login, superuser);
    }

    public String toString()
    {
        return "GenerateRoleRequestPayload{" +
               "roleNameOptions=" + roleNameOptions +
               ", sidecarGeneration=" + sidecarGeneration +
               ", passwordGeneration=" + passwordGeneration +
               ", login=" + login +
               ", superuser=" + superuser +
               '}';
    }
}
