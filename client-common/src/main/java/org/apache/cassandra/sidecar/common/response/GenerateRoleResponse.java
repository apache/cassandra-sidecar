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

package org.apache.cassandra.sidecar.common.response;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

public class GenerateRoleResponse
{
    public static final String GENERATED_ROLE_KEY = "generated_role";
    public static final String GENERATED_PASSWORD_KEY = "generated_password";

    @NotNull
    private final String role;

    private final String password;

    /**
     * Constructs a {@link GenerateRoleResponse} object with the given {@code role} and {@code password}.
     *
     * @param role     generated role
     * @param password generated password, null if not generated
     */
    public GenerateRoleResponse(@JsonProperty(GENERATED_ROLE_KEY) String role,
                                @JsonProperty(GENERATED_PASSWORD_KEY) String password)
    {
        this.role = Objects.requireNonNull(role, "role must not be null");
        this.password = password;
    }

    @JsonProperty(GENERATED_ROLE_KEY)
    public String role()
    {
        return role;
    }

    @JsonProperty(GENERATED_PASSWORD_KEY)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String password()
    {
        return password;
    }

    /**
     * @return whether the generated password is present
     */
    @JsonIgnore
    public boolean isGeneratedPassword()
    {
        return password != null;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GenerateRoleResponse response = (GenerateRoleResponse) o;
        return Objects.equals(role, response.role) && Objects.equals(password, response.password);
    }

    public int hashCode()
    {
        return Objects.hash(role, password);
    }

    public String toString()
    {
        return "GenerateRoleResponse{" +
               "role='" + role + '\'' +
               ", password='<REDACTED>" +
               '}';
    }
}
