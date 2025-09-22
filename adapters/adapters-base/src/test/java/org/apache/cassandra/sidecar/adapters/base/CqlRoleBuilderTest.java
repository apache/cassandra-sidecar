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

package org.apache.cassandra.sidecar.adapters.base;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.apache.cassandra.sidecar.adapters.base.CassandraRolesOperations.CqlRoleBuilder.createGeneratedRole;
import static org.apache.cassandra.sidecar.adapters.base.CassandraRolesOperations.CqlRoleBuilder.createRole;
import static org.apache.cassandra.sidecar.adapters.base.CassandraRolesOperations.UUIDGenerator.NAME_PREFIX_KEY;
import static org.apache.cassandra.sidecar.adapters.base.CassandraRolesOperations.UUIDGenerator.NAME_SIZE;
import static org.apache.cassandra.sidecar.adapters.base.CassandraRolesOperations.UUIDGenerator.NAME_SUFFIX_KEY;
import static org.assertj.core.api.Assertions.assertThat;

public class CqlRoleBuilderTest
{
    @Test
    public void testBuilder()
    {
        assertThat(createGeneratedRole().build())
        .isEqualTo("CREATE GENERATED ROLE;");

        assertThat(createGeneratedRole().login(true).build())
        .isEqualTo("CREATE GENERATED ROLE WITH LOGIN = true;");

        assertThat(createGeneratedRole().superuser(true).build())
        .isEqualTo("CREATE GENERATED ROLE WITH SUPERUSER = true;");

        assertThat(createGeneratedRole().login(true).superuser(true).build())
        .isEqualTo("CREATE GENERATED ROLE WITH LOGIN = true AND SUPERUSER = true;");
        assertThat(createGeneratedRole().generatedPassword(true).build())
        .isEqualTo("CREATE GENERATED ROLE WITH GENERATED PASSWORD;");

        assertThat(createGeneratedRole().password("123").build())
        .isEqualTo("CREATE GENERATED ROLE WITH PASSWORD = '123';");

        Map<String, String> optionsMap = new LinkedHashMap<>();
        optionsMap.put(NAME_PREFIX_KEY, "prefix_");
        optionsMap.put(NAME_SUFFIX_KEY, "_suffix");
        optionsMap.put(NAME_SIZE, "10");

        assertThat(createGeneratedRole().password("123").option(optionsMap).build())
        .isEqualTo("CREATE GENERATED ROLE WITH PASSWORD = '123' AND OPTIONS = {'"
                   + NAME_PREFIX_KEY + "':'prefix_','"
                   + NAME_SUFFIX_KEY + "':'_suffix','"
                   + NAME_SIZE + "':'10'};");

        assertThat(createRole("john").password("123").build())
        .isEqualTo("CREATE ROLE john WITH PASSWORD = '123';");

        assertThat(createRole("john").password("123").login(true).superuser(true).build())
        .isEqualTo("CREATE ROLE john WITH PASSWORD = '123' AND LOGIN = true AND SUPERUSER = true;");

        assertThat(createRole("john").generatedPassword(true).login(true).superuser(true).build())
        .isEqualTo("CREATE ROLE john WITH GENERATED PASSWORD AND LOGIN = true AND SUPERUSER = true;");

        // specific password mentioned later will reset generated password flag
        assertThat(createRole("john").generatedPassword(true).password("123").login(true).superuser(true).build())
        .isEqualTo("CREATE ROLE john WITH PASSWORD = '123' AND LOGIN = true AND SUPERUSER = true;");

        assertThat(createRole("john").password("123").generatedPassword(true).login(true).superuser(true).build())
        .isEqualTo("CREATE ROLE john WITH GENERATED PASSWORD AND LOGIN = true AND SUPERUSER = true;");
    }
}