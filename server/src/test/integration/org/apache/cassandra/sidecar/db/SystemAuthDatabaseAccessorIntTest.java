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

package org.apache.cassandra.sidecar.db;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.AuthMode;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

class SystemAuthDatabaseAccessorIntTest extends IntegrationTestBase
{
    @CassandraIntegrationTest(authMode = AuthMode.PASSWORD)
    void testCrudOperations()
    {
        waitForSchemaReady(20, TimeUnit.SECONDS);

        createRole("super_user_role", true);
        createRole("non_super_user_role", false);
        grantRole("non_super_user_role", "super_user_role");

        SystemAuthDatabaseAccessor accessor = injector.getInstance(SystemAuthDatabaseAccessor.class);
        Map<String, Boolean> actualSuperUsers = accessor.findAllRolesToSuperuserStatus();
        assertThat(actualSuperUsers.size()).isEqualTo(3);
        assertThat(actualSuperUsers.get("super_user_role")).isTrue();
        assertThat(actualSuperUsers.get("non_super_user_role")).isTrue();
    }
}
