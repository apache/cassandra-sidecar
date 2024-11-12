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

import org.junit.jupiter.api.Test;

import io.vertx.ext.auth.authorization.PermissionBasedAuthorization;
import io.vertx.ext.auth.authorization.WildcardPermissionBasedAuthorization;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

import static org.apache.cassandra.sidecar.utils.AuthUtils.actionFromName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link Action}
 */
class ActionTest
{
    @Test
    void testValidActions()
    {
        assertThat(actionFromName("CREATE_SNAPSHOT")).isInstanceOf(StandardAction.class);
        assertThat(actionFromName("OPERATE")).isInstanceOf(StandardAction.class);
        assertThat(actionFromName("CREATESNAPSHOT")).isInstanceOf(StandardAction.class);
        assertThat(actionFromName("CREATE:SNAPSHOT")).isInstanceOf(WildcardAction.class);
        assertThat(actionFromName("*:SNAPSHOT")).isInstanceOf(WildcardAction.class);
        assertThat(actionFromName("STREAM:*")).isInstanceOf(WildcardAction.class);
        assertThat(actionFromName("*")).isInstanceOf(WildcardAction.class);
        assertThat(actionFromName("*:*")).isInstanceOf(WildcardAction.class);
        assertThat(actionFromName("CREATE:SNAPSHOT:*")).isInstanceOf(WildcardAction.class);
    }

    @Test
    void testInvalidActions()
    {
        assertThatThrownBy(() -> actionFromName("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> actionFromName(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testToAuthorizationWithResource()
    {
        String expectedResource = VariableAwareResource.DATA_WITH_KEYSPACE_TABLE.resource();
        PermissionBasedAuthorization authorization
        = (PermissionBasedAuthorization) actionFromName("CREATESNAPSHOT").toAuthorization(expectedResource);
        assertThat(authorization.getResource()).isEqualTo(expectedResource);
        WildcardPermissionBasedAuthorization wildcardAuthorization
        = (WildcardPermissionBasedAuthorization) actionFromName("CREATE:SNAPSHOT").toAuthorization(expectedResource);
        assertThat(wildcardAuthorization.getResource()).isEqualTo(expectedResource);
    }

    @Test
    void testToAuthorizationWithEmptyResource()
    {
        PermissionBasedAuthorization authorization
        = (PermissionBasedAuthorization) actionFromName("CREATESNAPSHOT").toAuthorization("");
        assertThat(authorization.getResource()).isNull();
        WildcardPermissionBasedAuthorization wildcardAuthorization
        = (WildcardPermissionBasedAuthorization) actionFromName("CREATE:SNAPSHOT").toAuthorization("");
        assertThat(wildcardAuthorization.getResource()).isNull();
    }

    @Test
    void testInvalidWildcardActions()
    {
        assertThatThrownBy(() -> new WildcardAction("CREATE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Wildcard actions must either have wildcard token * or must have wildcard parts");

        assertThatThrownBy(() -> new WildcardAction(":"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Wildcard action parts can not be empty");

        assertThatThrownBy(() -> new WildcardAction("::"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Wildcard action parts can not be empty");
    }
}
