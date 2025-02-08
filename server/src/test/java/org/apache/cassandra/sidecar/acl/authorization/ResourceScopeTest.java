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

import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.CLUSTER_SCOPE;
import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.DATA_SCOPE;
import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.KEYSPACE_SCOPE;
import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.OPERATION_SCOPE;
import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.TABLE_SCOPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link ResourceScopes}
 */
class ResourceScopeTest
{
    @Test
    void testClusterScope()
    {
        assertThat(CLUSTER_SCOPE.variableAwareResource()).isEqualTo("cluster");
        assertThat(CLUSTER_SCOPE.resolveWithResource("any")).isEqualTo("cluster");
        assertThat(CLUSTER_SCOPE.expandedResources().size()).isOne();
        assertThat(CLUSTER_SCOPE.expandedResources()).contains("cluster");
    }

    @Test
    void testOperationScope()
    {
        assertThat(OPERATION_SCOPE.variableAwareResource()).isEqualTo("operation");
        assertThat(OPERATION_SCOPE.resolveWithResource("any")).isEqualTo("operation");
        assertThat(OPERATION_SCOPE.expandedResources().size()).isOne();
        assertThat(OPERATION_SCOPE.expandedResources()).contains("operation");
    }

    @Test
    void testDataScope()
    {
        assertThat(DATA_SCOPE.variableAwareResource()).isEqualTo("data");
        assertThat(DATA_SCOPE.expandedResources().size()).isOne();
        assertThat(DATA_SCOPE.expandedResources()).contains("data");
    }

    @Test
    void testKeyspaceScope()
    {
        assertThat(KEYSPACE_SCOPE.variableAwareResource()).isEqualTo("data/{keyspace}");
        // random resource is passed, resolved to variableAwareResource
        assertThat(KEYSPACE_SCOPE.resolveWithResource("data")).isEqualTo("data");
        assertThat(KEYSPACE_SCOPE.resolveWithResource("data/university/student")).isEqualTo("data/university");
        assertThat(KEYSPACE_SCOPE.expandedResources()).hasSize(2)
                                                      .contains("data", "data/{keyspace}");
    }

    @Test
    void testTableScope()
    {
        assertThat(TABLE_SCOPE.variableAwareResource()).isEqualTo("data/{keyspace}/{table}");
        assertThat(TABLE_SCOPE.resolveWithResource("data")).isEqualTo("data");
        assertThat(TABLE_SCOPE.resolveWithResource("data/university")).isEqualTo("data/university");
        assertThat(TABLE_SCOPE.resolveWithResource("data/university/student")).isEqualTo("data/university/student");
        assertThat(TABLE_SCOPE.expandedResources()).hasSize(4)
                                                   .contains("data",
                                                             "data/{keyspace}",
                                                             "data/{keyspace}/{TABLE_WILDCARD}",
                                                             "data/{keyspace}/{table}");
    }

    @Test
    void testResolvingResourceScopeWithEmptyValue()
    {
        assertThatThrownBy(() -> DATA_SCOPE.resolveWithResource(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DATA_SCOPE.resolveWithResource("")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInvalidDataResourceScopes()
    {
        assertThatThrownBy(() -> DATA_SCOPE.resolveWithResource("data/"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("data/ is not a valid data resource, expected format is data/<keyspace>/<table>");

        assertThatThrownBy(() -> DATA_SCOPE.resolveWithResource("dataOMG/ks/tbl"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("dataOMG/ks/tbl is not a valid data resource, expected format is data/<keyspace>/<table>");

        assertThatThrownBy(() -> DATA_SCOPE.resolveWithResource("data/ /tbl"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Keyspace or table can not be empty in data resource");

        assertThatThrownBy(() -> KEYSPACE_SCOPE.resolveWithResource("data//"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("data// is not a valid data resource, expected format is data/<keyspace>/<table>");

        assertThatThrownBy(() -> TABLE_SCOPE.resolveWithResource("data//tbl"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Keyspace or table can not be empty in data resource");

        assertThatThrownBy(() -> DATA_SCOPE.resolveWithResource("data/ks/tbl/extra"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("data/ks/tbl/extra is not a valid data resource, expected format is data/<keyspace>/<table>");

        assertThatThrownBy(() -> DATA_SCOPE.resolveWithResource("/"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("/ is not a valid data resource, expected format is data/<keyspace>/<table>");
    }
}
