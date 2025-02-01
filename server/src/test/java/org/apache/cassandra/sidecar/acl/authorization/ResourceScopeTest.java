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

import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.CLUSTER;
import static org.apache.cassandra.sidecar.acl.authorization.ResourceScopes.OPERATION;
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
        assertThat(CLUSTER.variableAwareResource()).isEqualTo("cluster");
        assertThat(CLUSTER.resolveWithResource("any")).isEqualTo("cluster");
        assertThat(CLUSTER.expandedResources().size()).isOne();
        assertThat(CLUSTER.expandedResources().contains("cluster")).isTrue();
    }

    @Test
    void testOperationScope()
    {
        assertThat(OPERATION.variableAwareResource()).isEqualTo("operation");
        assertThat(OPERATION.resolveWithResource("any")).isEqualTo("operation");
        assertThat(OPERATION.expandedResources().size()).isOne();
        assertThat(OPERATION.expandedResources().contains("operation")).isTrue();
    }

    @Test
    void testDataScope()
    {
        DataResourceScope dataScope = DataResourceScope.createWithDataScope();
        assertThat(dataScope.variableAwareResource()).isEqualTo("data");
        assertThat(dataScope.resolveWithResource("any")).isEqualTo("data");
        assertThat(dataScope.expandedResources().size()).isOne();
        assertThat(dataScope.expandedResources().contains("data")).isTrue();
    }

    @Test
    void testKeyspaceScope()
    {
        DataResourceScope keyspaceScope = DataResourceScope.createWithKeyspaceScope();
        assertThat(keyspaceScope.variableAwareResource()).isEqualTo("data/{keyspace}");
        // random resource is passed, resolved to variableAwareResource
        assertThat(keyspaceScope.resolveWithResource("any")).isEqualTo("data/{keyspace}");
        assertThat(keyspaceScope.resolveWithResource("data")).isEqualTo("data");
        assertThat(keyspaceScope.resolveWithResource("data/university/student")).isEqualTo("data/university");
        assertThat(keyspaceScope.expandedResources().size()).isEqualTo(2);
        assertThat(keyspaceScope.expandedResources().contains("data")).isTrue();
        assertThat(keyspaceScope.expandedResources().contains("data/{keyspace}")).isTrue();
    }

    @Test
    void testTableScope()
    {
        DataResourceScope tableScope = DataResourceScope.createWithTableScope();
        assertThat(tableScope.variableAwareResource()).isEqualTo("data/{keyspace}/{table}");
        assertThat(tableScope.resolveWithResource("any")).isEqualTo("data/{keyspace}/{table}");
        assertThat(tableScope.resolveWithResource("data")).isEqualTo("data");
        assertThat(tableScope.resolveWithResource("data/university")).isEqualTo("data/university");
        assertThat(tableScope.resolveWithResource("data/university/student")).isEqualTo("data/university/student");
        assertThat(tableScope.expandedResources().size()).isEqualTo(4);
        assertThat(tableScope.expandedResources().contains("data")).isTrue();
        assertThat(tableScope.expandedResources().contains("data/{keyspace}")).isTrue();
        assertThat(tableScope.expandedResources().contains("data/{keyspace}/{TABLE_WILDCARD}")).isTrue();
        assertThat(tableScope.expandedResources().contains("data/{keyspace}/{table}")).isTrue();
    }

    @Test
    void testResolvingResourceScopeWithEmptyValue()
    {
        DataResourceScope dataScope = DataResourceScope.createWithDataScope();
        assertThatThrownBy(() -> dataScope.resolveWithResource(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> dataScope.resolveWithResource("")).isInstanceOf(IllegalArgumentException.class);
    }
}
