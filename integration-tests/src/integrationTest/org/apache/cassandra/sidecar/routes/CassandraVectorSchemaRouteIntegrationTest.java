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

package org.apache.cassandra.sidecar.routes;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.vertx.core.http.HttpResponseExpectation;
import org.apache.cassandra.sidecar.common.response.SchemaResponse;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;

import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

class CassandraVectorSchemaRouteIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{
    protected static final int MIN_VERSION_WITH_VECTOR = 5;

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace("test_keyspace", Map.of("replication_factor", 1));
        createTestTable(new QualifiedName("test_keyspace", "int_table"),
                        "CREATE TABLE IF NOT EXISTS %s (a int, b int, PRIMARY KEY (a))");
        createTestTable(new QualifiedName("test_keyspace", "vector_table"),
                        "CREATE TABLE IF NOT EXISTS %s (a int, b vector<float, 3>, PRIMARY KEY (a))");
    }

    @Override
    protected void beforeClusterProvisioning()
    {
        assumeThat(SimpleCassandraVersion.create(testVersion.version()).major)
        .as("Vector type is supported since Cassandra 5.0")
        .isGreaterThanOrEqualTo(MIN_VERSION_WITH_VECTOR);
    }

    @Test
    void testSchemaHandlerWithVectorTable()
    {
        String testRoute = "/api/v1/schema/keyspaces/test_keyspace";
        SchemaResponse response = getBlocking(trustedClient()
                                              .get(serverWrapper.serverPort, "localhost", testRoute)
                                              .send()
                                              .expecting(HttpResponseExpectation.SC_OK))
                                  .bodyAsJson(SchemaResponse.class);
        assertThat(response).isNotNull();
        assertThat(response.keyspace()).isEqualTo("test_keyspace");
        assertThat(response.schema()).contains("vector_table");
    }
}
