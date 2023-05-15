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

import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.IntegrationTestBase;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.testing.CassandraIntegrationTest;
import org.apache.cassandra.sidecar.common.testing.CassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the token range replica mapping endpoint with cassandra container.
 */
@ExtendWith(VertxExtension.class)
class TokenRangeReplicaMapHandlerIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest
    void retrieveMappingWithUnknownKeyspace(VertxTestContext context) throws Exception
    {
        retrieveMappingWithKeyspace(context, "unknown_ks", response -> {
            int errorCode = HttpResponseStatus.NOT_FOUND.code();
            assertThat(response.statusCode()).isEqualTo(errorCode);
            JsonObject error = response.bodyAsJsonObject();
            assertThat(error.getInteger("code")).isEqualTo(errorCode);
            assertThat(error.getString("message")).contains("Unknown keyspace unknown_ks");

            context.completeNow();
        });
    }

    @CassandraIntegrationTest
    void retrieveMappingWithKeyspace(VertxTestContext context,
                                     CassandraTestContext cassandraTestContext) throws Exception
    {
        createTestKeyspace(cassandraTestContext);
        retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
            assertMappingResponseOK(response);
            context.completeNow();
        });
    }

    void retrieveMappingWithKeyspace(VertxTestContext context, String keyspace,
                                     Handler<HttpResponse<Buffer>> verifier) throws Exception
    {
        String testRoute = "/api/v1/keyspaces/" + keyspace + "/token-range-replicas";
        testWithClient(context, client -> client.get(config.getPort(), "localhost", testRoute)
                                                .send(context.succeeding(verifier)));
    }

    void assertMappingResponseOK(HttpResponse<Buffer> response)
    {
        TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
        assertThat(mappingResponse).isNotNull();
        assertThat(mappingResponse.naturalReplicas()).isNotNull();
        assertThat(mappingResponse.writeReplicas()).isNotNull();
        TokenRangeReplicasResponse.ReplicaInfo naturalReplica = mappingResponse.naturalReplicas().get(0);
        assertThat(naturalReplica.replicasByDatacenter()).isNotNull().hasSize(1);
        assertThat(naturalReplica.replicasByDatacenter().keySet()).isNotEmpty().contains("datacenter1");
        assertThat(naturalReplica.replicasByDatacenter().values()).isNotEmpty();
        TokenRangeReplicasResponse.ReplicaInfo writeReplica = mappingResponse.writeReplicas().get(0);
        assertThat(writeReplica.replicasByDatacenter()).isNotNull().hasSize(1);
        assertThat(writeReplica.replicasByDatacenter().keySet()).isNotEmpty().contains("datacenter1");
        assertThat(writeReplica.replicasByDatacenter().values()).isNotEmpty();
    }
}
