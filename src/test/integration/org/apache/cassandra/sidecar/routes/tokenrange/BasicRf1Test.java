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

package org.apache.cassandra.sidecar.routes.tokenrange;

import java.util.Collections;

import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the token range replica mapping endpoint with the in-jvm dtest framework.
 *
 * Note: Some related test classes are broken down to have a single test case to parallelize test execution and
 * therefore limit the instance size required to run the tests from CircleCI as the in-jvm-dtests tests are memory bound
 */
@ExtendWith(VertxExtension.class)
class BasicRf1Test extends BaseTokenRangeIntegrationTest
{
    @CassandraIntegrationTest(nodesPerDc = 3, gossip = true)
    void retrieveMappingRf1(VertxTestContext context) throws Exception
    {
        createTestKeyspace();
        retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
            assertMappingResponseOK(mappingResponse, 1, Collections.singleton("datacenter1"));
            context.completeNow();
        });
    }

    @CassandraIntegrationTest()
    void retrieveMappingSingleNodeRf1(VertxTestContext context) throws Exception
    {
        createTestKeyspace();
        retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
            assertMappingResponseOK(mappingResponse, 1, Collections.singleton("datacenter1"));
            context.completeNow();
        });
    }

}
