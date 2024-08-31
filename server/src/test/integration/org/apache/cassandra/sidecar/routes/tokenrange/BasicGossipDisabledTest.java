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

import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the token range mapping endpoint fails with service unavailable when gossip is disabled
 */
@ExtendWith(VertxExtension.class)
public class BasicGossipDisabledTest extends BaseTokenRangeIntegrationTest
{
    @CassandraIntegrationTest
    void tokenRangeEndpointFailsWhenGossipIsDisabled(CassandraTestContext context, VertxTestContext testContext)
    throws Exception
    {
        int disableGossip = context.cluster().getFirstRunningInstance().nodetool("disablegossip");
        assertThat(disableGossip).isEqualTo(0);
        retrieveMappingWithKeyspace(testContext, TEST_KEYSPACE, response -> {
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
            JsonObject payload = response.bodyAsJsonObject();
            assertThat(payload.getString("status")).isEqualTo("Service Unavailable");
            assertThat(payload.getInteger("code")).isEqualTo(503);
            assertThat(payload.getString("message"))
            .isEqualTo("Gossip is required for the operation but it is disabled");
            testContext.completeNow();
        });
    }
}
