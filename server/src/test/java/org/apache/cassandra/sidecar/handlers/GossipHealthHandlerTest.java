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

package org.apache.cassandra.sidecar.handlers;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.server.StorageOperations;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * Failure tests for {@link GossipHealthHandler}
 * Success cases are already covered by corresponding in-jvm test
 */
@ExtendWith(VertxExtension.class)
public class GossipHealthHandlerTest extends CommonTest
{
    StorageOperations storageOperations = mock(StorageOperations.class);

    @Test
    void testFailure(VertxTestContext context)
    {
        doThrow(new RuntimeException()).when(storageOperations).isGossipRunning();

        WebClient client = WebClient.create(vertx);
        client.get(server.actualPort(), "127.0.0.1", "/api/v1/cassandra/gossip/__health")
              .expect(ResponsePredicate.SC_INTERNAL_SERVER_ERROR)
              .send(context.succeeding(response -> {
                  assertThat(response.statusCode()).isEqualTo(INTERNAL_SERVER_ERROR.code());
                  context.completeNow();
              }));
    }
}
