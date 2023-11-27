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

package org.apache.cassandra.sidecar.routes.restore;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Session;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class CreateRestoreJobHandlerTest extends BaseRestoreJobTests
{
    private static final String CREATE_RESTORE_JOB_ENDPOINT = "/api/v1/keyspaces/%s/tables/%s/restore-jobs";

    @Test
    void testValidRequest(VertxTestContext context) throws Throwable
    {
        String jobId = "8e5799a4-d277-11ed-8d85-6916bb9b8056";
        // create a new job, and there is no existing job
        mockCreateRestoreJob(x -> createTestNewJob(jobId));
        mockLookupRestoreJob(id -> null);
        JsonObject payload = getRequestPayload(jobId);
        sendCreateRestoreJobRequestAndVerify("ks", "table", payload, context, HttpResponseStatus.OK.code());
    }

    @Test
    void testInvalidJobId(VertxTestContext context) throws Throwable
    {
        JsonObject payload = getRequestPayload("12951f25-d393-4158-9e90-ec0cbe05af21");
        sendCreateRestoreJobRequestAndVerify("ks", "table", payload, context, HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void testInvalidKeyspace(VertxTestContext context) throws Throwable
    {
        JsonObject payload = getRequestPayload("8e5799a4-d277-11ed-8d85-6916bb9b8056");
        sendCreateRestoreJobRequestAndVerify("sidecar_internal", "table", payload,
                                             context, HttpResponseStatus.FORBIDDEN.code());
    }

    @Test
    void testConflictWhenJobExists(VertxTestContext context) throws Throwable
    {
        JsonObject payload = getRequestPayload("7cd82ff9-d276-11ed-93e5-7fce0df1306f");
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        sendCreateRestoreJobRequestAndVerify("ks", "table", payload,
                                             context, HttpResponseStatus.CONFLICT.code());
    }

    @Test
    void testWithCreateJobFailure(VertxTestContext context) throws Throwable
    {
        JsonObject payload = getRequestPayload("8e5799a4-d277-11ed-8d85-6916bb9b8056");
        mockCreateRestoreJob(x -> {
            throw new RuntimeException("Failed to create job");
        });
        sendCreateRestoreJobRequestAndVerify("ks", "table", payload, context,
                                             HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
    }

    @Test
    void testPartialPayload(VertxTestContext context) throws Throwable
    {
        // test create a new job with only secrets
        JsonObject payload = new JsonObject();
        payload.put("secrets", SECRETS);
        payload.put("expireAt", System.currentTimeMillis() + 10000L);
        mockCreateRestoreJob(x -> createTestNewJob("8e5799a4-d277-11ed-8d85-6916bb9b8056"));
        sendCreateRestoreJobRequestAndVerify("ks", "table", payload, context, HttpResponseStatus.OK.code());
    }

    @Test
    void testEmptySecrets(VertxTestContext context) throws Throwable
    {
        JsonObject payload = new JsonObject();
        payload.put("jobId", "8e5799a4-d277-11ed-8d85-6916bb9b8056");
        payload.put("secrets", Collections.emptyMap()); // empty map to compose an empty json block, {}
        sendCreateRestoreJobRequestAndVerify("ks", "table", payload, context, HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void testExceptionThrownDuringExecution(VertxTestContext context) throws Throwable
    {
        JsonObject payload = getRequestPayload("8e5799a4-d277-11ed-8d85-6916bb9b8056");
        CQLSessionProvider sessionProviderWithNonWorkingSession = mock(CQLSessionProvider.class);
        Session nonWorkingSession = mock(Session.class);
        when(nonWorkingSession.execute(anyString())).thenAnswer(invocation -> {
            throw new RuntimeException("unexpected exception");
        });
        when(sessionProviderWithNonWorkingSession.get()).thenReturn(nonWorkingSession);
        sendCreateRestoreJobRequestAndVerify("ks", "table", payload,
                                             context, HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
    }

    @Test
    void testNullPayload(VertxTestContext context) throws Throwable
    {
        sendCreateRestoreJobRequestAndVerify("ks", "table", null,
                                             context, HttpResponseStatus.BAD_REQUEST.code());

    }

    private RestoreJob createTestNewJob(String jobId)
    {
        return RestoreJobTest.createNewTestingJob(UUID.fromString(jobId));
    }

    private JsonObject getRequestPayload(String jobId)
    {
        JsonObject payload = new JsonObject();
        payload.put("jobId", jobId);
        payload.put("jobAgent", "agent");
        payload.put("secrets", SECRETS);
        payload.put("expireAt", System.currentTimeMillis() + 10000L);
        return payload;
    }

    private void sendCreateRestoreJobRequestAndVerify(String keyspace,
                                                      String table,
                                                      JsonObject payload,
                                                      VertxTestContext context,
                                                      int expectedStatusCode) throws Throwable
    {
        WebClient client = WebClient.create(vertx, new WebClientOptions());
        client.post(server.actualPort(), "localhost", String.format(CREATE_RESTORE_JOB_ENDPOINT, keyspace, table))
              .as(BodyCodec.buffer())
              .sendJsonObject(payload, resp -> {
                  context.verify(() -> {
                      assertThat(resp.result().statusCode()).isEqualTo(expectedStatusCode);
                      if (expectedStatusCode == HttpResponseStatus.OK.code())
                      {
                          JsonObject responseBody = resp.result().bodyAsJsonObject();
                          String jobIdKey = "jobId";
                          if (payload.getString(jobIdKey) == null)
                          {
                              assertThat(responseBody.containsKey(jobIdKey)).isTrue();
                          }
                          else
                          {
                              assertThat(responseBody.getString(jobIdKey)).isEqualTo(payload.getString(jobIdKey));
                          }
                          assertThat(responseBody.getString("status")).isEqualTo("CREATED");
                      }
                  })
                  .completeNow();
                  client.close();
              });
        context.awaitCompletion(10, TimeUnit.SECONDS);
    }
}
