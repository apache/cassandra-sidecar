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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.data.SSTableImportOptions;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobTest;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class UpdateRestoreJobHandlerTest extends BaseRestoreJobTests
{
    private static final String RESTORE_JOB_UPDATE_ENDPOINT = "/api/v1/keyspaces/%s/tables/%s/restore-jobs/%s";

    @Test
    void testValidRequest(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        JsonObject payload = getRequestPayload();
        mockUpdateRestoreJob(x -> createTestNewJob("8e5799a4-d277-11ed-8d85-6916bb9b8056"));
        sendUpdateRestoreJobRequestAndVerify("ks", "table", "8e5799a4-d277-11ed-8d85-6916bb9b8056",
                                             payload, context, HttpResponseStatus.OK.code());
    }

    @Test
    void testInvalidJobId(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        JsonObject payload = getRequestPayload();
        sendUpdateRestoreJobRequestAndVerify("ks", "table", "12951f25-d393-4158-9e90-ec0cbe05af21",
                                             payload, context, HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void testInvalidKeyspace(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        JsonObject payload = getRequestPayload();
        sendUpdateRestoreJobRequestAndVerify("sidecar_internal", "table", "8e5799a4-d277-11ed-8d85-6916bb9b8056",
                                             payload, context, HttpResponseStatus.FORBIDDEN.code());
    }

    @Test
    void testJobAbsent(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(id -> null);
        JsonObject payload = getRequestPayload();
        sendUpdateRestoreJobRequestAndVerify("ks", "table", "7cd82ff9-d276-11ed-93e5-7fce0df1306f",
                                             payload, context, HttpResponseStatus.NOT_FOUND.code());
    }

    @Test
    void testUnexpectedJobStatusSetInRequest(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        JsonObject payload = new JsonObject();
        payload.put("jobAgent", "agent");
        Map<String, String> secrets = new HashMap<>();
        secrets.put("refresh_token", "token");
        secrets.put("auth_token", "token");
        payload.put("secrets", secrets);
        payload.put("status", "RUNNING");
        sendUpdateRestoreJobRequestAndVerify("ks", "table", "8e5799a4-d277-11ed-8d85-6916bb9b8056",
                                             payload, context, HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void testEmptyRequestBody(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        JsonObject payload = new JsonObject();
        sendUpdateRestoreJobRequestAndVerify("ks", "table", "8e5799a4-d277-11ed-8d85-6916bb9b8056",
                                             payload, context, HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void testAllFieldsEmptyWithEmptySecrets(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        JsonObject payload = new JsonObject();
        Map<String, String> secrets = new HashMap<>();
        payload.put("secrets", secrets);
        sendUpdateRestoreJobRequestAndVerify("ks", "table", "8e5799a4-d277-11ed-8d85-6916bb9b8056",
                                             payload, context, HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void testUpdateWhenJobInFinalState(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(id -> RestoreJobTest.createTestingJob(id, RestoreJobStatus.SUCCEEDED));
        JsonObject payload = getRequestPayload();
        sendUpdateRestoreJobRequestAndVerify("ks", "table", "8e5799a4-d277-11ed-8d85-6916bb9b8056",
                                             payload, context, HttpResponseStatus.CONFLICT.code());
    }

    @Test
    void testUpdateOnlyExpireAt(VertxTestContext context) throws Throwable
    {
        long futureTime = System.currentTimeMillis() + 1234L;
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        mockUpdateRestoreJob(payload -> {
            assertThat(payload.expireAtInMillis()).isEqualTo(futureTime);
            return createTestNewJob("8e5799a4-d277-11ed-8d85-6916bb9b8056");
        });
        JsonObject payload = new JsonObject();
        payload.put("expireAt", futureTime);
        sendUpdateRestoreJobRequestAndVerify("ks", "table", "8e5799a4-d277-11ed-8d85-6916bb9b8056",
                                             payload, context, HttpResponseStatus.OK.code());
    }

    private RestoreJob createTestNewJob(String jobId)
    {
        return RestoreJob.create(null, UUID.fromString(jobId), "ks", "table",
                                 "agent", RestoreJobStatus.SUCCEEDED, SECRETS, SSTableImportOptions.defaults());
    }

    private JsonObject getRequestPayload()
    {
        JsonObject payload = new JsonObject();
        payload.put("jobAgent", "agent");
        payload.put("secrets", SECRETS);
        payload.put("status", "SUCCEEDED");
        return payload;
    }

    private void sendUpdateRestoreJobRequestAndVerify(String keyspace,
                                                      String table,
                                                      String jobId,
                                                      JsonObject payload,
                                                      VertxTestContext context,
                                                      int expectedStatusCode) throws Throwable
    {
        WebClient client = WebClient.create(vertx, new WebClientOptions());
        client.patch(server.actualPort(),
                     "localhost",
                     String.format(RESTORE_JOB_UPDATE_ENDPOINT, keyspace, table, jobId))
              .as(BodyCodec.buffer())
              .sendJsonObject(payload, resp -> {
                  context.verify(() -> {
                      assertThat(resp.result().statusCode()).isEqualTo(expectedStatusCode);
                  })
                  .completeNow();
                  client.close();
              });
        context.awaitCompletion(10, TimeUnit.SECONDS);
    }
}
