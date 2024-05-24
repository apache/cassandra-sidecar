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

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.request.data.AbortRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.db.RestoreJobTest;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class AbortRestoreJobHandlerTest extends BaseRestoreJobTests
{
    private static final String RESTORE_JOB_ABORT_ENDPOINT = "/api/v1/keyspaces/%s/tables/%s/restore-jobs/%s/abort";

    @Test
    void testValidRequest(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        sendAbortRestoreJobRequestAndVerify("ks", "table", "8e5799a4-d277-11ed-8d85-6916bb9b8056",
                                            context, HttpResponseStatus.OK.code());
    }

    @Test
    void testInvalidJobId(VertxTestContext context) throws Throwable
    {
        sendAbortRestoreJobRequestAndVerify("ks", "table", "12951f25-d393-4158-9e90-ec0cbe05af21",
                                            context, HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void testInvalidKeyspace(VertxTestContext context) throws Throwable
    {
        sendAbortRestoreJobRequestAndVerify("sidecar_internal", "table", "8e5799a4-d277-11ed-8d85-6916bb9b8056",
                                            context, HttpResponseStatus.FORBIDDEN.code());
    }

    @Test
    void testJobAbsent(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(id -> null);
        sendAbortRestoreJobRequestAndVerify("ks", "table", "7cd82ff9-d276-11ed-93e5-7fce0df1306f",
                                            context, HttpResponseStatus.NOT_FOUND.code());
    }

    @Test
    void testAbortJobInFinalState(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(id -> RestoreJobTest.createTestingJob(id, RestoreJobStatus.SUCCEEDED));
        sendAbortRestoreJobRequestAndVerify("ks", "table", "8e5799a4-d277-11ed-8d85-6916bb9b8056",
                                            context, HttpResponseStatus.CONFLICT.code());
    }

    @Test
    void testAbortJobWithReason(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        sendAbortRestoreJobRequestAndVerify("ks", "table", "8e5799a4-d277-11ed-8d85-6916bb9b8056",
                                            context, HttpResponseStatus.OK.code(),
                                            new AbortRestoreJobRequestPayload("Analytics job has failed"));
    }

    private void sendAbortRestoreJobRequestAndVerify(String keyspace,
                                                     String table,
                                                     String jobId,
                                                     VertxTestContext context,
                                                     int expectedStatusCode) throws Throwable
    {
        sendAbortRestoreJobRequestAndVerify(keyspace, table, jobId, context, expectedStatusCode, null);
    }

    private void sendAbortRestoreJobRequestAndVerify(String keyspace,
                                                     String table,
                                                     String jobId,
                                                     VertxTestContext context,
                                                     int expectedStatusCode,
                                                     AbortRestoreJobRequestPayload requestPayload) throws Throwable
    {
        WebClient client = WebClient.create(vertx, new WebClientOptions());
        HttpRequest<Buffer> request = client.post(server.actualPort(),
                                                  "localhost",
                                                  String.format(RESTORE_JOB_ABORT_ENDPOINT, keyspace, table, jobId))
                                            .as(BodyCodec.buffer());
        Handler<AsyncResult<HttpResponse<Buffer>>> responseVerifier = resp -> {
            context.verify(() -> assertThat(resp.result().statusCode()).isEqualTo(expectedStatusCode))
                   .completeNow();
            client.close();
        };
        if (requestPayload != null)
        {
            request.sendJson(requestPayload, responseVerifier);
        }
        else
        {
            request.send(responseVerifier);
        }
        context.awaitCompletion(10, TimeUnit.SECONDS);
    }
}
