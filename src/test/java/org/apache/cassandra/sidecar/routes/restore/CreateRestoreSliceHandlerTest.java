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

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.request.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.db.RestoreJobTest;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.restore.RestoreSliceTracker;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class CreateRestoreSliceHandlerTest extends BaseRestoreJobTests
{
    private static final String CREATE_RESTORE_SLICE_ENDPOINT = "/api/v1/keyspaces/%s/tables/%s/restore-jobs/%s/slices";
    private static final String TEST_JOB_ID = "8e5799a4-d277-11ed-8d85-6916bb9b8056";

    @Test
    void testValidCreatedRequest(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        mockSubmitRestoreSlice(x -> RestoreSliceTracker.Status.CREATED);
        sendCreateRestoreSliceRequestAndVerify("ks", TEST_JOB_ID, dummy(), context,
                                               HttpResponseStatus.CREATED);
    }

    @Test
    void testValidAcceptedRequest(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        mockSubmitRestoreSlice(x -> RestoreSliceTracker.Status.PENDING);
        CreateSliceRequestPayload slice = new CreateSliceRequestPayload("2", 3, "bucket", "key",
                                                                        "checksum", BigInteger.ONE,
                                                                        BigInteger.valueOf(2), 123L, 234L);
        sendCreateRestoreSliceRequestAndVerify("ks", TEST_JOB_ID, slice, context,
                                               HttpResponseStatus.ACCEPTED);
    }

    @Test
    void testValidOkRequest(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        mockSubmitRestoreSlice(x -> RestoreSliceTracker.Status.COMPLETED);
        CreateSliceRequestPayload slice = new CreateSliceRequestPayload("3", 2, "bucket", "key",
                                                                        "checksum", BigInteger.ONE,
                                                                        BigInteger.valueOf(2), 123L, 234L);
        sendCreateRestoreSliceRequestAndVerify("ks", TEST_JOB_ID, slice, context,
                                               HttpResponseStatus.OK);
    }

    @Test
    void testCreateSliceWhenJobHasCompleted(VertxTestContext context) throws Throwable
    {
        // the restore job is completed / SUCCEEDED
        mockLookupRestoreJob(id -> RestoreJobTest.createTestingJob(id, RestoreJobStatus.SUCCEEDED));
        CreateSliceRequestPayload slice = new CreateSliceRequestPayload("3", 2, "bucket", "key",
                                                                        "checksum", BigInteger.ONE,
                                                                        BigInteger.valueOf(2), 123L, 234L);
        sendCreateRestoreSliceRequestAndVerify("ks", TEST_JOB_ID, slice, context,
                                               HttpResponseStatus.CONFLICT);
    }

    @Test
    void testCreateSliceWhenSliceHasFailed(VertxTestContext context) throws Throwable
    {
        // the restore job is still active
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        // submit the slice fails, because the job has been failed internally
        mockSubmitRestoreSlice(x -> {
            throw new RestoreJobFatalException("job failed");
        });
        CreateSliceRequestPayload slice = new CreateSliceRequestPayload("3", 2, "bucket", "key",
                                                                        "checksum", BigInteger.ONE,
                                                                        BigInteger.valueOf(2), 123L, 234L);
        sendCreateRestoreSliceRequestAndVerify("ks", TEST_JOB_ID, slice, context,
                                               HttpResponseStatus.valueOf(550));
    }

    @Test
    void testInvalidJobId(VertxTestContext context) throws Throwable
    {
        sendCreateRestoreSliceRequestAndVerify("ks", "12951f25-d393-4158-9e90-ec0cbe05af21", dummy(), context,
                                               HttpResponseStatus.BAD_REQUEST);
    }

    @Test
    void testInvalidKeyspace(VertxTestContext context) throws Throwable
    {
        sendCreateRestoreSliceRequestAndVerify("sidecar_internal", TEST_JOB_ID, dummy(), context,
                                               HttpResponseStatus.FORBIDDEN);
    }

    @Test
    void testRestoreJobNotFound(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(x -> null);
        sendCreateRestoreSliceRequestAndVerify("ks", "7cd82ff9-d276-11ed-93e5-7fce0df1306f", dummy(), context,
                                               HttpResponseStatus.NOT_FOUND);
    }

    private CreateSliceRequestPayload dummy()
    {
        return new CreateSliceRequestPayload("1", 5, "bucket", "key", "checksum",
                                             BigInteger.ONE, BigInteger.valueOf(2), 123L, 234L);
    }

    private void sendCreateRestoreSliceRequestAndVerify(String keyspace,
                                                        String jobId,
                                                        CreateSliceRequestPayload createSliceRequest,
                                                        VertxTestContext context,
                                                        HttpResponseStatus expectedStatus) throws Throwable
    {
        WebClient client = WebClient.create(vertx, new WebClientOptions());
        client.post(server.actualPort(),
                    "localhost",
                    String.format(CREATE_RESTORE_SLICE_ENDPOINT, keyspace, "table", jobId))
              .sendJsonObject(JsonObject.mapFrom(createSliceRequest), resp -> {
                  context.verify(() -> {
                      assertThat(resp.result().statusCode()).isEqualTo(expectedStatus.code());
                  })
                  .completeNow();
                  client.close();
              });
        context.awaitCompletion(10, TimeUnit.SECONDS);
    }
}
