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
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.data.ConsistencyLevel;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.request.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.db.RestoreJobTest;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.restore.RestoreJobProgressTracker;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class CreateRestoreSliceHandlerTest extends BaseRestoreJobTests
{
    private static final String CREATE_RESTORE_SLICE_ENDPOINT = "/api/v1/keyspaces/%s/tables/%s/restore-jobs/%s/slices";
    private static final String TEST_JOB_ID = "8e5799a4-d277-11ed-8d85-6916bb9b8056";

    @Test
    void testValidCreatedRequest(VertxTestContext context)
    {
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        mockSubmitRestoreSlice(x -> RestoreJobProgressTracker.Status.CREATED);
        sendCreateRestoreSliceRequestAndVerify("ks", TEST_JOB_ID, dummy(), context,
                                               HttpResponseStatus.CREATED);
    }

    @Test
    void testValidAcceptedRequest(VertxTestContext context)
    {
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        mockSubmitRestoreSlice(x -> RestoreJobProgressTracker.Status.PENDING);
        CreateSliceRequestPayload slice = new CreateSliceRequestPayload("2", 3, "bucket", "key",
                                                                        "checksum", BigInteger.ONE,
                                                                        BigInteger.valueOf(2), 123L, 234L);
        sendCreateRestoreSliceRequestAndVerify("ks", TEST_JOB_ID, slice, context,
                                               HttpResponseStatus.ACCEPTED);
    }

    @Test
    void testValidOkRequest(VertxTestContext context)
    {
        mockLookupRestoreJob(RestoreJobTest::createNewTestingJob);
        mockSubmitRestoreSlice(x -> RestoreJobProgressTracker.Status.COMPLETED);
        CreateSliceRequestPayload slice = new CreateSliceRequestPayload("3", 2, "bucket", "key",
                                                                        "checksum", BigInteger.ONE,
                                                                        BigInteger.valueOf(2), 123L, 234L);
        sendCreateRestoreSliceRequestAndVerify("ks", TEST_JOB_ID, slice, context,
                                               HttpResponseStatus.OK);
    }

    @Test
    void testCreateSliceWhenJobHasCompleted(VertxTestContext context)
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
    void testCreateSliceWhenSliceHasFailed(VertxTestContext context)
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
    void testInvalidJobId(VertxTestContext context)
    {
        String invalidJobId = "12951f25-d393-4158-9e90-ec0cbe05af21";
        sendCreateRestoreSliceRequestAndVerify("ks", invalidJobId, dummy(), context,
                                               HttpResponseStatus.BAD_REQUEST);
    }

    @Test
    void testInvalidKeyspace(VertxTestContext context)
    {
        sendCreateRestoreSliceRequestAndVerify("sidecar_internal", TEST_JOB_ID, dummy(), context,
                                               HttpResponseStatus.FORBIDDEN);
    }

    @Test
    void testRestoreJobNotFound(VertxTestContext context)
    {
        mockLookupRestoreJob(x -> null);
        sendCreateRestoreSliceRequestAndVerify("ks", TEST_JOB_ID, dummy(), context,
                                               HttpResponseStatus.NOT_FOUND);
    }

    @Test
    void testCreateSliceFailsToPersist(VertxTestContext context)
    {
        // the restore job is still active; it has CL defined, so the slice is to be persisted
        mockLookupRestoreJob(jobId -> RestoreJobTest.createTestingJob(jobId, "ks", RestoreJobStatus.CREATED, ConsistencyLevel.LOCAL_QUORUM, "dc1"));
        mockCreateRestoreSlice(slice -> {
            throw new RuntimeException("Persisting to Cassandra failed");
        });
        sendCreateRestoreSliceRequestAndVerify("ks", TEST_JOB_ID, dummy(), context,
                                               HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    void testCreateSliceAndPersist(VertxTestContext context)
    {
        mockLookupRestoreJob(jobId -> RestoreJobTest.createTestingJob(jobId, RestoreJobStatus.CREATED, ConsistencyLevel.QUORUM));
        int repeatCount = 5;
        Checkpoint persistedCount = context.checkpoint(repeatCount);
        Checkpoint responseReceived = context.checkpoint(repeatCount);
        mockCreateRestoreSlice(slice -> {
            persistedCount.flag();
            return slice;
        });
        CreateSliceRequestPayload createSliceRequestPayload = dummy();
        for (int i = 0; i < repeatCount; i++)
        {
            // unlike the spark-managed jobs, the creating the slice again with the same payload should still return CREATED, instead of ACCEPTED
            sendCreateRestoreSliceRequestAndVerify("ks", TEST_JOB_ID, createSliceRequestPayload, context, HttpResponseStatus.CREATED, resp -> {
                responseReceived.flag();
            });
        }
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
                                                        HttpResponseStatus expectedStatus)
    {
        sendCreateRestoreSliceRequestAndVerify(keyspace, jobId, createSliceRequest, context, expectedStatus, resp -> {
            // do nothing
        });
    }

    private void sendCreateRestoreSliceRequestAndVerify(String keyspace,
                                                        String jobId,
                                                        CreateSliceRequestPayload createSliceRequest,
                                                        VertxTestContext context,
                                                        HttpResponseStatus expectedStatus,
                                                        Consumer<HttpResponse<?>> additionalResponseVerifier)
    {
        Checkpoint completion = context.checkpoint();
        postAndVerify(String.format(CREATE_RESTORE_SLICE_ENDPOINT, keyspace, "table", jobId),
                      JsonObject.mapFrom(createSliceRequest),
                      asyncResult -> {
                          context.verify(() -> {
                              HttpResponse<?> response = asyncResult.result();
                              assertThat(response).isNotNull();
                              assertThat(response.statusCode()).isEqualTo(expectedStatus.code());
                              additionalResponseVerifier.accept(response);
                              completion.flag();
                          });
                      });

    }
}
