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

package org.apache.cassandra.sidecar.handlers.livemigration;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.LiveMigrationFilesVerificationRequest;
import org.apache.cassandra.sidecar.common.response.LiveMigrationFilesVerificationResponse;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.livemigration.FakeFilesVerificationTask;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationFilesVerificationTask.State;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationFilesVerificationTaskFactory;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;
import org.mockito.stubbing.Answer;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_STATUS_ROUTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class LiveMigrationFilesVerificationTaskHandlerTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationFilesVerificationTaskHandlerTest.class);

    // Destination host in the live migration map corresponding to the FIRST_SOURCE_HOST
    private static final String FIRST_DESTINATION_HOST = "127.0.0.1";

    // Source host in the live migration map corresponding to the FIRST_DESTINATION_HOST
    private static final String FIRST_SOURCE_HOST = "127.0.0.2";

    // Destination host in the live migration map corresponding to the SECOND_SOURCE_HOST
    private static final String SECOND_DESTINATION_HOST = "127.0.0.4";

    // Source host in the live migration map corresponding to the SECOND_DESTINATION_HOST
    private static final String SECOND_SOURCE_HOST = "127.0.0.3";

    private final Vertx vertx = Vertx.vertx();
    Server server;
    private Injector injector;

    @BeforeEach
    void setup(@TempDir Path tempDir) throws InterruptedException
    {
        FilesVerificationHandlerTestModule handlerTestModule = new FilesVerificationHandlerTestModule();
        InstanceFetcherTestModule instanceFetcherTestModule = new InstanceFetcherTestModule(tempDir);

        injector = Guice.createInjector(Modules.override(SidecarModules.all())
                                               .with(Modules.override(new TestModule())
                                                            .with(handlerTestModule, instanceFetcherTestModule)));
        server = injector.getInstance(Server.class);
        VertxTestContext context = new VertxTestContext();

        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(15, TimeUnit.SECONDS);
    }

    @AfterEach
    void after() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    @Test
    void testTaskSubmission(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        LiveMigrationFilesVerificationTaskFactory taskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);

        when(taskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(getFakeVerificationTaskAnswer(State.COMPLETED));

        final JsonObject filesVerificationTaskPayload = getFilesVerificationTaskPayload(5);

        // Files verification task request is submitted from a destination host
        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
              .as(BodyCodec.jsonObject())
              .sendJsonObject(filesVerificationTaskPayload)
              .onSuccess(result -> context.verify(() -> {
                  assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                  JsonObject response = result.body();
                  assertThat(response).isNotNull();
                  assertThat(response.getString("taskId")).isNotNull();
                  assertThat(response.getString("statusUrl")).isNotNull();
              }))
              .onFailure(cause -> context.failNow("Files verification task submission request should not fail"))
              .compose(result -> {
                  JsonObject response = result.body();
                  return Future.succeededFuture(response.getString("statusUrl"));
              })
              .compose(statusUrl -> client.get(server.actualPort(), FIRST_DESTINATION_HOST, statusUrl)
                                          .as(BodyCodec.jsonObject())
                                          .send())
              .onFailure(cause -> context.failNow("Couldn't fetch files verification task status URL."))
              .onSuccess(taskStatusResult -> context.verify(() -> {
                  assertThat(taskStatusResult.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  JsonObject taskStatus = taskStatusResult.body();
                  assertThat(taskStatus).isNotNull();
                  assertThat(taskStatus.getString("id")).isNotNull();
                  assertThat(taskStatus.getString("state")).isNotNull();
              }))
              .compose(taskStatus -> client.get(server.actualPort(),
                                                FIRST_DESTINATION_HOST,
                                                LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
                                           .as(BodyCodec.jsonArray())
                                           .send())
              .onSuccess(allTasksResult -> context.verify(() -> {
                  assertThat(allTasksResult.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  JsonArray allTasksJsonArray = allTasksResult.body();
                  assertThat(allTasksJsonArray).isNotNull();
                  assertThat(allTasksJsonArray).hasSize(1);
                  JsonObject taskStatus = allTasksJsonArray.getJsonObject(0);
                  assertThat(taskStatus).isNotNull();
                  assertThat(taskStatus.getString("id")).isNotNull();
                  assertThat(taskStatus.getString("state")).isNotNull();
              }))
              .onFailure(context::failNow)
              .onSuccess(result -> context.completeNow());
    }

    @Test
    void testTaskSubmissionWhenAnotherTaskIsInProgress(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        LiveMigrationFilesVerificationTaskFactory taskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        when(taskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(getFakeVerificationTaskAnswer(State.IN_PROGRESS));

        final JsonObject filesVerificationTaskPayload = getFilesVerificationTaskPayload(5);

        // Files verification task request is submitted from a destination host
        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
              .as(BodyCodec.json(Map.class))
              .sendJsonObject(filesVerificationTaskPayload)
              .onSuccess(result -> context.verify(() -> assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code())))
              .onFailure(cause -> context.failNow("First files verification task submission failed."))
              .compose(response -> client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
                                         .as(BodyCodec.jsonObject())
                                         .sendJsonObject(filesVerificationTaskPayload))
              .onSuccess(result -> context.verify(() -> {
                  assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.CONFLICT.code());
                  JsonObject task = result.body();
                  assertThat(task).isNotNull();
                  assertThat(task.getString("message")).isNotNull();
              }))
              .onFailure(context::failNow) // The call should not result a failure
              .onSuccess(result -> context.completeNow());
    }

    @Test
    public void testTaskSubmissionWhenLiveMigrationAlreadyMarkedAsCompleted(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        LiveMigrationFilesVerificationTaskFactory taskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        when(taskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(getFakeVerificationTaskAnswer(State.NOT_STARTED));

        final JsonObject filesVerificationTaskPayload = getFilesVerificationTaskPayload(5);

        // First make request to live migration status API to mimic live migration completion and then
        // submit files verification task.
        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_STATUS_ROUTE)
              .send()
              .compose(response -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  return Future.succeededFuture(response);
              })
              .compose(response -> client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
                                         .as(BodyCodec.jsonObject())
                                         .sendJsonObject(filesVerificationTaskPayload))
              .compose(filesVerificationTaskResponse -> {
                  assertThat(filesVerificationTaskResponse.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
                  return Future.succeededFuture();
              })
              .onSuccess(v -> context.completeNow())
              .onFailure(context::failNow) // The call should not result a failure
              .onComplete(result -> client.close());
    }

    @Test
    void testCreateTaskCancelAndCreateAnotherTask(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        LiveMigrationFilesVerificationTaskFactory taskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        when(taskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(getFakeVerificationTaskAnswer(State.IN_PROGRESS));

        final JsonObject filesVerificationTaskPayload = getFilesVerificationTaskPayload(5);

        // Files verification task request is submitted from a destination host
        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
              .as(BodyCodec.jsonObject())
              .sendJsonObject(filesVerificationTaskPayload)
              .onSuccess(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                  JsonObject taskInfo = response.body();
                  assertThat(taskInfo).isNotNull();
                  assertThat(taskInfo.getString("statusUrl")).isNotNull();
              }))
              .onFailure(cause -> context.failNow("First files verification task submission failed."))
              .compose(response -> {
                  JsonObject taskInfo = response.body();
                  return Future.succeededFuture(taskInfo.getString("statusUrl"));
              })
              .compose(statusUrl -> client.patch(server.actualPort(), FIRST_DESTINATION_HOST, statusUrl)
                                          .as(BodyCodec.jsonObject())
                                          .send())
              .onSuccess(response -> context.verify(() -> assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code())))
              .compose(response -> client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
                                         .as(BodyCodec.jsonObject())
                                         .sendJsonObject(filesVerificationTaskPayload))
              .onSuccess(newTaskResult -> context.verify(() -> {
                  assertThat(newTaskResult.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                  JsonObject newTask = newTaskResult.body();
                  assertThat(newTask).isNotNull();
              }))
              .onFailure(context::failNow)
              .onSuccess(result -> context.completeNow());
    }

    @Test
    void testCreateTaskBadRequestSubmission(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        LiveMigrationFilesVerificationTaskFactory taskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        when(taskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(getFakeVerificationTaskAnswer(State.NOT_STARTED));

        // Invalid digest algorithm should throw validation errors
        final JsonObject filesVerificationTaskPayload = getFilesVerificationTaskPayload(5);
        JsonObject badRequest = filesVerificationTaskPayload.copy().put("digestAlgorithm", "INVALID_ALGO");

        // Files verification task request is submitted from a destination host
        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
              .sendJsonObject(badRequest, statusResult -> context.verify(() -> {
                  assertThat(statusResult.succeeded()).isTrue();
                  assertThat(statusResult.result().statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testCreateFilesVerificationTaskInvalidMaxConcurrency(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        LiveMigrationFilesVerificationTaskFactory taskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        SidecarConfiguration sidecarConfiguration = injector.getInstance(SidecarConfiguration.class);
        when(taskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(getFakeVerificationTaskAnswer(State.NOT_STARTED));

        // Request with max concurrency greater than the allowed limit should throw validation errors
        int invalidMaxConcurrency = sidecarConfiguration.liveMigrationConfiguration().maxConcurrentFileRequests() + 1;
        final JsonObject badRequest = getFilesVerificationTaskPayload(invalidMaxConcurrency);

        // Files verification task request is submitted from a destination host
        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
              .as(BodyCodec.jsonObject())
              .sendJsonObject(badRequest)
              .onSuccess(result -> context.verify(() -> {
                  assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
                  JsonObject response = result.body();
                  assertThat(response).isNotNull();
                  assertThat(response.getString("message")).isNotNull();
              }))
              .onFailure(context::failNow)
              .onSuccess(result -> context.completeNow());
    }

    @Test
    void testCreateFilesVerificationTaskMalformedRequest(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        LiveMigrationFilesVerificationTaskFactory taskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        when(taskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(getFakeVerificationTaskAnswer(State.NOT_STARTED));

        // Malformed request - missing required field 'digestAlgorithm'
        final JsonObject malformedRequest = new JsonObject()
                .put("maxConcurrency", 5);

        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
              .as(BodyCodec.jsonObject())
              .sendJsonObject(malformedRequest)
              .onSuccess(result -> context.verify(() -> {
                  assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
                  JsonObject response = result.body();
                  assertThat(response).isNotNull();
                  assertThat(response.getString("message")).isNotNull();
              }))
              .onFailure(context::failNow)
              .onSuccess(result -> context.completeNow());
    }

    @Test
    void testGetTaskStatusWhichDoesNotExist(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        client.get(server.actualPort(), FIRST_DESTINATION_HOST, getFilesVerificationTaskStatusUrl("taskdonotexist"))
              .send(statusResult -> context.verify(() -> {
                  assertThat(statusResult.succeeded()).isTrue();
                  assertThat(statusResult.result().statusCode()).isEqualTo(HttpResponseStatus.NOT_FOUND.code());
                  context.completeNow();
              }));
    }

    @Test
    void testCancelTaskWhichDoesNotExist(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        // No need to mock - the real manager will throw exception when task doesn't exist
        client.patch(server.actualPort(), FIRST_DESTINATION_HOST, getFilesVerificationTaskStatusUrl("taskdonotexist"))
              .send(statusResult -> context.verify(() -> {
                  assertThat(statusResult.succeeded()).isTrue();
                  // When task is not found, the handler logs a warning but doesn't fail the response
                  assertThat(statusResult.result().statusCode()).isEqualTo(HttpResponseStatus.NOT_FOUND.code());
                  assertThat(statusResult.result().bodyAsJsonObject()).isNotNull();
                  context.completeNow();
              }));
    }

    @Test
    void testCancelSucceededTask(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        LiveMigrationFilesVerificationTaskFactory taskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        when(taskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(getFakeVerificationTaskAnswer(State.COMPLETED));

        final JsonObject filesVerificationTaskPayload = getFilesVerificationTaskPayload(5);

        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
              .as(BodyCodec.jsonObject())
              .sendJsonObject(filesVerificationTaskPayload)
              .onSuccess(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                  JsonObject taskInfo = response.body();
                  assertThat(taskInfo).isNotNull();
                  assertThat(taskInfo.getString("statusUrl")).isNotNull();
              }))
              .onFailure(cause -> context.failNow("Files verification task submission failed."))
              .compose(response -> {
                  JsonObject taskInfo = response.body();
                  return Future.succeededFuture(taskInfo.getString("statusUrl"));
              })
              .compose(statusUrl -> client.patch(server.actualPort(), FIRST_DESTINATION_HOST, statusUrl)
                                          .as(BodyCodec.jsonObject())
                                          .send())
              .onSuccess(response -> context.verify(() -> assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code())))
              .onSuccess(cancelTaskResult -> context.verify(() -> {
                  assertThat(cancelTaskResult.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  JsonObject cancelledTask = cancelTaskResult.body();
                  assertThat(cancelledTask).isNotNull();
                  assertThat(cancelledTask.getString("state")).isEqualTo("COMPLETED");
              }))
              .onFailure(context::failNow)
              .onSuccess(result -> context.completeNow());
    }

    @Test
    void testCancelCancelledTask(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        LiveMigrationFilesVerificationTaskFactory taskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        when(taskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(getFakeVerificationTaskAnswer(State.CANCELLED));

        final JsonObject filesVerificationTaskPayload = getFilesVerificationTaskPayload(5);

        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
              .as(BodyCodec.jsonObject())
              .sendJsonObject(filesVerificationTaskPayload)
              .onSuccess(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                  JsonObject taskInfo = response.body();
                  assertThat(taskInfo).isNotNull();
                  assertThat(taskInfo.getString("statusUrl")).isNotNull();
              }))
              .onFailure(cause -> context.failNow("Files verification task submission failed."))
              .compose(response -> {
                  JsonObject taskInfo = response.body();
                  return Future.succeededFuture(taskInfo.getString("statusUrl"));
              })
              .compose(statusUrl -> client.patch(server.actualPort(), FIRST_DESTINATION_HOST, statusUrl)
                                          .as(BodyCodec.jsonObject())
                                          .send())
              .onSuccess(response -> context.verify(() -> assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code())))
              .onSuccess(cancelTaskResult -> context.verify(() -> {
                  assertThat(cancelTaskResult.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  JsonObject cancelledTask = cancelTaskResult.body();
                  assertThat(cancelledTask).isNotNull();
                  assertThat(cancelledTask.getString("state")).isEqualTo("CANCELLED");
              }))
              .onFailure(context::failNow)
              .onSuccess(result -> context.completeNow());
    }

    @Test
    void testGetAllTasksWhenNoTasksExist(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        client.get(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
              .as(BodyCodec.jsonArray())
              .send()
              .onSuccess(result -> context.verify(() -> {
                  assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  JsonArray allTasksJsonArray = result.body();
                  assertThat(allTasksJsonArray).isNotNull();
                  assertThat(allTasksJsonArray).isEmpty();
                  context.completeNow();
              }))
              .onFailure(context::failNow);
    }

    @Test
    void testGetAllTasksWithMultipleTasks(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        LiveMigrationFilesVerificationTaskFactory taskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        when(taskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(getFakeVerificationTaskAnswer(State.IN_PROGRESS));

        final JsonObject filesVerificationTaskPayload = getFilesVerificationTaskPayload(5);

        // Submit first task
        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
              .as(BodyCodec.jsonObject())
              .sendJsonObject(filesVerificationTaskPayload)
              .andThen(ar -> {
                  context.verify(() -> assertThat(ar.result().statusCode())
                                       .isEqualTo(HttpResponseStatus.ACCEPTED.code()));
              })
              .onFailure(cause -> context.failNow("First task submission failed."))
              .compose(response -> {
                  JsonObject taskInfo = response.body();
                  return Future.succeededFuture(taskInfo.getString("statusUrl"));
              })
              // Cancel first task so we can submit a second one
              .compose(statusUrl -> client.patch(server.actualPort(), FIRST_DESTINATION_HOST, statusUrl)
                                          .as(BodyCodec.jsonObject())
                                          .send())
              .andThen(response -> {
                  context.verify(() -> assertThat(response.result().statusCode())
                                       .isEqualTo(HttpResponseStatus.OK.code()));
              })
              // Submit second task
              .compose(response -> client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
                                         .as(BodyCodec.jsonObject())
                                         .sendJsonObject(filesVerificationTaskPayload))
              .andThen(ar -> {
                  context.verify(() -> assertThat(ar.result().statusCode())
                                       .isEqualTo(HttpResponseStatus.ACCEPTED.code()));
              })
              .onFailure(cause -> context.failNow("Second task submission failed."))
              // Get all tasks
              .compose(response -> client.get(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
                                         .as(BodyCodec.jsonArray())
                                         .send())
              .onSuccess(allTasksResult -> context.verify(() -> {
                  assertThat(allTasksResult.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  JsonArray allTasksJsonArray = allTasksResult.body();
                  assertThat(allTasksJsonArray).isNotNull();
                  assertThat(allTasksJsonArray).hasSize(1);
                  context.completeNow();
              }))
              .onFailure(context::failNow);
    }

    @Test
    void testHostIsolationForTasks(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        LiveMigrationFilesVerificationTaskFactory taskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        when(taskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(getFakeVerificationTaskAnswer(State.IN_PROGRESS));

        final JsonObject filesVerificationTaskPayload = getFilesVerificationTaskPayload(5);

        // Submit task on FIRST_DESTINATION_HOST
        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
              .as(BodyCodec.jsonObject())
              .sendJsonObject(filesVerificationTaskPayload)
              .andThen(ar -> {
                  context.verify(() -> assertThat(ar.result().statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code()));
              })
              .onFailure(cause -> context.failNow("Task submission on first host failed."))
              // Submit task on SECOND_DESTINATION_HOST
              .compose(response -> client.post(server.actualPort(), SECOND_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
                                         .as(BodyCodec.jsonObject())
                                         .sendJsonObject(filesVerificationTaskPayload))
              .andThen(ar -> {
                  context.verify(() -> assertThat(ar.result().statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code()));
              })
              .onFailure(cause -> context.failNow("Task submission on second host failed."))
              // Get tasks from FIRST_DESTINATION_HOST - should only see 1 task
              .compose(response -> client.get(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
                                         .as(BodyCodec.jsonArray())
                                         .send())
              .andThen(firstHostTasks -> context.verify(() -> {
                  assertThat(firstHostTasks.result().statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  JsonArray tasksArray = firstHostTasks.result().body();
                  assertThat(tasksArray).isNotNull();
                  assertThat(tasksArray).hasSize(1);
              }))
              // Get tasks from SECOND_DESTINATION_HOST - should only see 1 task
              .compose(response -> client.get(server.actualPort(), SECOND_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
                                         .as(BodyCodec.jsonArray())
                                         .send())
              .andThen(secondHostTasks -> context.verify(() -> {
                  assertThat(secondHostTasks.result().statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  JsonArray tasksArray = secondHostTasks.result().body();
                  assertThat(tasksArray).isNotNull();
                  assertThat(tasksArray).hasSize(1);
                  context.completeNow();
              }))
              .onFailure(context::failNow);
    }

    @Test
    void testValidDigestAlgorithmXXHash32(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        LiveMigrationFilesVerificationTaskFactory taskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        when(taskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(getFakeVerificationTaskAnswer(State.COMPLETED));

        final JsonObject filesVerificationTaskPayload = new JsonObject()
                .put("maxConcurrency", 5)
                .put("digestAlgorithm", "XXHash32");

        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
              .as(BodyCodec.jsonObject())
              .sendJsonObject(filesVerificationTaskPayload)
              .onSuccess(result -> context.verify(() -> {
                  assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                  JsonObject response = result.body();
                  assertThat(response).isNotNull();
                  assertThat(response.getString("taskId")).isNotNull();
                  assertThat(response.getString("statusUrl")).isNotNull();
                  context.completeNow();
              }))
              .onFailure(context::failNow);
    }

    @Test
    void testMinValidMaxConcurrency(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        LiveMigrationFilesVerificationTaskFactory taskFactory = injector.getInstance(LiveMigrationFilesVerificationTaskFactory.class);
        when(taskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(getFakeVerificationTaskAnswer(State.COMPLETED));

        final JsonObject filesVerificationTaskPayload = getFilesVerificationTaskPayload(1);

        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE)
              .as(BodyCodec.jsonObject())
              .sendJsonObject(filesVerificationTaskPayload)
              .onSuccess(result -> context.verify(() -> {
                  assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                  JsonObject response = result.body();
                  assertThat(response).isNotNull();
                  assertThat(response.getString("taskId")).isNotNull();
                  context.completeNow();
              }))
              .onFailure(context::failNow);
    }

    private JsonObject getFilesVerificationTaskPayload(int maxConcurrency)
    {
        return new JsonObject()
               .put("maxConcurrency", maxConcurrency)
               .put("digestAlgorithm", "MD5");
    }

    Answer<?> getFakeVerificationTaskAnswer(State state)
    {
        return (Answer<Object>) invocation -> new FakeFilesVerificationTask(
        new LiveMigrationFilesVerificationResponse(invocation.getArgument(0),
                                                   "MD5",
                                                   state.name(),
                                                   invocation.getArgument(1),
                                                   invocation.getArgument(2),
                                                   0,
                                                   0,
                                                   50,
                                                   0,
                                                   0,
                                                   0,
                                                   50));
    }

    @SuppressWarnings("SameParameterValue")
    private String getFilesVerificationTaskStatusUrl(String taskId)
    {
        return LIVE_MIGRATION_FILES_VERIFICATION_TASKS_ROUTE + "/" + taskId;
    }

    static class FilesVerificationHandlerTestModule extends AbstractModule
    {

        @Override
        protected void configure()
        {
            final Map<String, String> migrationMap = new HashMap<>()
            {{
                put("localhost2", "localhost");
                put(FIRST_SOURCE_HOST, FIRST_DESTINATION_HOST);
                put("localhost3", "localhost4");
                put(SECOND_SOURCE_HOST, SECOND_DESTINATION_HOST);
            }};

            LiveMigrationConfiguration mockLiveMigrationConfiguration = mock(LiveMigrationConfiguration.class);
            when(mockLiveMigrationConfiguration.filesToExclude())
            .thenReturn(Collections.emptySet());
            when(mockLiveMigrationConfiguration.directoriesToExclude())
            .thenReturn(Collections.singleton("glob:${DATA_FILE_DIR}/*/*/snapshots"));
            when(mockLiveMigrationConfiguration.migrationMap())
            .thenReturn(migrationMap);
            when(mockLiveMigrationConfiguration.maxConcurrentFileRequests()).thenReturn(10);

            SidecarConfiguration sidecarConfiguration = SidecarConfigurationImpl.builder()
                                                                                .liveMigrationConfiguration(mockLiveMigrationConfiguration)
                                                                                .build();

            bind(SidecarConfiguration.class).toInstance(sidecarConfiguration);
            bind(LiveMigrationFilesVerificationTaskFactory.class)
            .toInstance(mock(LiveMigrationFilesVerificationTaskFactory.class));
        }
    }
}
