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
import java.util.List;
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
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;
import org.apache.cassandra.sidecar.common.response.LiveMigrationDataCopyResponse;
import org.apache.cassandra.sidecar.common.response.LiveMigrationDataCopyResponse.Status;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationTaskFactory;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_STATUS_ROUTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
class LiveMigrationDataCopyTaskHandlerTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationDataCopyTaskHandlerTest.class);

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

    private JsonObject getDataCopyTaskPayload()
    {
        return new JsonObject()
               .put("maxIterations", 3)
               .put("successThreshold", 1.0)
               .put("maxConcurrency", 1);
    }

    @BeforeEach
    void setup(@TempDir Path tempDir) throws InterruptedException
    {
        DataCopyHandlerTestModule handlerTestModule = new DataCopyHandlerTestModule();
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
        LiveMigrationTaskFactory taskFactory = injector.getInstance(LiveMigrationTaskFactory.class);
        when(taskFactory.create(anyString(), any(LiveMigrationDataCopyRequest.class), anyString(), anyInt(), any(InstanceMetadata.class)))
        .thenAnswer(invocation -> {
            return new FakeLiveMigrationTask(
            new LiveMigrationDataCopyResponse(invocation.getArgument(0),
                                              "dummysource", 9043, invocation.getArgument(1, LiveMigrationDataCopyRequest.class),
                                              List.of(new Status(0, "SUCCESS", 1000, 10, 1000, 10, 10, 0, 1000))
            ));
        });

        final JsonObject dataCopyTaskPayload = getDataCopyTaskPayload();

        // Data copy task request can only be submitted from a destination host (since it follows a pull model)
        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE)
              .as(BodyCodec.jsonObject())
              .sendJsonObject(dataCopyTaskPayload)
              .onSuccess(result -> context.verify(() -> {
                  assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                  JsonObject response = result.body();
                  assertThat(response).isNotNull();
                  assertThat(response.getString("taskId")).isNotNull();
                  assertThat(response.getString("statusUrl")).isNotNull();
              }))
              .onFailure(cause -> context.failNow("Data copy task submission request should not fail"))
              .compose(result -> {
                  JsonObject response = result.body();
                  return Future.succeededFuture(response.getString("statusUrl"));
              })
              .compose(statusUrl -> client.get(server.actualPort(), FIRST_DESTINATION_HOST, statusUrl)
                                          .as(BodyCodec.jsonObject())
                                          .send())
              .onFailure(cause -> context.failNow("Couldn't fetch data copy task status URL."))
              .onSuccess(taskStatusResult -> context.verify(() -> {
                  assertThat(taskStatusResult.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  JsonObject taskStatus = taskStatusResult.body();
                  assertThat(taskStatus).isNotNull();
                  assertThat(taskStatus.getString("taskId")).isNotNull();
                  assertThat(taskStatus.getString("status")).isNotNull();
              }))
              .compose(taskStatus -> client.get(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE)
                                           .as(BodyCodec.jsonArray())
                                           .send())
              .onSuccess(allTasksResult -> context.verify(() -> {
                  assertThat(allTasksResult.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  JsonArray allTasksJsonArray = allTasksResult.body();
                  assertThat(allTasksJsonArray).isNotNull();
                  JsonObject taskStatus = allTasksJsonArray.getJsonObject(0);
                  assertThat(taskStatus).isNotNull();
                  assertThat(taskStatus.getString("taskId")).isNotNull();
                  assertThat(taskStatus.getString("status")).isNotNull();
              }))
              .onFailure(context::failNow)
              .onSuccess(result -> context.completeNow());
    }

    @Test
    void testTaskSubmissionWhenAnotherTaskIsInProgress(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        LiveMigrationTaskFactory taskFactory = injector.getInstance(LiveMigrationTaskFactory.class);
        when(taskFactory.create(anyString(), any(LiveMigrationDataCopyRequest.class), anyString(), anyInt(), any(InstanceMetadata.class)))
        .thenAnswer(invocation -> {
            return new FakeLiveMigrationTask(
            new LiveMigrationDataCopyResponse(invocation.getArgument(0),
                                              "dummysource", 9043, invocation.getArgument(1, LiveMigrationDataCopyRequest.class),
                                              List.of(new Status(0, "STARTING", -1, -1, -1, -1, 0, 0, 0))
            ));
        }); // Task in starting state only.

        final JsonObject dataCopyTaskPayload = getDataCopyTaskPayload();

        // Data copy task request can only be submitted from a destination host (since it follows a pull model)
        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE)
              .as(BodyCodec.json(Map.class))
              .sendJsonObject(dataCopyTaskPayload)
              .onSuccess(result -> context.verify(() -> assertThat(result.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code())))
              .onFailure(cause -> context.failNow("First data copy task submission failed."))
              .compose(response -> client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE)
                                         .as(BodyCodec.jsonObject())
                                         .sendJsonObject(dataCopyTaskPayload))
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

        LiveMigrationTaskFactory taskFactory = injector.getInstance(LiveMigrationTaskFactory.class);
        when(taskFactory.create(anyString(), any(LiveMigrationDataCopyRequest.class), anyString(), anyInt(), any(InstanceMetadata.class)))
        .thenAnswer(invocation -> new FakeLiveMigrationTask(new LiveMigrationDataCopyResponse(
        invocation.getArgument(0),
        "dummysource", 9043, invocation.getArgument(1, LiveMigrationDataCopyRequest.class),
        List.of(new Status(0, "STARTING", -1, -1, -1, -1, 0, 0, 0))
        ))); // Task in starting state only.

        final JsonObject dataCopyTaskPayload = getDataCopyTaskPayload();

        // First make request to live migration status API to mimic live migration completion and then
        // submit data copy task.
        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_STATUS_ROUTE)
              .send()
              .compose(response -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
                  return Future.succeededFuture(response);
              })
              .compose(response -> client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE)
                                         .as(BodyCodec.jsonObject())
                                         .sendJsonObject(dataCopyTaskPayload))
              .compose(dataCopyTaskResponse -> {
                  assertThat(dataCopyTaskResponse.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
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

        LiveMigrationTaskFactory taskFactory = injector.getInstance(LiveMigrationTaskFactory.class);
        when(taskFactory.create(anyString(), any(LiveMigrationDataCopyRequest.class), anyString(), anyInt(), any(InstanceMetadata.class)))
        .thenAnswer(invocation -> {
            return new FakeLiveMigrationTask(
            new LiveMigrationDataCopyResponse(invocation.getArgument(0),
                                              "dummysource", 9043, invocation.getArgument(1, LiveMigrationDataCopyRequest.class),
                                              List.of(new Status(0, "STARTING", -1, -1, -1, -1, 0, 0, 0))
            ));
        }); // Task in starting state only.

        final JsonObject dataCopyTaskPayload = getDataCopyTaskPayload();

        // Data copy task request can only be submitted from a destination host (since it follows a pull model)
        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE)
              .as(BodyCodec.jsonObject())
              .sendJsonObject(dataCopyTaskPayload)
              .onSuccess(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                  JsonObject taskInfo = response.body();
                  assertThat(taskInfo).isNotNull();
                  assertThat(taskInfo.getString("statusUrl")).isNotNull();
              }))
              .onFailure(cause -> context.failNow("First data copy task submission failed."))
              .compose(response -> {
                  JsonObject taskInfo = response.body();
                  return Future.succeededFuture(taskInfo.getString("statusUrl"));
              })
              .compose(statusUrl -> client.patch(server.actualPort(), FIRST_DESTINATION_HOST, statusUrl)
                                          .as(BodyCodec.jsonObject())
                                          .send())
              .onSuccess(response -> context.verify(() -> assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code())))
              .compose(response -> client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE)
                                         .as(BodyCodec.jsonObject())
                                         .sendJsonObject(dataCopyTaskPayload))
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
        LiveMigrationTaskFactory taskFactory = injector.getInstance(LiveMigrationTaskFactory.class);
        when(taskFactory.create(anyString(), any(LiveMigrationDataCopyRequest.class), anyString(), anyInt(), any(InstanceMetadata.class)))
        .thenAnswer(invocation -> {
            return new FakeLiveMigrationTask(
            new LiveMigrationDataCopyResponse(invocation.getArgument(0),
                                              "dummysource", 9043, invocation.getArgument(1, LiveMigrationDataCopyRequest.class),
                                              List.of(new Status(0, "SUCCESS", 1000, 10, 1000, 10, 10, 0, 1000))
            ));
        });

        // successThreshold is range bound and valid range is (0.0, 1.0] and thus the below value should throw
        // validation errors. This test case is trying to test this particular scenario.
        final JsonObject dataCopyTaskPayload = getDataCopyTaskPayload();
        JsonObject badRequest = dataCopyTaskPayload.copy().put("successThreshold", 1.1);

        // Data copy task request can only be submitted from a destination host (since it follows a pull model)
        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE)
              .sendJsonObject(badRequest, statusResult -> context.verify(() -> {
                  assertThat(statusResult.succeeded()).isTrue();
                  assertThat(statusResult.result().statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
                  context.completeNow();
              }));
    }

    @Test
    void testCreateDataCopyTaskInvalidMaxConcurrency(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);
        LiveMigrationTaskFactory taskFactory = injector.getInstance(LiveMigrationTaskFactory.class);
        SidecarConfiguration sidecarConfiguration = injector.getInstance(SidecarConfiguration.class);
        when(taskFactory.create(anyString(), any(LiveMigrationDataCopyRequest.class), anyString(), anyInt(), any(InstanceMetadata.class)))
        .thenAnswer(invocation -> {
            return new FakeLiveMigrationTask(
            new LiveMigrationDataCopyResponse(invocation.getArgument(0),
                                              "dummysource", 9043, invocation.getArgument(1, LiveMigrationDataCopyRequest.class),
                                              List.of(new Status(0, "SUCCESS", 1000, 10, 1000, 10, 10, 0, 1000))
            ));
        });

        // Here, we are trying to trigger a data copy request with max concurrency greater than the allowed limit.
        // Thus, this should throw validation errors and this test case is trying to test this particular scenario.
        final JsonObject dataCopyTaskPayload = getDataCopyTaskPayload();
        JsonObject badRequest = dataCopyTaskPayload.copy().put("maxConcurrency", sidecarConfiguration.liveMigrationConfiguration().maxConcurrentFileRequests() + 1);

        // Data copy task request can only be submitted from a destination host (since it follows a pull model)
        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE)
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
    void testGetTaskStatusWhichDoesNotExist(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        client.get(server.actualPort(), FIRST_DESTINATION_HOST, getLiveMigrationTaskStatusUrl("taskdonotexist"))
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

        client.patch(server.actualPort(), FIRST_DESTINATION_HOST, getLiveMigrationTaskStatusUrl("taskdonotexist"))
              .send(statusResult -> context.verify(() -> {
                  assertThat(statusResult.succeeded()).isTrue();
                  assertThat(statusResult.result().statusCode()).isEqualTo(HttpResponseStatus.NOT_FOUND.code());
                  context.completeNow();
              }));
    }

    @Test
    void testCancelSucceededTask(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        LiveMigrationTaskFactory taskFactory = injector.getInstance(LiveMigrationTaskFactory.class);
        when(taskFactory.create(anyString(), any(LiveMigrationDataCopyRequest.class), anyString(), anyInt(), any(InstanceMetadata.class)))
        .thenAnswer(invocation -> {
            return new FakeLiveMigrationTask(
            new LiveMigrationDataCopyResponse(invocation.getArgument(0),
                                              "dummysource", 9043, invocation.getArgument(1, LiveMigrationDataCopyRequest.class),
                                              List.of(new Status(0, "SUCCESS", 1000, 2, 1000, 2, 2, 0, 1000))
            ));
        });

        final JsonObject dataCopyTaskPayload = getDataCopyTaskPayload();

        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE)
              .as(BodyCodec.jsonObject())
              .sendJsonObject(dataCopyTaskPayload)
              .onSuccess(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                  JsonObject taskInfo = response.body();
                  assertThat(taskInfo).isNotNull();
                  assertThat(taskInfo.getString("statusUrl")).isNotNull();
              }))
              .onFailure(cause -> context.failNow("First data copy task submission failed."))
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
                  assertThat(cancelledTask.getJsonArray("status").getJsonObject(0).getString("state"))
                  .isEqualTo("SUCCESS");
              }))
              .onFailure(context::failNow)
              .onSuccess(result -> context.completeNow());
    }

    @Test
    void testCancelCancelledTask(VertxTestContext context)
    {
        WebClient client = WebClient.create(vertx);

        LiveMigrationTaskFactory taskFactory = injector.getInstance(LiveMigrationTaskFactory.class);
        when(taskFactory.create(anyString(), any(LiveMigrationDataCopyRequest.class), anyString(), anyInt(), any(InstanceMetadata.class)))
        .thenAnswer(invocation -> {
            return new FakeLiveMigrationTask(
            new LiveMigrationDataCopyResponse(invocation.getArgument(0),
                                              "dummysource", 9043, invocation.getArgument(1, LiveMigrationDataCopyRequest.class),
                                              List.of(new Status(0, "CANCELLED", 1000, 2, 1000, 2, 1, 0, 100))
            ));
        });

        final JsonObject dataCopyTaskPayload = getDataCopyTaskPayload();

        client.post(server.actualPort(), FIRST_DESTINATION_HOST, LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE)
              .as(BodyCodec.jsonObject())
              .sendJsonObject(dataCopyTaskPayload)
              .onSuccess(response -> context.verify(() -> {
                  assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.ACCEPTED.code());
                  JsonObject taskInfo = response.body();
                  assertThat(taskInfo).isNotNull();
                  assertThat(taskInfo.getString("statusUrl")).isNotNull();
              }))
              .onFailure(cause -> context.failNow("First data copy task submission failed."))
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
                  assertThat(cancelledTask.getJsonArray("status").getJsonObject(0).getString("state"))
                  .isEqualTo("CANCELLED");
              }))
              .onFailure(context::failNow)
              .onSuccess(result -> context.completeNow());
    }

    @SuppressWarnings("SameParameterValue")
    private String getLiveMigrationTaskStatusUrl(String taskId)
    {
        return LIVE_MIGRATION_DATA_COPY_TASKS_ROUTE + "/" + taskId;
    }

    static class DataCopyHandlerTestModule extends AbstractModule
    {

        @Override
        protected void configure()
        {
            final Map<String, String> migrationMap = new HashMap<>()
            {{
                put("localhost2", "localhost");
                put(FIRST_SOURCE_HOST, FIRST_DESTINATION_HOST);
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
            bind(LiveMigrationTaskFactory.class).toInstance(mock(LiveMigrationTaskFactory.class));
        }
    }
}
