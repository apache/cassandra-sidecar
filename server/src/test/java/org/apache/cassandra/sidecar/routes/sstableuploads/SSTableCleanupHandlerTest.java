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

package org.apache.cassandra.sidecar.routes.sstableuploads;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link org.apache.cassandra.sidecar.routes.SSTableUploadsHandler}
 */
@ExtendWith(VertxExtension.class)
public class SSTableCleanupHandlerTest extends BaseUploadsHandlerTest
{
    @Test
    void testCleanupUpload(VertxTestContext context) throws IOException
    {
        UUID uploadId = UUID.randomUUID();
        String testRoute = String.format("/api/v1/uploads/%s", uploadId);

        Path uploadRoot = createStagedUploadFiles(uploadId);

        client.delete(server.actualPort(), "localhost", testRoute)
              .expect(ResponsePredicate.SC_OK)
              .send(context.succeeding(response -> {
                  context.verify(() -> {
                      assertThat(response.body()).isNull();
                      assertThat(uploadRoot).doesNotExist();
                      context.completeNow();
                  });
              }));
    }

    @Test
    void testCleanupUploadBadRequest(VertxTestContext context)
    {
        client.delete(server.actualPort(), "localhost", "/api/v1/uploads/1234")
              .expect(ResponsePredicate.SC_BAD_REQUEST)
              .send(context.succeeding(response -> context.verify(() -> {
                  JsonObject error = response.bodyAsJsonObject();
                  assertThat(error.getInteger("code")).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
                  assertThat(error.getString("status")).isEqualTo(HttpResponseStatus.BAD_REQUEST.reasonPhrase());
                  // when enable dev mode, see build.gradle#test.systemProperty, the exception is attached
                  assertThat(error.getString("message"))
                  .isEqualTo("Invalid upload id is supplied, uploadId=1234");
                  // when dev mode is disabled, the response is {"status":"Bad Request", "code":400, "message":"..."}
                  context.completeNow();
              })));
    }

    @Test
    void testCleanupUploadNotFound(VertxTestContext context)
    {
        UUID uploadId = UUID.randomUUID();
        String testRoute = String.format("/api/v1/uploads/%s", uploadId);

        client.delete(server.actualPort(), "localhost", testRoute)
              .expect(ResponsePredicate.SC_NOT_FOUND)
              .send(context.succeeding(response -> context.completeNow()));
    }
}
