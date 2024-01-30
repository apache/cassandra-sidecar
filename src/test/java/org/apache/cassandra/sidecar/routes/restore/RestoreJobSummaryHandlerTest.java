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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.utils.UUIDs;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.Json;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.common.data.RestoreJobSummaryResponsePayload;
import org.apache.cassandra.sidecar.common.data.SSTableImportOptions;
import org.apache.cassandra.sidecar.db.RestoreJob;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class RestoreJobSummaryHandlerTest extends BaseRestoreJobTests
{
    private static final String RESTORE_JOB_INFO_ENDPOINT = "/api/v1/keyspaces/%s/tables/%s/restore-jobs/%s";

    @Test
    void testValidRequest(VertxTestContext context) throws Throwable
    {
        String jobId = "7cd82ff9-d276-11ed-93e5-7fce0df1306f";
        mockLookupRestoreJob(x -> {
            UUID id = UUID.fromString(jobId);
            // keyspace name is different
            return RestoreJob.builder()
                             .createdAt(LocalDate.fromMillisSinceEpoch(UUIDs.unixTimestamp(id)))
                             .jobId(id).jobAgent("job agent")
                             .keyspace("ks").table("table")
                             .jobStatus(RestoreJobStatus.CREATED)
                             .jobSecrets(SECRETS)
                             .sstableImportOptions(SSTableImportOptions.defaults())
                             .build();
        });
        sendGetRestoreJobSummaryRequestAndVerify("ks", "table", jobId, context, HttpResponseStatus.OK.code());
    }

    @Test
    void testInvalidJobId(VertxTestContext context) throws Throwable
    {
        sendGetRestoreJobSummaryRequestAndVerify("ks", "table", "12951f25-d393-4158-9e90-ec0cbe05af21",
                                                 context, HttpResponseStatus.BAD_REQUEST.code());
    }

    @Test
    void testInvalidKeyspace(VertxTestContext context) throws Throwable
    {
        sendGetRestoreJobSummaryRequestAndVerify("sidecar_internal", "table", "8e5799a4-d277-11ed-8d85-6916bb9b8056",
                                                 context, HttpResponseStatus.FORBIDDEN.code());
    }

    @Test
    void testNonMatchingKeyspaceTable(VertxTestContext context) throws Throwable
    {
        String jobId = "7cd82ff9-d276-11ed-93e5-7fce0df1306f";
        mockLookupRestoreJob(x -> {
            // keyspace name is different
            return RestoreJob.builder()
                             .createdAt(null)
                             .jobId(UUID.fromString(jobId))
                             .keyspace("ks").table("table")
                             .jobStatus(RestoreJobStatus.CREATED)
                             .build();
        });
        sendGetRestoreJobSummaryRequestAndVerify("ks1", "table", "7cd82ff9-d276-11ed-93e5-7fce0df1306f",
                                                 context,  HttpResponseStatus.NOT_FOUND.code());
    }

    @Test
    void testJobNotFound(VertxTestContext context) throws Throwable
    {
        // finds nothing == return null
        mockLookupRestoreJob(x -> null);
        sendGetRestoreJobSummaryRequestAndVerify("ks", "table", "8e5799a4-d277-11ed-8d85-6916bb9b8056",
                                                 context, HttpResponseStatus.NOT_FOUND.code());
    }

    @Test
    void testExceptionThrownDuringExecution(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(x -> {
            throw new RuntimeException("Execution failure");
        });
        sendGetRestoreJobSummaryRequestAndVerify("ks", "table", "7cd82ff9-d276-11ed-93e5-7fce0df1306f",
                                                 context, HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
    }

    @Test
    void testReadIncompleteRecordFails(VertxTestContext context) throws Throwable
    {
        mockLookupRestoreJob(x -> {
            UUID jobId = UUID.fromString("7cd82ff9-d276-11ed-93e5-7fce0df1306f");
            return RestoreJob.builder()
                             .createdAt(LocalDate.fromMillisSinceEpoch(UUIDs.unixTimestamp(jobId)))
                             .jobId(jobId).jobAgent("job agent")
                             .keyspace("ks").table("table")
                             .jobStatus(RestoreJobStatus.CREATED)
                             .build();
        });
        sendGetRestoreJobSummaryRequestAndVerify("ks", "table", "7cd82ff9-d276-11ed-93e5-7fce0df1306f",
                                                 context, HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
    }

    private void sendGetRestoreJobSummaryRequestAndVerify(String keyspace,
                                                          String table,
                                                          String jobId,
                                                          VertxTestContext context,
                                                          int expectedStatusCode) throws Throwable
    {
        WebClient client = WebClient.create(vertx, new WebClientOptions());
        client.get(server.actualPort(), "localhost", String.format(RESTORE_JOB_INFO_ENDPOINT, keyspace, table, jobId))
              .as(BodyCodec.buffer())
              .send(resp -> {
                  context.verify(() -> {
                      assertThat(resp.result().statusCode()).isEqualTo(expectedStatusCode);
                      if (expectedStatusCode == HttpResponseStatus.OK.code())
                      {
                          RestoreJobSummaryResponsePayload response
                          = Json.decodeValue(resp.result().body(), RestoreJobSummaryResponsePayload.class);
                          assertThat(response.keyspace()).isEqualTo(keyspace);
                          assertThat(response.table()).isEqualTo(table);
                          assertThat(response.jobAgent()).isNotNull();
                          assertThat(response.secrets()).isEqualTo(SECRETS);
                          assertThat(response.status()).isNotNull();
                      }
                  })
                  .completeNow();
                  client.close();
              });
        context.awaitCompletion(10, TimeUnit.SECONDS);
    }
}
