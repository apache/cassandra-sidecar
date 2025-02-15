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

package org.apache.cassandra.sidecar.restore;

import java.math.BigInteger;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.apache.cassandra.sidecar.cluster.locator.LocalTokenRangesProvider;
import org.apache.cassandra.sidecar.common.data.ConsistencyLevel;
import org.apache.cassandra.sidecar.common.request.data.CreateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.request.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.common.request.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.db.RestoreRangeDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.foundation.RestoreJobSecretsGen;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;
import org.apache.cassandra.sidecar.utils.SSTableImporter;

import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test utils for restore job related testing
 */
public class RestoreJobTestUtils
{
    private RestoreJobTestUtils()
    {
        throw new UnsupportedOperationException("Do not instantiate me");
    }

    public static RestoreJobClient client(WebClient client, String host, int port)
    {
        return new RestoreJobClient(client, host, port);
    }

    public static void assertRestoreRange(RestoreRange range, long startToken, long endToken)
    {
        assertRestoreRange(range, startToken, endToken, null);
    }

    public static void assertRestoreRange(RestoreRange range, long startToken, long endToken, Consumer<RestoreRange> additionalAssertions)
    {
        assertThat(range.startToken())
        .describedAs("startTokens do not match")
        .isEqualTo(BigInteger.valueOf(startToken));
        assertThat(range.endToken())
        .describedAs("endTokens do not match")
        .isEqualTo(BigInteger.valueOf(endToken));
        if (additionalAssertions != null)
        {
            additionalAssertions.accept(range);
        }
    }

    public static UUID createJob(RestoreJobTestUtils.RestoreJobClient testClient, QualifiedTableName tableName)
    {
        UUID jobId = UUIDs.timeBased();
        long expireAt = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
        CreateRestoreJobRequestPayload payload = CreateRestoreJobRequestPayload
                                                 .builder(RestoreJobSecretsGen.genRestoreJobSecrets(), expireAt)
                                                 .jobId(jobId)
                                                 .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM, "datacenter1").build();
        testClient.createRestoreJob(tableName, payload);
        return jobId;
    }

    public static Module disableRestoreProcessor()
    {
        return new AbstractModule()
        {
            @Provides
            @Singleton
            public RestoreProcessor processor(ExecutorPools executorPools,
                                              SidecarConfiguration config,
                                              SidecarSchema sidecarSchema,
                                              StorageClientPool s3ClientPool,
                                              SSTableImporter importer,
                                              RestoreRangeDatabaseAccessor rangeDatabaseAccessor,
                                              RestoreJobUtil restoreJobUtil,
                                              LocalTokenRangesProvider localTokenRangesProvider,
                                              SidecarMetrics metrics)
            {
                return new RestoreProcessor(executorPools, config, sidecarSchema,
                                            s3ClientPool, importer, rangeDatabaseAccessor,
                                            restoreJobUtil, localTokenRangesProvider, metrics)
                {
                    @Override
                    public ScheduleDecision scheduleDecision()
                    {
                        return ScheduleDecision.SKIP;
                    }
                };
            }
        };
    }

    /**
     * Wraps the webclient for restore job related API calls
     */
    public static class RestoreJobClient
    {
        WebClient client;
        String host;
        int port;

        private RestoreJobClient(WebClient client, String host, int port)
        {
            this.client = client;
            this.host = host;
            this.port = port;
        }

        public void createRestoreJob(QualifiedTableName qtn, CreateRestoreJobRequestPayload payload)
        {
            String testRoute = "/api/v1/keyspaces/" + qtn.keyspace() + "/tables/" + qtn.tableName() + "/restore-jobs";
            HttpResponse<Buffer> response = getBlocking(client.post(port, host, testRoute).sendJson(payload),
                                                        10, TimeUnit.SECONDS,
                                                        "Create RestoreJob");
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
        }

        public void updateRestoreJob(QualifiedTableName qtn, UUID jobId, UpdateRestoreJobRequestPayload payload)
        {
            String testRoute = "/api/v1/keyspaces/" + qtn.keyspace() + "/tables/" + qtn.tableName() + "/restore-jobs/" + jobId;
            HttpResponse<Buffer> response = getBlocking(client.patch(port, host, testRoute).sendJson(payload),
                                                        10, TimeUnit.SECONDS,
                                                        "Update RestoreJob");
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
        }

        public void createRestoreSlice(QualifiedTableName qtn, UUID jobId, CreateSliceRequestPayload payload)
        {
            String testRoute = "/api/v1/keyspaces/" + qtn.keyspace() + "/tables/" + qtn.tableName() + "/restore-jobs/" + jobId + "/slices";
            HttpResponse<Buffer> response = getBlocking(client.post(port, host, testRoute).sendJson(payload),
                                                        10, TimeUnit.SECONDS,
                                                        "Create RestoreSlice");
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.CREATED.code());
        }
    }
}
