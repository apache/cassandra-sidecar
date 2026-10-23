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

package org.apache.cassandra.sidecar.handlers.restore;

import java.util.EnumSet;
import java.util.UUID;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.common.data.CredentialType;
import org.apache.cassandra.sidecar.config.yaml.RestoreJobConfigurationImpl;
import org.apache.cassandra.sidecar.db.RestoreJobTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that {@link CreateRestoreJobHandler} enforces the server-side {@code allowed_credential_types}
 * policy (see {@link RestoreJobConfigurationImpl#allowedCredentialTypes()}), rejecting a restore job
 * creation request whose {@code credentialType} is not permitted by the operator's configuration.
 */
@ExtendWith(VertxExtension.class)
class CreateRestoreJobHandlerCredentialPolicyTest
{
    private static final String CREATE_RESTORE_JOB_ENDPOINT = "/api/v1/keyspaces/%s/tables/%s/restore-jobs";
    private static final String JOB_ID = "8e5799a4-d277-11ed-8d85-6916bb9b8056";

    @Nested
    class IamOnlyPolicy extends BaseRestoreJobTests
    {
        @Override
        protected void configureTestModule(TestModule testModule)
        {
            testModule.restoreJobConfiguration(RestoreJobConfigurationImpl.builder()
                                                                           .allowedCredentialTypes(EnumSet.of(CredentialType.IAM))
                                                                           .build());
        }

        @Test
        void rejectsStaticCredentialsRequest(VertxTestContext context) throws Throwable
        {
            mockLookupRestoreJob(id -> null);
            JsonObject payload = new JsonObject();
            payload.put("jobId", JOB_ID);
            payload.put("secrets", SECRETS); // defaults to STATIC credentialType
            payload.put("expireAt", System.currentTimeMillis() + 10000L);
            postThenComplete(context, String.format(CREATE_RESTORE_JOB_ENDPOINT, "ks", "table"), payload, asyncResult -> {
                HttpResponse<?> resp = asyncResult.result();
                assertThat(resp).isNotNull();
                assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
            });
        }

        @Test
        void allowsIamCredentialsRequest(VertxTestContext context) throws Throwable
        {
            mockCreateRestoreJob(x -> RestoreJobTest.createNewTestingJob(UUID.fromString(JOB_ID)));
            mockLookupRestoreJob(id -> null);
            JsonObject regionOnlyCredentials = new JsonObject().put("region", "us-east-1");
            JsonObject secrets = new JsonObject()
                                 .put("readCredentials", regionOnlyCredentials)
                                 .put("writeCredentials", regionOnlyCredentials);
            JsonObject payload = new JsonObject();
            payload.put("jobId", JOB_ID);
            payload.put("credentialType", "IAM");
            payload.put("secrets", secrets);
            payload.put("expireAt", System.currentTimeMillis() + 10000L);
            postThenComplete(context, String.format(CREATE_RESTORE_JOB_ENDPOINT, "ks", "table"), payload, asyncResult -> {
                HttpResponse<?> resp = asyncResult.result();
                assertThat(resp).isNotNull();
                assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            });
        }
    }

    @Nested
    class StaticOnlyPolicy extends BaseRestoreJobTests
    {
        @Override
        protected void configureTestModule(TestModule testModule)
        {
            testModule.restoreJobConfiguration(RestoreJobConfigurationImpl.builder()
                                                                           .allowedCredentialTypes(EnumSet.of(CredentialType.STATIC))
                                                                           .build());
        }

        @Test
        void rejectsIamCredentialsRequest(VertxTestContext context) throws Throwable
        {
            mockLookupRestoreJob(id -> null);
            JsonObject regionOnlyCredentials = new JsonObject().put("region", "us-east-1");
            JsonObject secrets = new JsonObject()
                                 .put("readCredentials", regionOnlyCredentials)
                                 .put("writeCredentials", regionOnlyCredentials);
            JsonObject payload = new JsonObject();
            payload.put("jobId", JOB_ID);
            payload.put("credentialType", "IAM");
            payload.put("secrets", secrets);
            payload.put("expireAt", System.currentTimeMillis() + 10000L);
            postThenComplete(context, String.format(CREATE_RESTORE_JOB_ENDPOINT, "ks", "table"), payload, asyncResult -> {
                HttpResponse<?> resp = asyncResult.result();
                assertThat(resp).isNotNull();
                assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
            });
        }

        @Test
        void allowsStaticCredentialsRequest(VertxTestContext context) throws Throwable
        {
            mockCreateRestoreJob(x -> RestoreJobTest.createNewTestingJob(UUID.fromString(JOB_ID)));
            mockLookupRestoreJob(id -> null);
            JsonObject payload = new JsonObject();
            payload.put("jobId", JOB_ID);
            payload.put("secrets", SECRETS); // defaults to STATIC credentialType
            payload.put("expireAt", System.currentTimeMillis() + 10000L);
            postThenComplete(context, String.format(CREATE_RESTORE_JOB_ENDPOINT, "ks", "table"), payload, asyncResult -> {
                HttpResponse<?> resp = asyncResult.result();
                assertThat(resp).isNotNull();
                assertThat(resp.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
            });
        }
    }
}
