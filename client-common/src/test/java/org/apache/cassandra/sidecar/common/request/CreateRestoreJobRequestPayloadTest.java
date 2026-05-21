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

package org.apache.cassandra.sidecar.common.request;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import org.apache.cassandra.sidecar.common.data.ConsistencyLevel;
import org.apache.cassandra.sidecar.common.data.CredentialType;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.SSTableImportOptions;
import org.apache.cassandra.sidecar.common.data.StorageCredentials;
import org.apache.cassandra.sidecar.common.request.data.CreateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.foundation.RestoreJobSecretsGen;

import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_CONSISTENCY_LEVEL;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_RESTORE_TO_LOCAL_DATA_CENTER_ONLY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CreateRestoreJobRequestPayloadTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

    @Test
    void testSerDeser() throws JsonProcessingException
    {
        String id = "e870e5dc-d25e-11ed-afa1-0242ac120002";
        RestoreJobSecrets secrets = RestoreJobSecretsGen.genRestoreJobSecrets();
        long expireAt = System.currentTimeMillis() + 10000;
        Date date = Date.from(Instant.ofEpochMilli(expireAt));
        CreateRestoreJobRequestPayload req = CreateRestoreJobRequestPayload.builder(secrets, expireAt)
                                                                           .jobId(UUID.fromString(id))
                                                                           .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM, "DC1")
                                                                           .jobAgent("agent")
                                                                           .build();
        String json = MAPPER.writeValueAsString(req);
        // Check out the javadoc com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT for definition
        assertThat(json).describedAs("Default value fields should be excluded").doesNotContain(JOB_RESTORE_TO_LOCAL_DATA_CENTER_ONLY)
                        .describedAs("Non-default value fields should be included").contains(JOB_CONSISTENCY_LEVEL)
                        .isEqualTo("{\"jobId\":\"e870e5dc-d25e-11ed-afa1-0242ac120002\"," +
                                   "\"jobAgent\":\"agent\"," +
                                   "\"secrets\":" + MAPPER.writeValueAsString(secrets) + "," +
                                   "\"credentialType\":\"STATIC\"," +
                                   "\"importOptions\":{" +
                                   "\"resetLevel\":\"true\"," +
                                   "\"clearRepaired\":\"true\"," +
                                   "\"verifySSTables\":\"true\"," +
                                   "\"verifyTokens\":\"true\"," +
                                   "\"invalidateCaches\":\"true\"," +
                                   "\"extendedVerify\":\"true\"," +
                                   "\"copyData\":\"false\"," +
                                   "\"failOnMissingIndex\":\"false\"," +
                                   "\"validateIndexChecksum\":\"false\"}," +
                                   "\"expireAt\":" + expireAt + "," +
                                   "\"consistencyLevel\":\"LOCAL_QUORUM\"," +
                                   "\"localDatacenter\":\"DC1\"}");
        CreateRestoreJobRequestPayload test = MAPPER.readValue(json, CreateRestoreJobRequestPayload.class);
        assertThat(test.jobId()).hasToString(id);
        assertThat(test.jobAgent()).isEqualTo("agent");
        assertThat(test.secrets()).isEqualTo(secrets);
        assertThat(test.expireAtInMillis()).isEqualTo(expireAt);
        assertThat(test.expireAtAsDate()).isEqualTo(date);
        assertThat(test.importOptions()).isEqualTo(SSTableImportOptions.defaults());
        assertThat(test.consistencyLevel()).isEqualTo("LOCAL_QUORUM");
        assertThat(test.localDatacenter()).isEqualTo("DC1");
        assertThat(test.shouldRestoreToLocalDatacenterOnly()).isFalse();
        assertThat(test.storageRegion()).isEqualTo(secrets.readCredentials().region());
    }

    @Test
    void testReadFromJsonFailsWithUnknownFields() throws JsonProcessingException
    {
        String uuid = "e870e5dc-d25e-11ed-afa1-0242ac120002";
        String json = "{\"jobId\":\"" + uuid + "\"," +
                      "\"jobAgent\":\"Spark Bulk Analytics\"," +
                      "\"status\":\"Completed\"," +
                      "\"expireAt\":" + (System.currentTimeMillis() + 1000) +
                      ",\"secrets\":" + MAPPER.writeValueAsString(RestoreJobSecretsGen.genRestoreJobSecrets()) + "}";
        assertThatThrownBy(() -> MAPPER.readValue(json, CreateRestoreJobRequestPayload.class))
        .isInstanceOf(UnrecognizedPropertyException.class)
        .hasMessageContaining("Unrecognized field \"status\"");
    }

    @Test
    void testReadFromJsonWithInvalidSecrets()
    {
        // Credentials with accessKeyId but no region — StorageCredentials requires region
        String json = "{\"secrets\":" +
                      "{\"readCredentials\":{\"accessKeyId\":\"accessKeyId\"}," +
                      "\"writeCredentials\":{\"accessKeyId\":\"accessKeyId\"}}}";
        assertThatThrownBy(() -> MAPPER.readValue(json, CreateRestoreJobRequestPayload.class))
        .isInstanceOf(ValueInstantiationException.class)
        .hasCauseInstanceOf(NullPointerException.class)
        .hasMessageContaining("region must be supplied");
    }

    @Test
    void testReadFromJsonWithPartialFields() throws JsonProcessingException
    {
        RestoreJobSecrets secrets = RestoreJobSecretsGen.genRestoreJobSecrets();
        String json = "{\"secrets\":" + MAPPER.writeValueAsString(secrets) +
                      ", \"expireAt\":" + (System.currentTimeMillis() + 1000) + "}";
        CreateRestoreJobRequestPayload req = MAPPER.readValue(json, CreateRestoreJobRequestPayload.class);
        assertThat(req).isNotNull();
        assertThat(req.jobId()).isNull();
        assertThat(req.jobAgent()).isNull();
        assertThat(req.secrets()).isEqualTo(secrets);
        assertThat(req.importOptions()).isEqualTo(SSTableImportOptions.defaults());
    }

    @Test
    void testReadFromJsonFailsWithoutSecrets()
    {
        // secrets is always required; there is no sentinel fallback field
        String json = "{\"expireAt\":" + (System.currentTimeMillis() + 1000) + "}";
        assertThatThrownBy(() -> MAPPER.readValue(json, CreateRestoreJobRequestPayload.class))
        .isInstanceOf(ValueInstantiationException.class)
        .hasCauseInstanceOf(NullPointerException.class)
        .hasMessageContaining("secrets must be provided");
    }

    @Test
    void testReadFromJsonFailsWithOutExpireAt() throws JsonProcessingException
    {
        RestoreJobSecrets secrets = RestoreJobSecretsGen.genRestoreJobSecrets();
        String json = "{\"secrets\":" + MAPPER.writeValueAsString(secrets) + "}";
        assertThatThrownBy(() -> MAPPER.readValue(json, CreateRestoreJobRequestPayload.class))
        .isInstanceOf(ValueInstantiationException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expireAt cannot be absent or a time in past");
    }

    @Test
    void testReadFromJsonFailWithInvalidExpireAt() throws JsonProcessingException
    {
        RestoreJobSecrets secrets = RestoreJobSecretsGen.genRestoreJobSecrets();
        String json = "{\"secrets\":" + MAPPER.writeValueAsString(secrets) +
                      ", \"expireAt\":" + (System.currentTimeMillis() - 1000) + "}";
        assertThatThrownBy(() -> MAPPER.readValue(json, CreateRestoreJobRequestPayload.class))
        .isInstanceOf(ValueInstantiationException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("expireAt cannot be absent or a time in past");
    }

    @Test
    void testReadFromJsonFailsWithInvalidJobId() throws JsonProcessingException
    {
        String json = "{\"jobId\":\"12951f25-d393-4158-9e90-ec0cbe05af21\"," +
                      "\"expireAt\":\"" + (System.currentTimeMillis() + 1000) + "\"," +
                      "\"secrets\":" + MAPPER.writeValueAsString(RestoreJobSecretsGen.genRestoreJobSecrets()) + "}";
        assertThatThrownBy(() -> MAPPER.readValue(json, CreateRestoreJobRequestPayload.class))
        .isInstanceOf(ValueInstantiationException.class);
    }

    @Test
    void testReadFromJsonWithoutConsistencyLevel() throws JsonProcessingException
    {
        RestoreJobSecrets secrets = RestoreJobSecretsGen.genRestoreJobSecrets();
        long time = System.currentTimeMillis() + 10000;
        Date date = Date.from(Instant.ofEpochMilli(time));
        String json = "{\"jobId\":\"e870e5dc-d25e-11ed-afa1-0242ac120002\"," +
                      "\"jobAgent\":\"agent\"," +
                      "\"expireAt\":\"" + time + "\"," +
                      "\"secrets\":" + MAPPER.writeValueAsString(secrets) + "}";
        CreateRestoreJobRequestPayload test = MAPPER.readValue(json, CreateRestoreJobRequestPayload.class);
        assertThat(test.jobId()).hasToString("e870e5dc-d25e-11ed-afa1-0242ac120002");
        assertThat(test.jobAgent()).isEqualTo("agent");
        assertThat(test.secrets()).isEqualTo(secrets);
        assertThat(test.expireAtInMillis()).isEqualTo(time);
        assertThat(test.expireAtAsDate()).isEqualTo(date);
        assertThat(test.importOptions()).isEqualTo(SSTableImportOptions.defaults());
        assertThat(test.consistencyLevel()).isNull();
        assertThat(test.shouldRestoreToLocalDatacenterOnly()).isFalse();
    }

    @Test
    void testBuilder()
    {
        RestoreJobSecrets secrets = RestoreJobSecretsGen.genRestoreJobSecrets();
        CreateRestoreJobRequestPayload req = CreateRestoreJobRequestPayload
                                             .builder(secrets, System.currentTimeMillis() + 10000)
                                             .jobAgent("agent")
                                             .updateImportOptions(options -> {
                                                 options
                                                 .resetLevel(false)
                                                 .clearRepaired(false);
                                             })
                                             .consistencyLevel(ConsistencyLevel.QUORUM)
                                             .build();
        assertThat(req.secrets()).isEqualTo(secrets);
        assertThat(req.jobAgent()).isEqualTo("agent");
        assertThat(req.importOptions()).isEqualTo(SSTableImportOptions.defaults()
                                                                      .resetLevel(false)
                                                                      .clearRepaired(false));
        assertThat(req.consistencyLevel()).isEqualTo("QUORUM");
    }

    @Test
    void testCreateLocalQuorumJobWithoutLocalDCFails()
    {
        RestoreJobSecrets secrets = RestoreJobSecretsGen.genRestoreJobSecrets();

        for (ConsistencyLevel localCL : Arrays.asList(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_ONE))
        {
            assertThatThrownBy(() -> CreateRestoreJobRequestPayload.builder(secrets, System.currentTimeMillis() + 10000)
                                                                   .consistencyLevel(localCL)
                                                                   .build())
            .hasMessage("Must specify a non-empty localDatacenter for consistency level: " + localCL.name());

            assertThatThrownBy(() -> CreateRestoreJobRequestPayload.builder(secrets, System.currentTimeMillis() + 10000)
                                                                   .consistencyLevel(localCL, "")
                                                                   .build())
            .hasMessage("Must specify a non-empty localDatacenter for consistency level: " + localCL.name());
        }
    }

    @Test
    void testRestoreToLocalDatacenterOnly()
    {
        RestoreJobSecrets secrets = RestoreJobSecretsGen.genRestoreJobSecrets();
        CreateRestoreJobRequestPayload req = CreateRestoreJobRequestPayload
                                             .builder(secrets, System.currentTimeMillis() + 10000)
                                             .jobAgent("agent")
                                             .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM, "dc1")
                                             .restoreToLocalDatacenterOnly(true)
                                             .build();
        assertThat(req.shouldRestoreToLocalDatacenterOnly()).isTrue();
    }

    @Test
    void testRestoreToLocalDatacenterOnlyWithSpecifyingLocalDatacenterFails()
    {
        RestoreJobSecrets secrets = RestoreJobSecretsGen.genRestoreJobSecrets();
        assertThatThrownBy(() -> CreateRestoreJobRequestPayload
                                 .builder(secrets, System.currentTimeMillis() + 10000)
                                 .jobAgent("agent")
                                 .consistencyLevel(ConsistencyLevel.QUORUM)
                                 .restoreToLocalDatacenterOnly(true)
                                 .build())
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("Must specify a localDatacenter when restoreToLocalDatacenterOnly is true");
    }

    @Test
    void testIamModeWithRegionOnly()
    {
        long expireAt = System.currentTimeMillis() + 10000;
        RestoreJobSecrets iamSecrets = RestoreJobSecrets.iamMode("us-east-1");
        CreateRestoreJobRequestPayload req = CreateRestoreJobRequestPayload
                                             .builder(iamSecrets, expireAt)
                                             .credentialType(CredentialType.IAM)
                                             .jobAgent("agent")
                                             .build();
        assertThat(req.credentialType()).isEqualTo(CredentialType.IAM);
        assertThat(req.secrets()).isNotNull();
        assertThat(req.storageRegion()).isEqualTo("us-east-1");
        assertThat(req.secrets().readCredentials().region()).isEqualTo("us-east-1");
        assertThat(req.secrets().readCredentials().accessKeyId()).isNull();
        assertThat(req.secrets().readCredentials().secretAccessKey()).isNull();
        assertThat(req.secrets().readCredentials().sessionToken()).isNull();
        assertThat(req.secrets().readCredentials().hasStaticCredentials()).isFalse();
    }

    @Test
    void testIamModeSerDeser() throws JsonProcessingException
    {
        long expireAt = System.currentTimeMillis() + 10000;
        CreateRestoreJobRequestPayload req = CreateRestoreJobRequestPayload
                                             .builder(RestoreJobSecrets.iamMode("us-west-2"), expireAt)
                                             .credentialType(CredentialType.IAM)
                                             .build();
        String json = MAPPER.writeValueAsString(req);
        assertThat(json).contains("\"us-west-2\"")
                        .contains("\"credentialType\":\"IAM\"")
                        .doesNotContain("\"accessKeyId\"")
                        .doesNotContain("\"secretAccessKey\"")
                        .doesNotContain("\"sessionToken\"");

        CreateRestoreJobRequestPayload deserialized = MAPPER.readValue(json, CreateRestoreJobRequestPayload.class);
        assertThat(deserialized.credentialType()).isEqualTo(CredentialType.IAM);
        assertThat(deserialized.storageRegion()).isEqualTo("us-west-2");
        assertThat(deserialized.secrets().readCredentials().hasStaticCredentials()).isFalse();
        assertThat(deserialized.secrets().readCredentials().accessKeyId()).isNull();
        assertThat(deserialized.secrets().readCredentials().secretAccessKey()).isNull();
        assertThat(deserialized.secrets().readCredentials().sessionToken()).isNull();
    }

    @Test
    void testIamModeFromJson() throws JsonProcessingException
    {
        long expireAt = System.currentTimeMillis() + 10000;
        String json = "{\"credentialType\":\"IAM\"," +
                      "\"secrets\":{\"readCredentials\":{\"region\":\"eu-west-1\"}," +
                      "\"writeCredentials\":{\"region\":\"eu-west-1\"}}," +
                      "\"expireAt\":" + expireAt + "}";
        CreateRestoreJobRequestPayload req = MAPPER.readValue(json, CreateRestoreJobRequestPayload.class);
        assertThat(req.credentialType()).isEqualTo(CredentialType.IAM);
        assertThat(req.storageRegion()).isEqualTo("eu-west-1");
        assertThat(req.secrets().readCredentials().hasStaticCredentials()).isFalse();
        assertThat(req.secrets().readCredentials().accessKeyId()).isNull();
        assertThat(req.secrets().readCredentials().secretAccessKey()).isNull();
        assertThat(req.secrets().readCredentials().sessionToken()).isNull();
    }

    @Test
    void testIamModeWithStaticCredentialsIsRejected() throws JsonProcessingException
    {
        long expireAt = System.currentTimeMillis() + 10000;
        StorageCredentials fullCreds = RestoreJobSecretsGen.genReadStorageCredentials();
        String json = "{\"credentialType\":\"IAM\"," +
                      "\"secrets\":{\"readCredentials\":" + MAPPER.writeValueAsString(fullCreds) + "," +
                      "\"writeCredentials\":" + MAPPER.writeValueAsString(fullCreds) + "}," +
                      "\"expireAt\":" + expireAt + "}";
        assertThatThrownBy(() -> MAPPER.readValue(json, CreateRestoreJobRequestPayload.class))
        .isInstanceOf(ValueInstantiationException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("IAM credentials must not contain key fields");
    }

    @Test
    void testIamModeFactoryRequiresRegion()
    {
        // RestoreJobSecrets.iamMode requires a non-null region; null means StorageCredentials constructor
        // will throw because region is the only required field in that class
        assertThatThrownBy(() -> RestoreJobSecrets.iamMode(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("region must be supplied");
    }

    @Test
    void testPartialStaticCredentialsAreRejected()
    {
        long expireAt = System.currentTimeMillis() + 10000;
        // readCredentials has only accessKeyId — secretAccessKey and sessionToken absent
        String json = "{\"secrets\":" +
                      "{\"readCredentials\":{\"accessKeyId\":\"key\",\"region\":\"us-east-1\"}," +
                      "\"writeCredentials\":{\"accessKeyId\":\"key\",\"region\":\"us-east-1\"}}," +
                      "\"expireAt\":" + expireAt + "}";
        assertThatThrownBy(() -> MAPPER.readValue(json, CreateRestoreJobRequestPayload.class))
        .isInstanceOf(ValueInstantiationException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Static credentials must have all key fields present");
    }

    @Test
    void testExplicitStaticCredentialTypeWithPartialCredentialsIsRejected() throws JsonProcessingException
    {
        long expireAt = System.currentTimeMillis() + 10000;
        // credentialType is explicitly STATIC but credentials are partial — should still be rejected
        String json = "{\"credentialType\":\"STATIC\"," +
                      "\"secrets\":{\"readCredentials\":{\"accessKeyId\":\"key\",\"region\":\"us-east-1\"}," +
                      "\"writeCredentials\":{\"accessKeyId\":\"key\",\"region\":\"us-east-1\"}}," +
                      "\"expireAt\":" + expireAt + "}";
        assertThatThrownBy(() -> MAPPER.readValue(json, CreateRestoreJobRequestPayload.class))
        .isInstanceOf(ValueInstantiationException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Static credentials must have all key fields present");
    }

    @Test
    void testPartialWriteCredentialsAreRejected() throws JsonProcessingException
    {
        long expireAt = System.currentTimeMillis() + 10000;
        // readCredentials is fully populated, writeCredentials is partial — both must be validated
        StorageCredentials fullRead = RestoreJobSecretsGen.genReadStorageCredentials();
        String json = "{\"secrets\":" +
                      "{\"readCredentials\":" + MAPPER.writeValueAsString(fullRead) + "," +
                      "\"writeCredentials\":{\"accessKeyId\":\"key\",\"region\":\"us-east-1\"}}," +
                      "\"expireAt\":" + expireAt + "}";
        assertThatThrownBy(() -> MAPPER.readValue(json, CreateRestoreJobRequestPayload.class))
        .isInstanceOf(ValueInstantiationException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Static credentials must have all key fields present for writeCredentials");
    }
}
