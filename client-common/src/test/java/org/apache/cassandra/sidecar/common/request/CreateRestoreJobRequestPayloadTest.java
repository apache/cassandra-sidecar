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
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.SSTableImportOptions;
import org.apache.cassandra.sidecar.common.request.data.CreateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.foundation.RestoreJobSecretsGen;

import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_CONSISTENCY_LEVEL;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_LOCAL_DATA_CENTER;
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
                                                                           .consistencyLevel(ConsistencyLevel.QUORUM)
                                                                           .jobAgent("agent")
                                                                           .build();
        String json = MAPPER.writeValueAsString(req);
        assertThat(json).describedAs("Null value fields should be excluded").doesNotContain(JOB_LOCAL_DATA_CENTER)
                        .describedAs("Non-null value fields should be included").contains(JOB_CONSISTENCY_LEVEL)
                        .isEqualTo("{\"jobId\":\"e870e5dc-d25e-11ed-afa1-0242ac120002\"," +
                                   "\"jobAgent\":\"agent\"," +
                                   "\"secrets\":" + MAPPER.writeValueAsString(secrets) + "," +
                                   "\"importOptions\":{" +
                                   "\"verifyTokens\":\"true\"," +
                                   "\"resetLevel\":\"true\"," +
                                   "\"clearRepaired\":\"true\"," +
                                   "\"extendedVerify\":\"true\"," +
                                   "\"verifySSTables\":\"true\"," +
                                   "\"invalidateCaches\":\"true\"," +
                                   "\"copyData\":\"false\"}," +
                                   "\"expireAt\":" + expireAt + "," +
                                   "\"consistencyLevel\":\"QUORUM\"}");
        CreateRestoreJobRequestPayload test = MAPPER.readValue(json, CreateRestoreJobRequestPayload.class);
        assertThat(test.jobId()).hasToString(id);
        assertThat(test.jobAgent()).isEqualTo("agent");
        assertThat(test.secrets()).isEqualTo(secrets);
        assertThat(test.expireAtInMillis()).isEqualTo(expireAt);
        assertThat(test.expireAtAsDate()).isEqualTo(date);
        assertThat(test.importOptions()).isEqualTo(SSTableImportOptions.defaults());
        assertThat(test.consistencyLevel()).isEqualTo("QUORUM");
        assertThat(test.localDatacenter()).isNull();
    }

    @Test
    void testReadFromJsonFailsWithUnknownFields() throws JsonProcessingException
    {
        String uuid = "e870e5dc-d25e-11ed-afa1-0242ac120002";
        String json = "{\"jobId\":\"" + uuid + "\"," +
                      "\"jobAgent\":\"Spark Bulk Analytics\"," +
                      "\"status\":\"Completed\"," +
                      "\"expireAt\":" + System.currentTimeMillis() + 1000 +
                      ",\"secrets\":" + MAPPER.writeValueAsString(RestoreJobSecretsGen.genRestoreJobSecrets()) + "}";
        assertThatThrownBy(() -> MAPPER.readValue(json, CreateRestoreJobRequestPayload.class))
        .isInstanceOf(UnrecognizedPropertyException.class)
        .hasMessageContaining("Unrecognized field \"status\"");
    }

    @Test
    void testReadFromJsonWithInvalidSecrets()
    {
        String json = "{\"secrets\":" +
                      "{\"readCredentials\":{\"accessKeyId\":\"accessKeyId\"}," +
                      "\"writeCredentials\":{\"accessKeyId\":\"accessKeyId\"}}}";
        assertThatThrownBy(() -> MAPPER.readValue(json, CreateRestoreJobRequestPayload.class))
        .isInstanceOf(ValueInstantiationException.class)
        .hasMessageContaining("Cannot construct instance");
    }

    @Test
    void testReadFromJsonWithPartialFields() throws JsonProcessingException
    {
        RestoreJobSecrets secrets = RestoreJobSecretsGen.genRestoreJobSecrets();
        String json = "{\"secrets\":" + MAPPER.writeValueAsString(secrets) +
                      ", \"expireAt\":" + System.currentTimeMillis() + 1000 + "}";
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
        String json = "{\"expireAt\":" + System.currentTimeMillis() + 1000 + "}";
        assertThatThrownBy(() -> MAPPER.readValue(json, CreateRestoreJobRequestPayload.class))
        .isInstanceOf(ValueInstantiationException.class)
        .hasCauseInstanceOf(NullPointerException.class)
        .hasMessageContaining("secrets cannot be null");
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
                      "\"expireAt\":\"" + System.currentTimeMillis() + 1000 + "\"," +
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
            assertThatThrownBy(() -> CreateRestoreJobRequestPayload.builder(secrets, System.currentTimeMillis())
                                                                   .consistencyLevel(localCL)
                                                                   .build())
            .hasMessage("Must specify a non-empty localDatacenter for consistency level: " + localCL.name());

            assertThatThrownBy(() -> CreateRestoreJobRequestPayload.builder(secrets, System.currentTimeMillis())
                                                                   .consistencyLevel(localCL, "")
                                                                   .build())
            .hasMessage("Must specify a non-empty localDatacenter for consistency level: " + localCL.name());
        }

    }
}
