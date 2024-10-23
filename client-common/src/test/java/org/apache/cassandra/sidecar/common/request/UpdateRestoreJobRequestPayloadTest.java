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
import java.util.Date;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.sidecar.common.data.RestoreJobConstants;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.request.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.foundation.RestoreJobSecretsGen;

import static org.assertj.core.api.Assertions.assertThat;

class UpdateRestoreJobRequestPayloadTest
{
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testEmptySecrets() throws JsonProcessingException
    {
        String json = "{\"jobAgent\":\"Spark Bulk Analytics\"}";
        UpdateRestoreJobRequestPayload req = mapper.readValue(json, UpdateRestoreJobRequestPayload.class);

        assertThat(req).isNotNull();
        assertThat(req.jobAgent()).isEqualTo("Spark Bulk Analytics");
        assertThat(req.status()).isNull();
        assertThat(req.expireAtInMillis()).isNull();
        assertThat(req.expireAtAsDate()).isNull();
        assertThat(req.sliceCount()).isNull();
        assertThat(req.secrets()).isNull();
    }

    @Test
    void testExpireAt() throws JsonProcessingException
    {
        long time = System.currentTimeMillis() + 1000;
        Date date = Date.from(Instant.ofEpochMilli(time));
        String json = "{\"expireAt\":" + time + "}";
        UpdateRestoreJobRequestPayload req = mapper.readValue(json, UpdateRestoreJobRequestPayload.class);

        assertThat(req.expireAtAsDate()).isEqualTo(date);
        assertThat(req.expireAtInMillis()).isEqualTo(time);

        assertThat(mapper.writeValueAsString(req)).isEqualTo(json);
    }

    @Test
    void testSliceCount() throws JsonProcessingException
    {
        long sliceCount = 123L;
        String json = "{\"sliceCount\":" + sliceCount + "}";
        UpdateRestoreJobRequestPayload req = mapper.readValue(json, UpdateRestoreJobRequestPayload.class);
        assertThat(req.sliceCount()).isEqualTo(sliceCount);

        assertThat(mapper.writeValueAsString(req)).isEqualTo(json);
    }

    @Test
    void testJsonSerializationShouldIgnoreUnwantedFields() throws JsonProcessingException
    {
        RestoreJobSecrets secrets = RestoreJobSecretsGen.genRestoreJobSecrets();
        UpdateRestoreJobRequestPayload payload = new UpdateRestoreJobRequestPayload(null, secrets
                                                                                    , null, null, null);
        String json = mapper.writeValueAsString(payload);
        assertThat(json).contains(RestoreJobConstants.JOB_SECRETS)
                        .doesNotContain(RestoreJobConstants.JOB_AGENT,
                                        RestoreJobConstants.JOB_EXPIRE_AT,
                                        RestoreJobConstants.JOB_STATUS,
                                        RestoreJobConstants.JOB_SLICE_COUNT)
                        .doesNotContain("empty"); // ignored by @JsonIgnore
    }
}
