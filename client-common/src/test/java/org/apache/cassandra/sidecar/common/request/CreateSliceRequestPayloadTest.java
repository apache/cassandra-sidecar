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

import java.math.BigInteger;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import org.apache.cassandra.sidecar.common.request.data.CreateSliceRequestPayload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CreateSliceRequestPayloadTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testReadFromJsonString() throws JsonProcessingException
    {
        int bucketId = 0;
        String json = "{\"sliceId\":\"TASK_ID-RETRY_COUNT-SEQUENCE\"," +
                      "\"bucketId\": " + bucketId + "," +
                      "\"startToken\":\"1\"," +
                      "\"endToken\":\"10\"," +
                      "\"storageBucket\":\"myBucket\"," +
                      "\"storageKey\":\"/path/to/object\"," +
                      "\"sliceChecksum\":\"12321abc\"}";
        CreateSliceRequestPayload req = MAPPER.readValue(json, CreateSliceRequestPayload.class);
        assertThat(req).isNotNull();
        assertThat(req.sliceId()).isEqualTo("TASK_ID-RETRY_COUNT-SEQUENCE");
        assertThat(req.bucketId()).isEqualTo(bucketId);
        assertThat(req.checksum()).isEqualTo("12321abc");
        assertThat(req.bucket()).isEqualTo("myBucket");
        assertThat(req.key()).isEqualTo("/path/to/object");
        assertThat(req.startToken()).isEqualTo(BigInteger.ONE);
        assertThat(req.endToken()).isEqualTo(BigInteger.TEN);
        assertThat(req.compressedSize())
        .describedAs("size is not present in the json")
        .isNull();
        assertThat(req.uncompressedSize())
        .describedAs("uncompressed size is not present in the json")
        .isNull();
    }

    @Test
    void testReadFromJsonFailsWithMissingFields()
    {
        String json = "{\"sliceId\":\"TASK_ID-RETRY_COUNT-SEQUENCE\"}";
        assertThatThrownBy(() -> MAPPER.readValue(json, CreateSliceRequestPayload.class))
        .isInstanceOf(ValueInstantiationException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid create slice request payload");
    }

    @Test
    void testSerDeser() throws JsonProcessingException
    {
        int bucketId = 2;
        CreateSliceRequestPayload req = new CreateSliceRequestPayload("id", bucketId, "bucket", "key", "checksum",
                                                                      BigInteger.ONE, BigInteger.TEN, 234L, 123L);
        String json = MAPPER.writeValueAsString(req);
        CreateSliceRequestPayload test = MAPPER.readValue(json, CreateSliceRequestPayload.class);
        assertThat(test.sliceId()).isEqualTo("id");
        assertThat(test.bucketId()).isEqualTo(bucketId);
        assertThat(test.bucket()).isEqualTo("bucket");
        assertThat(test.key()).isEqualTo("key");
        assertThat(test.checksum()).isEqualTo("checksum");
        assertThat(test.startToken()).isEqualTo(BigInteger.ONE);
        assertThat(test.endToken()).isEqualTo(BigInteger.TEN);
        assertThat(test.uncompressedSize()).isEqualTo(234L);
        assertThat(test.compressedSize()).isEqualTo(123L);
    }

    @Test
    void testSerDeserWithoutOptionalFields() throws JsonProcessingException
    {
        CreateSliceRequestPayload req = new CreateSliceRequestPayload("id", 1, "bucket", "key", "checksum",
                                                                      BigInteger.ONE, BigInteger.TEN, null, null);
        String json = MAPPER.writeValueAsString(req);
        assertThat(json).doesNotContain("uncompressedSize")
                        .doesNotContain("compressedSize");
        CreateSliceRequestPayload test = MAPPER.readValue(json, CreateSliceRequestPayload.class);
        assertThat(test.compressedSize())
        .describedAs("size was not set in the original object")
        .isNull();
        assertThat(test.uncompressedSize())
        .describedAs("uncompressed size was not set in the original object")
        .isNull();
    }
}
