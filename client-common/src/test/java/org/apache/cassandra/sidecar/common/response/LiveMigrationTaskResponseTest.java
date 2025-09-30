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

package org.apache.cassandra.sidecar.common.response;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.sidecar.common.response.LiveMigrationTaskResponse.Status;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link LiveMigrationTaskResponse} JSON serialization and deserialization.
 */
class LiveMigrationTaskResponseTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testSerializationDeserializationRoundTrip() throws Exception
    {
        List<Status> statusList = Arrays.asList(
        new Status(1, "DOWNLOAD_COMPLETE", 1000000L, 100, 500000L, 50, 25, 2, 250000L),
        new Status(2, "DOWNLOAD_COMPLETE", 1000000L, 100, 300000L, 30, 28, 1, 280000L),
        new Status(3, "SUCCESS", 1000000L, 100, 200000L, 20, 20, 0, 200000L)
        );

        LiveMigrationTaskResponse original = new LiveMigrationTaskResponse(
        "task-123",
        "192.168.1.100",
        9043,
        10,
        0.95,
        5,
        statusList
        );

        String json = objectMapper.writeValueAsString(original);
        LiveMigrationTaskResponse deserialized = objectMapper.readValue(json, LiveMigrationTaskResponse.class);

        assertThat(deserialized.taskId()).isEqualTo(original.taskId());
        assertThat(deserialized.source()).isEqualTo(original.source());
        assertThat(deserialized.port()).isEqualTo(original.port());
        assertThat(deserialized.maxIterations()).isEqualTo(original.maxIterations());
        assertThat(deserialized.successThreshold()).isEqualTo(original.successThreshold());
        assertThat(deserialized.maxConcurrency()).isEqualTo(original.maxConcurrency());
        assertThat(deserialized.status()).hasSize(original.status().size());

        for (int i = 0; i < statusList.size(); i++)
        {
            Status originalStatus = original.status().get(i);
            Status deserializedStatus = deserialized.status().get(i);

            assertThat(deserializedStatus.iteration()).isEqualTo(originalStatus.iteration());
            assertThat(deserializedStatus.state()).isEqualTo(originalStatus.state());
            assertThat(deserializedStatus.totalSize()).isEqualTo(originalStatus.totalSize());
            assertThat(deserializedStatus.totalFiles()).isEqualTo(originalStatus.totalFiles());
            assertThat(deserializedStatus.bytesToDownload()).isEqualTo(originalStatus.bytesToDownload());
            assertThat(deserializedStatus.filesToDownload()).isEqualTo(originalStatus.filesToDownload());
            assertThat(deserializedStatus.filesDownloaded()).isEqualTo(originalStatus.filesDownloaded());
            assertThat(deserializedStatus.downloadFailures()).isEqualTo(originalStatus.downloadFailures());
            assertThat(deserializedStatus.bytesDownloaded()).isEqualTo(originalStatus.bytesDownloaded());
        }
    }
}
