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

package org.apache.cassandra.sidecar.db;

import java.math.BigInteger;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.request.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RestoreSliceTest
{
    @Test
    void testEquals()
    {
        UUID jobId = UUIDs.timeBased();
        RestoreSlice slice1 = createTestingSlice(jobId, "slice-id", 0L, 10L);
        RestoreSlice slice2 = createTestingSlice(jobId, "slice-id", 0L, 10L);
        assertThat(slice1).isEqualTo(slice2);
        RestoreSlice slice3 = slice1.unbuild().build();
        assertThat(slice1).isEqualTo(slice3);
    }

    @Test
    void testNotEquals()
    {
        RestoreSlice slice1 = createTestingSlice(UUIDs.timeBased(), "slice-id", 0L, 10L);
        RestoreSlice slice2 = slice1.unbuild().endToken(BigInteger.valueOf(20L)).build();
        assertThat(slice1).isNotEqualTo(slice2);

        RestoreSlice slice3 = slice1.unbuild().compressedSize(123).build();
        assertThat(slice1).isNotEqualTo(slice3);
        assertThat(slice2).isNotEqualTo(slice3);
    }

    @Test
    void testCreateFromRow()
    {
        RestoreJob restoreJob = RestoreJobTest.createNewTestingJob(UUIDs.timeBased());
        RestoreSlice slice = createTestingSlice(restoreJob, "slice-id", 0L, 10L);
        Row mockRow = mock(Row.class);

        when(mockRow.getUUID("job_id")).thenReturn(slice.jobId());
        when(mockRow.getString("slice_id")).thenReturn(slice.sliceId());
        when(mockRow.getShort("bucket_id")).thenReturn(slice.bucketId());
        when(mockRow.getString("bucket")).thenReturn(slice.bucket());
        when(mockRow.getString("key")).thenReturn(slice.key());
        when(mockRow.getString("checksum")).thenReturn(slice.checksum());
        when(mockRow.getVarint("start_token")).thenReturn(slice.startToken());
        when(mockRow.getVarint("end_token")).thenReturn(slice.endToken());
        when(mockRow.getLong("compressed_size")).thenReturn(slice.compressedSize());
        when(mockRow.getLong("uncompressed_size")).thenReturn(slice.uncompressedSize());

        RestoreSlice sliceFromRow = RestoreSlice.from(mockRow, restoreJob);
        assertThat(sliceFromRow).isEqualTo(slice);
    }

    @Test
    void testCreateFromPayload()
    {
        CreateSliceRequestPayload payload = new CreateSliceRequestPayload("slice-id", 0, "bucket", "key", "checksum",
                                                                          BigInteger.ONE, // first token
                                                                          BigInteger.TEN, // end token
                                                                          123L, // uncompressed size
                                                                          100L); // compressed size
        RestoreSlice slice = RestoreSlice.builder().createSliceRequestPayload(payload).build();
        assertThat(slice.sliceId()).isEqualTo("slice-id");
        assertThat(slice.bucketId()).isEqualTo((short) 0);
        assertThat(slice.bucket()).isEqualTo("bucket");
        assertThat(slice.key()).isEqualTo("key");
        assertThat(slice.checksum()).isEqualTo("checksum");
        assertThat(slice.startToken())
        .describedAs("Start token is exclusive end in the range")
        .isEqualTo(BigInteger.ZERO);
        assertThat(slice.endToken()).isEqualTo(BigInteger.TEN);
        assertThat(slice.uncompressedSize()).isEqualTo(123L);
        assertThat(slice.compressedSize()).isEqualTo(100L);
    }

    @Test
    void testNoSplit()
    {
        RestoreSlice slice = createTestingSlice(UUIDs.timeBased(), "slice-id", 0L, 10L);
        RestoreSlice result = slice.trimMaybe(new TokenRange(-10L, 10L));
        assertThat(result)
        .describedAs("No trim is done when fully enclosed by the local token range")
        .isSameAs(slice);
    }

    @Test
    void testSplitNoIntersection()
    {
        RestoreSlice slice = createTestingSlice(UUIDs.timeBased(), "slice-id", 0L, 10L);
        // (0, 10] does not intersect with (100, 110]
        assertThatThrownBy(() -> slice.trimMaybe(new TokenRange(100L, 110L)))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("Token range of the slice does not intersect with the local token range. " +
                    "slice_range: TokenRange(0, 10], local_range: TokenRange(100, 110]");
    }

    public static RestoreSlice createTestingSlice(RestoreJob restoreJob, String sliceId, long startToken, long endToken)
    {
        return createTestingSlice(restoreJob.jobId, sliceId, startToken, endToken)
               .unbuild()
               .keyspace(restoreJob.keyspaceName)
               .table(restoreJob.tableName)
               .build();
    }

    public static RestoreSlice createTestingSlice(UUID jobId, String sliceId, long startToken, long endToken)
    {
        return RestoreSlice.builder()
                           .jobId(jobId)
                           .keyspace("keyspace")
                           .table("table")
                           .sliceId(sliceId).bucketId((short) 0)
                           .storageBucket("myBucket")
                           .storageKey("myKey")
                           .checksum("checksum")
                           .startToken(BigInteger.valueOf(startToken))
                           .endToken(BigInteger.valueOf(endToken))
                           .build();
    }
}
