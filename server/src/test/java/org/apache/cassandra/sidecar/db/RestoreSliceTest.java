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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse.ReplicaInfo;

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
    void testNoSplit()
    {
        RestoreSlice slice = createTestingSlice(UUIDs.timeBased(), "slice-id", 0L, 10L);
        List<RestoreSlice> result = slice.splitMaybe(null);
        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isSameAs(slice);

        TokenRangeReplicasResponse topology = mock(TokenRangeReplicasResponse.class);
        when(topology.writeReplicas()).thenReturn(Collections.singletonList(new ReplicaInfo("-10", "10", null)));
        // range in topology fully encloses the slice
        result = slice.splitMaybe(topology);
        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isSameAs(slice);
    }

    @Test
    void testSplitNoOverlap()
    {
        RestoreSlice slice = createTestingSlice(UUIDs.timeBased(), "slice-id", 0L, 10L);
        TokenRangeReplicasResponse topology = mock(TokenRangeReplicasResponse.class);
        when(topology.writeReplicas()).thenReturn(Collections.singletonList(new ReplicaInfo("100", "110", null)));
        // (0, 10] does not overlap with (100, 110]
        assertThatThrownBy(() -> slice.splitMaybe(topology))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("Token range of the slice is not found in the write replicas. slice range: (0, 10]");
    }

    @Test
    void testSplitIntoMultiple()
    {
        RestoreSlice slice = createTestingSlice(UUIDs.timeBased(), "slice-id", 2L, 25L);
        TokenRangeReplicasResponse topology = mock(TokenRangeReplicasResponse.class);
        ReplicaInfo r1 = new ReplicaInfo("-10", "10", null);
        ReplicaInfo r2 = new ReplicaInfo("10", "20", null);
        ReplicaInfo r3 = new ReplicaInfo("20", "30", null);
        ReplicaInfo r4 = new ReplicaInfo("30", "40", null);
        when(topology.writeReplicas()).thenReturn(Arrays.asList(r1, r2, r3, r4));
        List<RestoreSlice> result = slice.splitMaybe(topology);
        assertThat(result).hasSize(3);
        assertThat(result.get(0).startToken()).isEqualTo(BigInteger.valueOf(2L));
        assertThat(result.get(0).endToken()).isEqualTo(BigInteger.valueOf(10L));
        assertThat(result.get(1).startToken()).isEqualTo(BigInteger.valueOf(10L));
        assertThat(result.get(1).endToken()).isEqualTo(BigInteger.valueOf(20L));
        assertThat(result.get(2).startToken()).isEqualTo(BigInteger.valueOf(20L));
        assertThat(result.get(2).endToken()).isEqualTo(BigInteger.valueOf(25L));
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
