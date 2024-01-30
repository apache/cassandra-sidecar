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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.RestoreSliceStatus;
import org.apache.cassandra.sidecar.db.RestoreSlice;

import static org.assertj.core.api.Assertions.assertThat;

class RestoreSliceTest
{
    @Test
    void testEquals()
    {
        Path path = Paths.get(".");
        RestoreSlice slice1
        = RestoreSlice.builder()
                      .jobId(UUIDs.timeBased()).keyspace("keyspace").table("table")
                      .sliceId("sliceId-123").bucketId((short) 1)
                      .storageBucket("myBucket").storageKey("myKey").checksum("checksum")
                      .startToken(BigInteger.ONE).endToken(BigInteger.valueOf(2))
                      .replicaStatus(Collections.singletonMap("replica1", RestoreSliceStatus.COMMITTING))
                      .replicas(Collections.singleton("replica1"))
                      .stageDirectory(path, "uploadId")
                      .build();
        RestoreSlice slice2 = slice1.unbuild().build();
        assertThat(slice1).isEqualTo(slice2);

        RestoreSlice slice3 = slice1.unbuild()
                                    .jobId(UUIDs.timeBased())
                                    .bucketId((short) 2)
                                    .startToken(BigInteger.valueOf(2)).endToken(BigInteger.TEN)
                                    .build();
        assertThat(slice3).isNotEqualTo(slice1)
                          .isNotEqualTo(slice2);
    }


}
