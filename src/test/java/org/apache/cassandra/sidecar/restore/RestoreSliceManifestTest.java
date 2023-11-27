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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class RestoreSliceManifestTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testSerDeser() throws JsonProcessingException
    {
        RestoreSliceManifest origin = new RestoreSliceManifest();
        origin.put("sstable1", new RestoreSliceManifest.ManifestEntry(Collections.singletonMap("file1", "checksum1"),
                                                                      BigInteger.ONE, BigInteger.valueOf(2)));
        origin.put("sstable2", new RestoreSliceManifest.ManifestEntry(Collections.singletonMap("file2", "checksum1"),
                                                                      BigInteger.ONE, BigInteger.valueOf(2)));
        String json = MAPPER.writeValueAsString(origin);
        RestoreSliceManifest manifestRead = MAPPER.readValue(json, RestoreSliceManifest.class);

        assertThat(manifestRead.mergeAllChecksums()).isEqualTo(origin.mergeAllChecksums());
    }

    @Test
    void testMergeAllChecksums()
    {
        RestoreSliceManifest manifest = new RestoreSliceManifest();
        for (int i = 0; i < 10; i++)
        {
            manifest.put("sstable" + i,
                         new RestoreSliceManifest.ManifestEntry(Collections.singletonMap("file" + i, "checksum" + i),
                                                                BigInteger.valueOf(i), BigInteger.valueOf(i + 1)));
        }

        Map<String, String> merged = manifest.mergeAllChecksums();
        assertThat(merged).hasSize(10);
        Map<String, String> expected = new HashMap<>(10);
        for (int i = 0; i < 10; i++)
        {
            expected.put("file" + i, "checksum" + i);
        }
        assertThat(merged).isEqualTo(expected);
    }
}
