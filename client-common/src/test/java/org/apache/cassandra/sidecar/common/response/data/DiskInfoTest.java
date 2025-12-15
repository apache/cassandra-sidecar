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

package org.apache.cassandra.sidecar.common.response.data;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link DiskInfo} JSON serialization and deserialization.
 */
class DiskInfoTest
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testSerializationDeserializationRoundTrip() throws Exception
    {
        DiskInfo original = new DiskInfo(1000000000L, 500000000L, 450000000L,
                                         "data", "/dev/sda1", "ext4");

        String json = objectMapper.writeValueAsString(original);
        DiskInfo deserialized = objectMapper.readValue(json, DiskInfo.class);

        assertThat(deserialized.totalSpace()).isEqualTo(original.totalSpace());
        assertThat(deserialized.freeSpace()).isEqualTo(original.freeSpace());
        assertThat(deserialized.usableSpace()).isEqualTo(original.usableSpace());
        assertThat(deserialized.name()).isEqualTo(original.name());
        assertThat(deserialized.mount()).isEqualTo(original.mount());
        assertThat(deserialized.type()).isEqualTo(original.type());
    }

    @Test
    void testJsonStructure() throws Exception
    {
        DiskInfo response = new DiskInfo(1000000000L, 500000000L, 450000000L,
                                         "data", "/dev/sda1", "ext4");

        String json = objectMapper.writeValueAsString(response);

        assertThat(json).contains("\"totalSpace\":1000000000");
        assertThat(json).contains("\"freeSpace\":500000000");
        assertThat(json).contains("\"usableSpace\":450000000");
        assertThat(json).contains("\"name\":\"data\"");
        assertThat(json).contains("\"mount\":\"/dev/sda1\"");
        assertThat(json).contains("\"type\":\"ext4\"");
    }

    @Test
    void testMultipleDisksSerialization() throws Exception
    {
        DiskInfo disk1 = new DiskInfo(1000000000L, 500000000L, 450000000L,
                                      "data1", "/dev/sda1", "ext4");
        DiskInfo disk2 = new DiskInfo(2000000000L, 1000000000L, 950000000L,
                                      "data2", "/dev/sdb1", "xfs");

        String json1 = objectMapper.writeValueAsString(disk1);
        String json2 = objectMapper.writeValueAsString(disk2);

        DiskInfo deserialized1 = objectMapper.readValue(json1, DiskInfo.class);
        DiskInfo deserialized2 = objectMapper.readValue(json2, DiskInfo.class);

        assertThat(deserialized1.name()).isEqualTo("data1");
        assertThat(deserialized1.mount()).isEqualTo("/dev/sda1");
        assertThat(deserialized1.type()).isEqualTo("ext4");

        assertThat(deserialized2.name()).isEqualTo("data2");
        assertThat(deserialized2.mount()).isEqualTo("/dev/sdb1");
        assertThat(deserialized2.type()).isEqualTo("xfs");
    }
}
