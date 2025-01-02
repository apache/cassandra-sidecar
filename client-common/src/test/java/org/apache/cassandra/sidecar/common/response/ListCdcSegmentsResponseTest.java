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
import org.apache.cassandra.sidecar.common.response.data.CdcSegmentInfo;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test {@link ListCdcSegmentsResponse}
 */
class ListCdcSegmentsResponseTest
{
    @Test
    void testSerDeser() throws Exception
    {
        List<CdcSegmentInfo> segments = Arrays.asList(new CdcSegmentInfo("commit-log1", 100, 100, true, 1732148713725L),
                                                      new CdcSegmentInfo("commit-log2", 100, 10, false, 1732148713725L));
        ListCdcSegmentsResponse response = new ListCdcSegmentsResponse("localhost", 9043, segments);
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(response);
        assertThat(json).isEqualTo("{\"host\":\"localhost\"," +
                                   "\"port\":9043," +
                                   "\"segmentsInfo\":[" +
                                   "{\"name\":\"commit-log1\",\"size\":100,\"idx\":100,\"completed\":true,\"lastModifiedTimestamp\":1732148713725}," +
                                   "{\"name\":\"commit-log2\",\"size\":100,\"idx\":10,\"completed\":false,\"lastModifiedTimestamp\":1732148713725}]}");
        ListCdcSegmentsResponse deserialized = mapper.readValue(json, ListCdcSegmentsResponse.class);
        assertThat(deserialized.host()).isEqualTo("localhost");
        assertThat(deserialized.port()).isEqualTo(9043);
        assertThat(deserialized.segmentsInfo()).hasSize(2);
        assertThat(deserialized.segmentsInfo().get(0).name).isEqualTo("commit-log1");
        assertThat(deserialized.segmentsInfo().get(1).name).isEqualTo("commit-log2");
    }

    @Test
    void testHandleNoSegmentsInfoInDeserialization() throws Exception
    {
        String json = "{\"host\":\"localhost\",\"port\":9043}";
        ObjectMapper mapper = new ObjectMapper();
        ListCdcSegmentsResponse deserialized =  mapper.readValue(json, ListCdcSegmentsResponse.class);
        assertThat(deserialized.host()).isEqualTo("localhost");
        assertThat(deserialized.port()).isEqualTo(9043);
        assertThat(deserialized.segmentsInfo()).isEmpty();
    }
}
