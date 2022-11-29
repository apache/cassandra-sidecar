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

package org.apache.cassandra.sidecar.client.request;

import org.apache.cassandra.sidecar.client.RequestContext;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.data.ListSnapshotFilesResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for client requests accessing the list snapshot endpoint
 */
public class ListSnapshotFilesRequestTestParameters implements RequestTestParameters<ListSnapshotFilesResponse>
{
    @Override
    public RequestContext.Builder specificRequest(RequestContext.Builder requestContextBuilder)
    {
        return requestContextBuilder.listSnapshotFilesRequest("keyspace1", "standard1", "2023.04.10");
    }

    @Override
    public String okResponseBody()
    {
        return "{\"snapshotFilesInfo\":[{\"size\":80,\"host\":\"localhost2\",\"port\":9043,\"dataDirIndex\":0," +
               "\"snapshotName\":\"2023.04.10\",\"keySpaceName\":\"keyspace1\",\"tableName\":" +
               "\"standard1-bc1a8c20d7f111eda9b8056729c856e9\",\"fileName\":\"nb-1-big-TOC.txt\"}," +
               "{\"size\":10,\"host\":\"localhost2\",\"port\":9043," +
               "\"dataDirIndex\":0,\"snapshotName\":\"2023.04.10\"," +
               "\"keySpaceName\":\"keyspace1\",\"tableName\":\"standard1-bc1a8c20d7f111eda9b8056729c856e9\"," +
               "\"fileName\":\"nb-1-big-Digest.crc32\"},{\"size\":424,\"host\":\"localhost2\",\"port\":9043," +
               "\"dataDirIndex\":0,\"snapshotName\":\"2023.04.10\",\"keySpaceName\":\"keyspace1\"," +
               "\"tableName\":\"standard1-bc1a8c20d7f111eda9b8056729c856e9\",\"fileName\":\"nb-1-big-CRC.db\"}," +
               "{\"size\":5222,\"host\":\"localhost2\",\"port\":9043,\"dataDirIndex\":0," +
               "\"snapshotName\":\"2023.04.10\",\"keySpaceName\":\"keyspace1\"," +
               "\"tableName\":\"standard1-bc1a8c20d7f111eda9b8056729c856e9\",\"fileName\":\"nb-1-big-Summary.db\"}," +
               "{\"size\":500769,\"host\":\"localhost2\",\"port\":9043,\"dataDirIndex\":0," +
               "\"snapshotName\":\"2023.04.10\",\"keySpaceName\":\"keyspace1\"," +
               "\"tableName\":\"standard1-bc1a8c20d7f111eda9b8056729c856e9\",\"fileName\":\"nb-1-big-Index.db\"}," +
               "{\"size\":31,\"host\":\"localhost2\",\"port\":9043,\"dataDirIndex\":0," +
               "\"snapshotName\":\"2023.04.10\",\"keySpaceName\":\"keyspace1\"," +
               "\"tableName\":\"standard1-bc1a8c20d7f111eda9b8056729c856e9\",\"fileName\":\"manifest.json\"}," +
               "{\"size\":6870000,\"host\":\"localhost2\",\"port\":9043,\"dataDirIndex\":0," +
               "\"snapshotName\":\"2023.04.10\",\"keySpaceName\":\"keyspace1\"," +
               "\"tableName\":\"standard1-bc1a8c20d7f111eda9b8056729c856e9\",\"fileName\":\"nb-1-big-Data.db\"}," +
               "{\"size\":864,\"host\":\"localhost2\",\"port\":9043,\"dataDirIndex\":0," +
               "\"snapshotName\":\"2023.04.10\",\"keySpaceName\":\"keyspace1\"," +
               "\"tableName\":\"standard1-bc1a8c20d7f111eda9b8056729c856e9\",\"fileName\":\"schema.cql\"}," +
               "{\"size\":37512,\"host\":\"localhost2\",\"port\":9043,\"dataDirIndex\":0," +
               "\"snapshotName\":\"2023.04.10\",\"keySpaceName\":\"keyspace1\"," +
               "\"tableName\":\"standard1-bc1a8c20d7f111eda9b8056729c856e9\",\"fileName\":\"nb-1-big-Filter.db\"}," +
               "{\"size\":10353,\"host\":\"localhost2\",\"port\":9043,\"dataDirIndex\":0," +
               "\"snapshotName\":\"2023.04.10\",\"keySpaceName\":\"keyspace1\"," +
               "\"tableName\":\"standard1-bc1a8c20d7f111eda9b8056729c856e9\"," +
               "\"fileName\":\"nb-1-big-Statistics.db\"}]}";
    }

    @Override
    public String expectedEndpointPath()
    {
        return ApiEndpointsV1.SNAPSHOTS_ROUTE.replaceAll(ApiEndpointsV1.KEYSPACE_PATH_PARAM, "keyspace1")
                                             .replaceAll(ApiEndpointsV1.TABLE_PATH_PARAM, "standard1")
                                             .replaceAll(ApiEndpointsV1.SNAPSHOT_PATH_PARAM, "2023.04.10");
    }

    @Override
    public void validateResponse(ListSnapshotFilesResponse response)
    {
        assertThat(response).isNotNull();
        assertThat(response.getSnapshotFilesInfo()).hasSize(10);
        assertThat(response.getSnapshotFilesInfo().get(0).fileName).isEqualTo("nb-1-big-TOC.txt");
        assertThat(response.getSnapshotFilesInfo().get(0).size).isEqualTo(80);
        assertThat(response.getSnapshotFilesInfo().get(1).fileName).isEqualTo("nb-1-big-Digest.crc32");
        assertThat(response.getSnapshotFilesInfo().get(1).size).isEqualTo(10);
        assertThat(response.getSnapshotFilesInfo().get(2).fileName).isEqualTo("nb-1-big-CRC.db");
        assertThat(response.getSnapshotFilesInfo().get(2).size).isEqualTo(424);
        assertThat(response.getSnapshotFilesInfo().get(3).fileName).isEqualTo("nb-1-big-Summary.db");
        assertThat(response.getSnapshotFilesInfo().get(3).size).isEqualTo(5222);
        assertThat(response.getSnapshotFilesInfo().get(4).fileName).isEqualTo("nb-1-big-Index.db");
        assertThat(response.getSnapshotFilesInfo().get(4).size).isEqualTo(500769);
        assertThat(response.getSnapshotFilesInfo().get(5).fileName).isEqualTo("manifest.json");
        assertThat(response.getSnapshotFilesInfo().get(5).size).isEqualTo(31);
        assertThat(response.getSnapshotFilesInfo().get(6).fileName).isEqualTo("nb-1-big-Data.db");
        assertThat(response.getSnapshotFilesInfo().get(6).size).isEqualTo(6870000);
        assertThat(response.getSnapshotFilesInfo().get(7).fileName).isEqualTo("schema.cql");
        assertThat(response.getSnapshotFilesInfo().get(7).size).isEqualTo(864);
        assertThat(response.getSnapshotFilesInfo().get(8).fileName).isEqualTo("nb-1-big-Filter.db");
        assertThat(response.getSnapshotFilesInfo().get(8).size).isEqualTo(37512);
        assertThat(response.getSnapshotFilesInfo().get(9).fileName).isEqualTo("nb-1-big-Statistics.db");
        assertThat(response.getSnapshotFilesInfo().get(9).size).isEqualTo(10353);
    }
}
