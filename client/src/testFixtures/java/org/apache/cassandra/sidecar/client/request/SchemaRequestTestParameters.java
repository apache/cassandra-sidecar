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
import org.apache.cassandra.sidecar.common.response.SchemaResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for client requests accessing the schema endpoint for a keyspace
 */
public class SchemaRequestTestParameters implements RequestTestParameters<SchemaResponse>
{
    public static final String KEYSPACE = "cycling";

    @Override
    public RequestContext.Builder specificRequest(RequestContext.Builder requestContextBuilder)
    {
        return requestContextBuilder.schemaRequest(KEYSPACE);
    }

    @Override
    public String okResponseBody()
    {
        return "{\"keyspace\":\"cycling\",\"schema\":\"CREATE KEYSPACE cycling WITH REPLICATION = { 'class' : " +
               "'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '1' } " +
               "AND DURABLE_WRITES = true;" +
               "\\n\\nCREATE TABLE cycling.cyclist_category (\\n    category text,\\n    points int," +
               "\\n    id uuid,\\n" +
               "    lastname text,\\n    PRIMARY KEY (category, points)\\n) WITH CLUSTERING ORDER BY (points DESC)\\n" +
               "    AND read_repair = 'BLOCKING'\\n    AND gc_grace_seconds = 864000\\n    AND " +
               "additional_write_policy = '99p'\\n    AND bloom_filter_fp_chance = 0.01\\n" +
               "    AND caching = { 'keys' :" +
               " 'ALL', 'rows_per_partition' : 'NONE' }\\n    AND comment = ''\\n    AND compaction = { 'class' :" +
               " 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32," +
               " 'min_threshold' : 4 }\\n    AND compression = { 'chunk_length_in_kb' : 16, 'class' :" +
               " 'org.apache.cassandra.io.compress.LZ4Compressor' }\\n    AND default_time_to_live = 0\\n    " +
               "AND speculative_retry = '99p'\\n    AND min_index_interval = 128\\n" +
               "    AND max_index_interval = 2048\\n" +
               "    AND crc_check_chance = 1.0\\n    AND cdc = false\\n    AND memtable_flush_period_in_ms = 0;" +
               "\\n\\nCREATE TABLE cycling.cyclist_name (\\n    id uuid,\\n    firstname text," +
               "\\n    lastname text,\\n" +
               "    PRIMARY KEY (id)\\n) WITH read_repair = 'BLOCKING'\\n    AND gc_grace_seconds = 864000\\n    " +
               "AND additional_write_policy = '99p'\\n    AND bloom_filter_fp_chance = 0.01\\n    AND caching = " +
               "{ 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }\\n    AND comment = ''\\n    AND compaction = { " +
               "'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, " +
               "'min_threshold' : 4 }\\n    AND compression = { 'chunk_length_in_kb' : 16, 'class' : " +
               "'org.apache.cassandra.io.compress.LZ4Compressor' }\\n    AND default_time_to_live = 0\\n    " +
               "AND speculative_retry = '99p'\\n    AND min_index_interval = 128\\n" +
               "    AND max_index_interval = 2048\\n" +
               "    AND crc_check_chance = 1.0\\n    AND cdc = false\\n    AND memtable_flush_period_in_ms = 0;" +
               "\\n\\nCREATE TABLE cycling.rank_by_year_and_name (\\n    race_year int,\\n    race_name text,\\n    " +
               "rank int,\\n    cyclist_name text,\\n    PRIMARY KEY ((race_year, race_name), rank)\\n)" +
               " WITH CLUSTERING" +
               " ORDER BY (rank ASC)\\n    AND read_repair = 'BLOCKING'\\n    AND gc_grace_seconds = 864000\\n    " +
               "AND additional_write_policy = '99p'\\n    AND bloom_filter_fp_chance = 0.01\\n    AND caching = " +
               "{ 'keys' : 'ALL', 'rows_per_partition' : 'NONE' }\\n    AND comment = ''\\n    AND compaction = " +
               "{ 'class' : 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold' : 32, " +
               "'min_threshold' : 4 }\\n    AND compression = { 'chunk_length_in_kb' : 16, 'class' : " +
               "'org.apache.cassandra.io.compress.LZ4Compressor' }\\n    AND default_time_to_live = 0\\n    AND " +
               "speculative_retry = '99p'\\n    AND min_index_interval = 128\\n    AND max_index_interval = 2048\\n" +
               "    AND crc_check_chance = 1.0\\n    AND cdc = false\\n    AND memtable_flush_period_in_ms = 0;\\n\"}";
    }

    @Override
    public String expectedEndpointPath()
    {
        return ApiEndpointsV1.KEYSPACE_SCHEMA_ROUTE.replaceAll(ApiEndpointsV1.KEYSPACE_PATH_PARAM, KEYSPACE);
    }

    @Override
    public void validateResponse(SchemaResponse response)
    {
        assertThat(response).isNotNull();
        assertThat(response.keyspace()).isEqualTo(KEYSPACE);
        assertThat(response.schema()).contains("CREATE KEYSPACE cycling");
        assertThat(response.schema()).contains("CREATE TABLE cycling.cyclist_category");
        assertThat(response.schema()).contains("CREATE TABLE cycling.cyclist_name");
        assertThat(response.schema()).contains("CREATE TABLE cycling.rank_by_year_and_name");
    }
}
