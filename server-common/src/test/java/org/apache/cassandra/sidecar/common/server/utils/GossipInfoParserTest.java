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

package org.apache.cassandra.sidecar.common.server.utils;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.response.GossipInfoResponse;

import static org.assertj.core.api.Assertions.assertThat;

class GossipInfoParserTest
{
    @Test
    void testParseGossipInfo()
    {
        GossipInfoResponse result = GossipInfoParser.parse(SAMPLE_GOSSIP_INFO);
        assertThat(result).containsKey("/127.0.0.1")
                          .containsKey("localhost2/127.0.0.2")
                          .containsKey("/127.0.0.3");
        assertThat(result.get("/127.0.0.1")).hasSize(6);
        assertThat(result.get("localhost2/127.0.0.2")).hasSize(6);
        assertThat(result.get("/127.0.0.3")).hasSize(6);
        GossipInfoResponse.GossipInfo gossipInfo = result.get("/127.0.0.1");
        assertThat(gossipInfo.generation()).isEqualTo("1668100877");
        assertThat(gossipInfo.heartbeat()).isEqualTo("242");
        assertThat(gossipInfo.load()).isEqualTo("88971.0");
        assertThat(gossipInfo.statusWithPort()).isEqualTo("NORMAL,-9223372036854775808");
        assertThat(gossipInfo.sstableVersions()).isEqualTo(Collections.singletonList("big-nb"));
        assertThat(gossipInfo.tokens()).isEqualTo("<hidden>");
    }

    @Test
    void testGossipInfoHostHeaderCheck()
    {
        assertThat(GossipInfoParser.isGossipInfoHostHeader("/127.0.0.1"))
            .as("should parse format /IP")
            .isTrue();
        assertThat(GossipInfoParser.isGossipInfoHostHeader("optional_hostname/127.0.0.1"))
            .as("should parse format HOSTNAME/IP")
            .isTrue();
        assertThat(GossipInfoParser.isGossipInfoHostHeader("127.0.0.1"))
            .as("should reject IP without the heading /")
            .isFalse();
    }

    private static final String SAMPLE_GOSSIP_INFO =
        "/127.0.0.3\n" +
        "  generation:1668100877\n" +
        "  heartbeat:248\n" +
        "  LOAD:217:88883.0\n" +
        "  STATUS_WITH_PORT:71:NORMAL,3074457345618258602\n" +
        "  SSTABLE_VERSIONS:6:big-nb\n" +
        "  TOKENS:70:<hidden>\n" +
        "localhost2/127.0.0.2\n" +
        "  generation:1668100877\n" +
        "  heartbeat:243\n" +
        "  LOAD:211:83702.0\n" +
        "  STATUS_WITH_PORT:19:NORMAL,-3074457345618258603\n" +
        "  SSTABLE_VERSIONS:6:big-nb\n" +
        "  TOKENS:18:<hidden>\n" +
        "/127.0.0.1\n" +
        "  generation:1668100877\n" +
        "  heartbeat:242\n" +
        "  LOAD:211:88971.0\n" +
        "  STATUS_WITH_PORT:19:NORMAL,-9223372036854775808\n" +
        "  SSTABLE_VERSIONS:6:big-nb\n" +
        "  TOKENS:18:<hidden>";
}
