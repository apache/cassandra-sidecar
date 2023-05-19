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
import org.apache.cassandra.sidecar.common.data.GossipInfoResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for client requests accessing the gossip info endpoint
 */
public class GossipInfoRequestTestParameters implements RequestTestParameters<GossipInfoResponse>
{
    @Override
    public RequestContext.Builder specificRequest(RequestContext.Builder requestContextBuilder)
    {
        return requestContextBuilder.gossipInfoRequest();
    }

    @Override
    public String okResponseBody()
    {
        return "{\"/127.0.0.3:7000\":{\"generation\":\"1679961466\"," +
               "\"schema\":\"4994b214-7a05-35ea-a11c-d092a554dd5d\",\"rack\":\"101000301\"," +
               "\"heartbeat\":\"2114\",\"releaseVersion\":\"4.0.8\"," +
               "\"hostId\":\"be19c254-becb-40b9-8951-30c589c7028e\",\"nativeAddressAndPort\":\"127.0.0.3:9042\"," +
               "\"load\":\"89083.0\",\"sstableVersions\":\"big-nb\",\"tokens\":\"<hidden>\",\"rpcReady\":\"true\"," +
               "\"dc\":\"LO\",\"netVersion\":\"12\",\"statusWithPort\":\"NORMAL,3074457345618258602\"}," +
               "\"/127.0.0.1:7000\":{\"generation\":\"1679961466\"," +
               "\"schema\":\"4994b214-7a05-35ea-a11c-d092a554dd5d\",\"rack\":\"101000101\",\"heartbeat\":\"2108\"," +
               "\"releaseVersion\":\"4.0.8\",\"rpcAddress\":\"172.17.0.2\"," +
               "\"hostId\":\"33cae238-8203-41c1-880f-8cdf98ee6720\",\"nativeAddressAndPort\":\"172.17.0.2:9042\"," +
               "\"load\":\"89082.0\",\"sstableVersions\":\"big-nb\",\"tokens\":\"<hidden>\",\"rpcReady\":\"true\"," +
               "\"status\":\"NORMAL,-9223372036854775808\",\"dc\":\"LO\",\"netVersion\":\"12\",\"statusWithPort\":" +
               "\"NORMAL,-9223372036854775808\"},\"/127.0.0.2:7000\":{\"generation\":\"1679961465\"," +
               "\"schema\":\"4994b214-7a05-35ea-a11c-d092a554dd5d\",\"rack\":\"101000201\",\"heartbeat\":\"2108\"," +
               "\"releaseVersion\":\"4.0.8\",\"hostId\":\"dba02656-ea8c-4a1d-8011-cbc0dab5f411\"," +
               "\"nativeAddressAndPort\":\"127.0.0.2:9042\",\"load\":\"89084.0\",\"sstableVersions\":\"big-nb\"," +
               "\"tokens\":\"<hidden>\",\"rpcReady\":\"true\",\"dc\":\"LO\",\"netVersion\":\"12\"," +
               "\"statusWithPort\":\"NORMAL,-3074457345618258603\"}}";
    }

    @Override
    public String expectedEndpointPath()
    {
        return ApiEndpointsV1.GOSSIP_INFO_ROUTE;
    }

    @Override
    public void validateResponse(GossipInfoResponse gossipInfo)
    {
        assertThat(gossipInfo.size()).isEqualTo(3);
    }
}
