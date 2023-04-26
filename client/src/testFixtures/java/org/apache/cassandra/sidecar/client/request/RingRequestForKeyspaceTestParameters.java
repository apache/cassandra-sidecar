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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.cassandra.sidecar.client.RequestContext;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.data.RingEntry;
import org.apache.cassandra.sidecar.common.data.RingResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for client requests accessing the ring endpoint for a given keyspace
 */
public class RingRequestForKeyspaceTestParameters implements RequestTestParameters<RingResponse>
{
    public static final String KEYSPACE = "tutorialspoint";

    @Override
    public RequestContext.Builder specificRequest(RequestContext.Builder requestContextBuilder)
    {
        return requestContextBuilder.ringRequest(KEYSPACE);
    }

    @Override
    public String okResponseBody()
    {
        return "[{\"datacenter\":\"LO\",\"address\":\"172.17.0.2\",\"port\":7000,\"rack\":\"101000101\"," +
               "\"status\":\"Up\",\"state\":\"Normal\",\"load\":\"86.99 KiB\",\"owns\":\"100.00%\"," +
               "\"token\":\"-9223372036854775808\",\"fqdn\":\"172.17.0.2\"," +
               "\"hostId\":\"33cae238-8203-41c1-880f-8cdf98ee6720\"},{\"datacenter\":\"LO\"," +
               "\"address\":\"127.0.0.2\",\"port\":7000,\"rack\":\"101000201\",\"status\":\"Up\"," +
               "\"state\":\"Normal\",\"load\":\"87 KiB\",\"owns\":\"100.00%\",\"token\":\"-3074457345618258603\"," +
               "\"fqdn\":\"127.0.0.2\",\"hostId\":\"dba02656-ea8c-4a1d-8011-cbc0dab5f411\"},{\"datacenter\":\"LO\"," +
               "\"address\":\"127.0.0.3\",\"port\":7000,\"rack\":\"101000301\",\"status\":\"Up\"," +
               "\"state\":\"Normal\",\"load\":\"87 KiB\",\"owns\":\"100.00%\",\"token\":\"3074457345618258602\"," +
               "\"fqdn\":\"127.0.0.3\",\"hostId\":\"be19c254-becb-40b9-8951-30c589c7028e\"}]";
    }

    @Override
    public String expectedEndpointPath()
    {
        return ApiEndpointsV1.RING_ROUTE_PER_KEYSPACE.replaceAll(ApiEndpointsV1.KEYSPACE_PATH_PARAM, KEYSPACE);
    }

    @Override
    public void validateResponse(RingResponse response)
    {
        assertThat(response.size()).isEqualTo(3);
        List<RingEntry> ringEntryList = StreamSupport
                                        .stream(((Iterable<RingEntry>) response).spliterator(), false)
                                        .collect(Collectors.toList());
        assertThat(ringEntryList.get(0).token()).isEqualTo("-9223372036854775808");
        assertThat(ringEntryList.get(1).token()).isEqualTo("-3074457345618258603");
        assertThat(ringEntryList.get(2).token()).isEqualTo("3074457345618258602");
    }
}
