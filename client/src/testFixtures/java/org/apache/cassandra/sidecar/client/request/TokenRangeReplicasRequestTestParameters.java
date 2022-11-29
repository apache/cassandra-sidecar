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
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for client requests accessing the token range replicas endpoint
 */
class TokenRangeReplicasRequestTestParameters implements RequestTestParameters<TokenRangeReplicasResponse>
{
    public static final String KEYSPACE = "cycling";

    @Override
    public RequestContext.Builder specificRequest(RequestContext.Builder requestContextBuilder)
    {
        return requestContextBuilder.tokenRangeReplicasRequest(KEYSPACE);
    }

    @Override
    public String okResponseBody()
    {
        return "{\"writeReplicas\":[{\"start\":\"-9223372036854775808\",\"end\":\"-3074457345618258603\"," +
               "\"replicas\":{\"datacenter1\":[\"127.0.0.2:7000\"]}},{\"start\":\"-3074457345618258603\"," +
               "\"end\":\"3074457345618258602\",\"replicas\":{\"datacenter1\":[\"127.0.0.3:7000\"]}}," +
               "{\"start\":\"3074457345618258602\",\"end\":\"9223372036854775807\",\"replicas\":" +
               "{\"datacenter1\":[\"127.0.0.1:7000\"]}}],\"readReplicas\":[{\"start\":\"3074457345618258602\"," +
               "\"end\":\"9223372036854775807\",\"replicas\":{\"datacenter1\":[\"127.0.0.1:7000\"]}}," +
               "{\"start\":\"-3074457345618258603\",\"end\":\"3074457345618258602\",\"replicas\":" +
               "{\"datacenter1\":[\"127.0.0.3:7000\"]}},{\"start\":\"-9223372036854775808\"," +
               "\"end\":\"-3074457345618258603\",\"replicas\":{\"datacenter1\":[\"127.0.0.2:7000\"]}}]}";
    }

    @Override
    public String expectedEndpointPath()
    {
        return ApiEndpointsV1.KEYSPACE_TOKEN_MAPPING_ROUTE.replaceAll(ApiEndpointsV1.KEYSPACE_PATH_PARAM, KEYSPACE);
    }

    @Override
    public void validateResponse(TokenRangeReplicasResponse response)
    {
        assertThat(response).isNotNull();
        assertThat(response.writeReplicas()).isNotNull();
        assertThat(response.readReplicas()).isNotNull();
        assertThat(response.writeReplicas()).hasSize(3);
        assertThat(response.writeReplicas().get(0).start()).isEqualTo("-9223372036854775808");
        assertThat(response.writeReplicas().get(0).end()).isEqualTo("-3074457345618258603");
        assertThat(response.writeReplicas().get(0).replicasByDatacenter()).hasSize(1);
        assertThat(response.writeReplicas().get(0).replicasByDatacenter())
        .hasEntrySatisfying("datacenter1", v -> assertThat(v).hasSize(1));
        assertThat(response.readReplicas()).hasSize(3);
        assertThat(response.readReplicas().get(1).start()).isEqualTo("-3074457345618258603");
        assertThat(response.readReplicas().get(1).end()).isEqualTo("3074457345618258602");
        assertThat(response.readReplicas().get(1).replicasByDatacenter()).hasSize(1);
        assertThat(response.readReplicas().get(1).replicasByDatacenter())
        .hasEntrySatisfying("datacenter1", v -> assertThat(v).hasSize(1));
    }
}
