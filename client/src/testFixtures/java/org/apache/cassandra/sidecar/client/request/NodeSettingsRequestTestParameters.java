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
import org.apache.cassandra.sidecar.common.NodeSettings;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for client requests accessing the node settings endpoint
 */
public class NodeSettingsRequestTestParameters implements RequestTestParameters<NodeSettings>
{
    @Override
    public RequestContext.Builder specificRequest(RequestContext.Builder requestContextBuilder)
    {
        return requestContextBuilder.nodeSettingsRequest();
    }

    @Override
    public String okResponseBody()
    {
        return "{\"partitioner\":\"test-partitioner\", \"releaseVersion\": \"4.0.0\"}";
    }

    @Override
    public String expectedEndpointPath()
    {
        return ApiEndpointsV1.NODE_SETTINGS_ROUTE;
    }

    @Override
    public void validateResponse(NodeSettings settings)
    {
        assertThat(settings.partitioner()).isEqualTo("test-partitioner");
        assertThat(settings.releaseVersion()).isEqualTo("4.0.0");
    }
}
