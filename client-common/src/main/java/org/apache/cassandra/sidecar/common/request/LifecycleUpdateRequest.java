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

 package org.apache.cassandra.sidecar.common.request;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.request.data.NodeCommandRequestPayload;
import org.apache.cassandra.sidecar.common.response.LifecycleInfoResponse;

/**
 * Lifecycle update request
 */
public class LifecycleUpdateRequest extends JsonRequest<LifecycleInfoResponse>
{
    private final NodeCommandRequestPayload requestPayload;

    /**
     * Constructs a lifecycle update request with the provided parameters
     *
     * @param state "start" or "stop" indicating the desired operation
     */
    public LifecycleUpdateRequest(NodeCommandRequestPayload.State state)
    {
        super(ApiEndpointsV1.LIFECYCLE_ROUTE);
        this.requestPayload = new NodeCommandRequestPayload(state.toValue());
    }

    @Override
    public HttpMethod method()
    {
        return HttpMethod.PUT;
    }

    @Override
    public Object requestBody()
    {
        return requestPayload;
    }
}
