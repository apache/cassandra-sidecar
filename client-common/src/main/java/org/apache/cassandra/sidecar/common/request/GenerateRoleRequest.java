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
import org.apache.cassandra.sidecar.common.request.data.GenerateRoleRequestPayload;
import org.apache.cassandra.sidecar.common.response.GenerateRoleResponse;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.GENERATE_ROLE;

/**
 * Server-side role generation needs CEP-55. If executed against older nodes,
 * then client-side generation has to be used.
 */
public class GenerateRoleRequest extends JsonRequest<GenerateRoleResponse>
{
    private final GenerateRoleRequestPayload payload;

    /**
     * Constructs a request to generate a role.
     *
     * @param payload payload with generation parameters
     */
    public GenerateRoleRequest(GenerateRoleRequestPayload payload)
    {
        super(GENERATE_ROLE);
        this.payload = payload;
    }

    public HttpMethod method()
    {
        return HttpMethod.PUT;
    }

    public GenerateRoleRequestPayload requestBody()
    {
        return payload;
    }
}
