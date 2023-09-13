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

import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;

/**
 * Represents a request to retrieve information from the token-range replicas endpoint
 */
public class TokenRangeReplicasRequest extends DecodableRequest<TokenRangeReplicasResponse>
{
    /**
     * Constructs a new request to retrieve information by keyspace from token-range replicas endpoint
     */
    public TokenRangeReplicasRequest(String keyspace)
    {
        super(ApiEndpointsV1.KEYSPACE_TOKEN_MAPPING_ROUTE.replaceAll(ApiEndpointsV1.KEYSPACE_PATH_PARAM, keyspace));
    }

    @Override
    public HttpMethod method()
    {
        return HttpMethod.GET;
    }
}
