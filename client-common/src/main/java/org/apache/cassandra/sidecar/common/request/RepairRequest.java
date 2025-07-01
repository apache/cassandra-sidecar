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
import org.apache.cassandra.sidecar.common.request.data.RepairPayload;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;

/**
 * Class representing the repair API request
 */
public class RepairRequest extends JsonRequest<OperationalJobResponse>
{
    private final RepairPayload repairRequestPayload;


    /**
     * Constructs a Sidecar repair request with the given parameters.
     *
     * @param keyspace             name of the keyspace in the cluster
     * @param repairRequestPayload request payload
     */
    public RepairRequest(String keyspace, RepairPayload repairRequestPayload)
    {
        super(requestURI(keyspace));
        this.repairRequestPayload = repairRequestPayload;
    }

    @Override
    public HttpMethod method()
    {
        return HttpMethod.PUT;
    }

    @Override
    public Object requestBody()
    {
        return repairRequestPayload;
    }

    static String requestURI(String keyspace)
    {
        return ApiEndpointsV1.REPAIR_ROUTE
               .replaceAll(ApiEndpointsV1.KEYSPACE_PATH_PARAM, keyspace);
    }
}
