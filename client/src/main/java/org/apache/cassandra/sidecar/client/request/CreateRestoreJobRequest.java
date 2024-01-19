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
import org.apache.cassandra.sidecar.common.data.CreateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.data.CreateRestoreJobResponsePayload;

public class CreateRestoreJobRequest extends DecodableRequest<CreateRestoreJobResponsePayload> implements JsonPayloadRequest
{
    private final CreateRestoreJobRequestPayload requestPayload;

    /**
     * Constructs a decodable request with the provided parameters
     *
     * @param keyspace       name of the keyspace in the cluster
     * @param table          name of the table in the cluster
     * @param requestPayload request payload
     */
    public CreateRestoreJobRequest(String keyspace, String table, CreateRestoreJobRequestPayload requestPayload)
    {
        super(requestURI(keyspace, table));
        this.requestPayload = requestPayload;
    }

    @Override
    public HttpMethod method()
    {
        return HttpMethod.POST;
    }

    @Override
    public Object json()
    {
        return requestPayload;
    }

    static String requestURI(String keyspace, String table)
    {
        return ApiEndpointsV1.CREATE_RESTORE_JOB_ROUTE
               .replaceAll(ApiEndpointsV1.KEYSPACE_PATH_PARAM, keyspace)
               .replaceAll(ApiEndpointsV1.TABLE_PATH_PARAM, table);
    }
}
