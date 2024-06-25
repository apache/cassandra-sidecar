/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.common.request;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.request.data.RestoreJobProgressRequestParams;
import org.apache.cassandra.sidecar.common.response.data.RestoreJobProgressResponsePayload;

/**
 * Represents a request to retrieve the status of a restore job
 */
public class RestoreJobProgressRequest extends JsonRequest<RestoreJobProgressResponsePayload>
{
    /**
     * Constructs a decodable request with the provided {@code requestURI}
     */
    public RestoreJobProgressRequest(RestoreJobProgressRequestParams requestParams)
    {
        super(requestURI(requestParams));
    }

    @Override
    public HttpMethod method()
    {
        return HttpMethod.GET;
    }

    static String requestURI(RestoreJobProgressRequestParams requestParams)
    {
        String base = ApiEndpointsV1.RESTORE_JOB_PROGRESS_ROUTE
                      .replaceAll(ApiEndpointsV1.KEYSPACE_PATH_PARAM, requestParams.keyspace)
                      .replaceAll(ApiEndpointsV1.TABLE_PATH_PARAM, requestParams.table)
                      .replaceAll(ApiEndpointsV1.JOB_ID_PATH_PARAM, requestParams.jobId.toString());
        return base + "?" + ApiEndpointsV1.FETCH_POLICY + "=" + requestParams.fetchPolicy.toString();
    }
}
