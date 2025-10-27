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

import java.util.List;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.request.data.NodeFlushRequestPayload;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;

/**
 * Represents a node flush request
 */
public class NodeFlushRequest extends JsonRequest<OperationalJobResponse>
{
    private final NodeFlushRequestPayload payload;

    /**
     * Constructs a NodeFlushRequest for the given keyspace and table names
     *
     * @param keyspace   the keyspace name
     * @param tableNames the list of table names to flush (can be empty)
     */
    public NodeFlushRequest(String keyspace, List<String> tableNames)
    {
        super(ApiEndpointsV1.NODE_FLUSH_ROUTE.replace(ApiEndpointsV1.KEYSPACE_PATH_PARAM, keyspace));
        this.payload = new NodeFlushRequestPayload(tableNames);
    }

    /**
     * @return the list of table names to flush
     */
    public List<String> tableNames()
    {
        return payload.tableNames();
    }

    @Override
    public HttpMethod method()
    {
        return HttpMethod.POST;
    }

    @Override
    public Object requestBody()
    {
        return payload;
    }
}
