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

package org.apache.cassandra.sidecar.routes;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.vertx.core.http.HttpServerRequest;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.NodeSettings;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;


/**
 * Provides REST endpoint to get the configured settings of a cassandra node
 */
@Singleton
@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
public class CassandraSettingsService
{
    private final InstanceMetadataFetcher metadataFetcher;

    @Inject
    public CassandraSettingsService(InstanceMetadataFetcher metadataFetcher)
    {
        this.metadataFetcher = metadataFetcher;
    }

    @Operation(summary = "Cassandra's node settings",
            description = "Returns HTTP 200 if Cassandra is available,400 on incorrect instanceId, 503 otherwise",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Cassandra status is available"),
                    @ApiResponse(responseCode = "400", description = "Incorrect instanceId"),
                    @ApiResponse(responseCode = "503", description = "Cassandra status is not available")
            })
    @GET
    @Path("/v1/cassandra/settings")
    public Response settings(@Context HttpServerRequest req,
                             @QueryParam(AbstractHandler.INSTANCE_ID) Integer instanceId)
    {
        NodeSettings nodeSettings;
        try
        {
            CassandraAdapterDelegate cassandra = metadataFetcher.getDelegate(req.host(), instanceId);
            nodeSettings = cassandra.getSettings();
        }
        catch (IllegalArgumentException e)
        {
            return Response.status(HttpResponseStatus.BAD_REQUEST.code()).entity(e.getMessage()).build();
        }
        int statusCode = nodeSettings != null
                ? HttpResponseStatus.OK.code()
                : HttpResponseStatus.SERVICE_UNAVAILABLE.code();
        return Response.status(statusCode).entity(nodeSettings).build();
    }

}
