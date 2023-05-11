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
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.Json;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * Provides a simple REST endpoint to determine if a node is available
 */
@Singleton
@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
public class CassandraHealthService
{
    private final InstanceMetadataFetcher metadataFetcher;

    @Inject
    public CassandraHealthService(InstanceMetadataFetcher metadataFetcher)
    {
        this.metadataFetcher = metadataFetcher;
    }

    @Operation(summary = "Health Check for Cassandra's status",
    description = "Returns HTTP 200 if Cassandra is available, 503 otherwise",
    responses = {
    @ApiResponse(responseCode = "200", description = "Cassandra is available"),
    @ApiResponse(responseCode = "503", description = "Cassandra is not available")
    })
    @GET
    @Path("/v1/cassandra/__health")
    public Response cassandraHealth(@Context HttpServerRequest req,
                                    @QueryParam(AbstractHandler.INSTANCE_ID) Integer instanceId)
    {
        CassandraAdapterDelegate cassandra;
        try
        {
            cassandra = instanceId == null
                        ? metadataFetcher.delegate(req.host())
                        : metadataFetcher.delegate(instanceId);
        }
        catch (IllegalArgumentException e)
        {
            return Response.status(HttpResponseStatus.BAD_REQUEST.code()).entity(e.getMessage()).build();
        }
        return healthResponse(cassandra);
    }

    @Operation(summary = "Health Check for a particular cassandra instance's status",
    description = "Returns HTTP 200 if Cassandra instance is available, 503 otherwise",
    responses = {
    @ApiResponse(responseCode = "200", description = "Cassandra is available"),
    @ApiResponse(responseCode = "503", description = "Cassandra is not available")
    })
    @GET
    @Path("/v1/cassandra/instance/{instanceId}/__health")
    public Response cassandraHealthForInstance(@PathParam("instanceId") int instanceId)
    {
        final CassandraAdapterDelegate cassandra = metadataFetcher.delegate(instanceId);
        return healthResponse(cassandra);
    }

    private Response healthResponse(CassandraAdapterDelegate cassandra)
    {
        final boolean up = cassandra.isUp();
        int status = up ? HttpResponseStatus.OK.code() : HttpResponseStatus.SERVICE_UNAVAILABLE.code();
        return Response.status(status).entity(Json.encode(ImmutableMap.of("status", up ?
                                                                                    "OK" : "NOT_OK"))).build();
    }
}
