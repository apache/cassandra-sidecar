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

import java.util.Collections;
import javax.servlet.ServletConfig;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import io.swagger.v3.core.util.Json;
import io.swagger.v3.jaxrs2.Reader;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.integration.SwaggerConfiguration;
import io.swagger.v3.oas.models.OpenAPI;

/**
 * Exposes Swagger OpenAPI definition for all SideCar REST APIs
 */
@Path("/api/v1/schema/openapi.{type:json}")
public class SwaggerOpenApiResource
{
    static final OpenAPI OAS;
    @Context
    ServletConfig config;
    @Context
    Application app;

    public SwaggerOpenApiResource()
    {
        super();
    }

    @GET
    @Produces({ MediaType.APPLICATION_JSON })
    @Operation(hidden = true)
    public Response openApi(@Context HttpHeaders headers,
                            @Context UriInfo uriInfo,
                            @PathParam("type") String type)
    {
        return Response.status(Response.Status.OK)
                       .entity(Json.pretty(OAS))
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .build();
    }

    static
    {
        Reader reader = new Reader(new SwaggerConfiguration());
        OAS = reader.read(Collections.emptySet());
    }
}
