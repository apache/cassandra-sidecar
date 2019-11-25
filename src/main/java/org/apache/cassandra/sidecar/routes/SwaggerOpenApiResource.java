package org.apache.cassandra.sidecar.routes;

import java.util.Arrays;
import java.util.HashSet;
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

    static
    {
        Reader reader = new Reader(new SwaggerConfiguration());
        OAS = reader.read(new HashSet(Arrays.asList(HealthService.class)));
    }

    @Context
    ServletConfig config;

    @Context
    Application app;

    @GET
    @Produces({ MediaType.APPLICATION_JSON})
    @Operation(hidden = true)
    public Response getOpenApi(@Context HttpHeaders headers,
                               @Context UriInfo uriInfo,
                               @PathParam("type") String type)
    {
        return Response.status(Response.Status.OK)
                       .entity(Json.pretty(OAS))
                       .type(MediaType.APPLICATION_JSON_TYPE)
                       .build();
    }

    public SwaggerOpenApiResource()
    {
        super();
    }
}
