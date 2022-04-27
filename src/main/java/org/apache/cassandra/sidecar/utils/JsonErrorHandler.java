package org.apache.cassandra.sidecar.utils;


import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.ErrorHandler;
import io.vertx.ext.web.handler.HttpException;

import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_TIMEOUT;

/**
 * Handles failures in Sidecar, and provides a detailed JSON response payload for {@link HttpException HttpExceptions}
 * and {@code REQUEST_TIMEOUT} errors.
 */
public class JsonErrorHandler implements ErrorHandler
{
    /**
     * {@inheritDoc}
     */
    @Override
    public void handle(RoutingContext ctx)
    {
        Throwable t = ctx.failure();

        if (t instanceof HttpException)
        {
            handleHttpException(ctx, (HttpException) t);
        }
        else if (ctx.statusCode() == REQUEST_TIMEOUT.code())
        {
            handleRequestTimeout(ctx);
        }
        else
        {
            ctx.next(); // let vertx handle other errors
        }
    }

    /**
     * Writes a JSON payload to the response with the {@code Fail} status and the {@code message} provided
     * by the {@code httpException}. The {@link HttpException#getStatusCode() status code} from the exception
     * is used for the status code for the response.
     *
     * @param ctx           the context for the handling of a request in Vert.x-Web
     * @param httpException the {@link HttpException} to be handled
     */
    private void handleHttpException(RoutingContext ctx, HttpException httpException)
    {
        JsonObject payload = new JsonObject()
                             .put("status", "Fail")
                             .put("message", httpException.getPayload());

        writeResponse(ctx, httpException.getStatusCode(), payload);
    }

    /**
     * Writes a JSON payload to the response with the {@code Request Timeout} status and the
     * {@link io.netty.handler.codec.http.HttpResponseStatus#REQUEST_TIMEOUT} status code.
     *
     * @param ctx the context for the handling of a request in Vert.x-Web
     */
    private void handleRequestTimeout(RoutingContext ctx)
    {
        JsonObject payload = new JsonObject().put("status", "Request Timeout");
        writeResponse(ctx, REQUEST_TIMEOUT.code(), payload);
    }

    /**
     * Writes the {@code payload} with the given {@code statusCode} to the response.
     *
     * @param ctx        the context for the handling of a request in Vert.x-Web
     * @param statusCode the HTTP status code for the response
     * @param payload    the JSON payload for the response
     */
    private void writeResponse(RoutingContext ctx, int statusCode, JsonObject payload)
    {
        HttpServerResponse response = ctx.response();
        if (!response.ended() && !response.closed())
        {
            response.setStatusCode(statusCode)
                    .putHeader(HttpHeaders.CONTENT_TYPE.toString(), "application/json")
                    .end(payload.encode());
        }
    }
}
