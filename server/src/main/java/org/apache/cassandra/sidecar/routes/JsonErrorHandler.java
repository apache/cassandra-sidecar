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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.common.WebEnvironment;
import io.vertx.ext.web.handler.ErrorHandler;
import io.vertx.ext.web.handler.HttpException;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_TIMEOUT;

/**
 * Handles failures in Sidecar, and provides a detailed JSON response payload for {@link HttpException HttpExceptions}
 * and {@code REQUEST_TIMEOUT} errors.
 */
public class JsonErrorHandler implements ErrorHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonErrorHandler.class);

    /**
     * Flag to enable/disable printing the full stack trace of exceptions.
     */
    private final boolean displayExceptionDetails;

    public JsonErrorHandler()
    {
        this(WebEnvironment.development());
    }

    /**
     * Constructs a JsonErrorHandlerObject with the provided {@code displayExceptionDetails} flag.
     *
     * @param displayExceptionDetails true if exception details should be displayed
     */
    public JsonErrorHandler(boolean displayExceptionDetails)
    {
        this.displayExceptionDetails = displayExceptionDetails;
    }

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
            handleThrowable(ctx, t);
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
                             .put("status", httpException.getMessage())
                             .put("code", httpException.getStatusCode())
                             .put("message", httpException.getPayload());

        writeResponse(ctx, httpException.getStatusCode(), payload);
    }

    /**
     * Writes a JSON payload to the response with the {@code Request Timeout} status and the
     * {@link HttpResponseStatus#REQUEST_TIMEOUT} status code.
     *
     * @param ctx the context for the handling of a request in Vert.x-Web
     */
    private void handleRequestTimeout(RoutingContext ctx)
    {
        JsonObject payload = new JsonObject().put("status", "Request Timeout");
        writeResponse(ctx, REQUEST_TIMEOUT.code(), payload);
    }

    /**
     * Writes a JSON payload to the response with the {@code statusCode} if valid and different
     * from {@link HttpResponseStatus#OK}. Otherwise, report a {@link HttpResponseStatus#INTERNAL_SERVER_ERROR}
     * code.
     *
     * @param ctx       the context for the handling of a request in Vert.x-Web
     * @param exception the throwable that produced the error
     */
    private void handleThrowable(RoutingContext ctx, Throwable exception)
    {
        int effectiveStatusCode = ctx.statusCode() != 200 ? ctx.statusCode() : INTERNAL_SERVER_ERROR.code();
        HttpResponseStatus responseStatus = HttpResponseStatus.valueOf(effectiveStatusCode);
        JsonObject payload = new JsonObject().put("status", responseStatus.reasonPhrase());
        if (displayExceptionDetails)
        {
            payload.put("code", effectiveStatusCode);

            if (exception != null)
            {
                JsonArray stack = new JsonArray();
                for (StackTraceElement elem : exception.getStackTrace())
                {
                    stack.add(elem.toString());
                }
                payload.put("message", exception.getMessage())
                       .put("stack", stack);
            }
            else
            {
                payload.put("message", responseStatus.reasonPhrase());
            }
        }
        writeResponse(ctx, responseStatus.code(), payload);
        LOGGER.error("Exception effectiveStatusCode={}", effectiveStatusCode, exception);
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
