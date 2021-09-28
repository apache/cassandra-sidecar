package org.apache.cassandra.sidecar.models;

import java.io.File;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerResponse;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Wrapper around HttpServerResponse
 */
public class HttpResponse
{
    private final HttpServerResponse response;

    public HttpResponse(HttpServerResponse response)
    {
        this.response = response;
    }

    public void setRetryAfterHeader(long microsToWait)
    {
        response.setStatusCode(HttpResponseStatus.TOO_MANY_REQUESTS.code());
        response.putHeader(HttpHeaderNames.RETRY_AFTER, Long.toString(MICROSECONDS.toSeconds(microsToWait))).end();
    }

    public void setTooManyRequestsStatus()
    {
        response.setStatusCode(HttpResponseStatus.TOO_MANY_REQUESTS.code()).end();
    }

    public void setRangeNotSatisfiable(String msg)
    {
        response.setStatusCode(HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE.code()).setStatusMessage(msg).end();
    }

    public void setPartialContentStatus(Range range)
    {
        response.setStatusCode(HttpResponseStatus.PARTIAL_CONTENT.code())
                .putHeader(HttpHeaderNames.CONTENT_RANGE, contentRangeHeader(range));
    }

    private String contentRangeHeader(Range r)
    {
        return "bytes " + r.start() + "-" + r.end() + "/" + r.length();
    }

    public void setBadRequestStatus(String msg)
    {
        response.setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).setStatusMessage(msg).end();
    }

    public void setNotFoundStatus(String msg)
    {
        response.setStatusCode(HttpResponseStatus.NOT_FOUND.code()).setStatusMessage(msg).end();
    }

    public void setInternalErrorStatus(String msg)
    {
        response.setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()).setStatusMessage(msg).end();
    }

    public void sendFile(File file)
    {
        response.putHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_OCTET_STREAM)
                .putHeader(HttpHeaderNames.CONTENT_LENGTH, Long.toString(file.length()))
                .sendFile(file.getAbsolutePath());
    }

    public void sendFile(File file, Range range)
    {
        if (range.length() != file.length())
        {
            setPartialContentStatus(range);
        }
        response.putHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_OCTET_STREAM)
                .putHeader(HttpHeaderNames.CONTENT_LENGTH, Long.toString(range.length()))
                .sendFile(file.getAbsolutePath(), range.start(), range.length());
    }

    public void setForbiddenStatus(String msg)
    {
        response.setStatusCode(HttpResponseStatus.FORBIDDEN.code()).setStatusMessage(msg).end();
    }
}
