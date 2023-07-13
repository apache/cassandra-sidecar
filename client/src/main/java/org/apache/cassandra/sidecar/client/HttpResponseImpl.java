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

package org.apache.cassandra.sidecar.client;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A simple implementation of the {@link HttpResponse}
 */
public class HttpResponseImpl implements HttpResponse
{
    private final int statusCode;
    private final String statusMessage;
    private final byte[] raw;
    private final Map<String, List<String>> headers;
    private final SidecarInstance respondingServer;

    /**
     * Constructs a response object with the provided values
     *
     * @param statusCode    the status code of the response
     * @param statusMessage the status message of the response
     * @param headers       the headers from the response
     * @param server        the server that returns the response
     */
    public HttpResponseImpl(int statusCode,
                            String statusMessage,
                            Map<String, List<String>> headers,
                            SidecarInstance server)
    {
        this(statusCode, statusMessage, null, headers, server);
    }

    /**
     * Constructs a response object with the provided values
     *
     * @param statusCode    the status code of the response
     * @param statusMessage the status message of the response
     * @param raw           the raw bytes received from the response
     * @param headers       the headers from the response
     * @param server        the server that returns the response
     */
    public HttpResponseImpl(int statusCode,
                            String statusMessage,
                            byte[] raw,
                            Map<String, List<String>> headers,
                            SidecarInstance server)
    {
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.raw = raw;
        this.headers = Collections.unmodifiableMap(headers);
        this.respondingServer = server;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int statusCode()
    {
        return statusCode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String statusMessage()
    {
        return statusMessage;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] raw()
    {
        return raw;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String contentAsString(String charset)
    {
        if (raw == null)
        {
            return null;
        }
        try
        {
            return new String(raw, charset);
        }
        catch (UnsupportedEncodingException unsupportedEncodingException)
        {
            throw new RuntimeException("Unable to decode for charset " + charset, unsupportedEncodingException);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, List<String>> headers()
    {
        return headers;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SidecarInstance respondingServer()
    {
        return respondingServer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "HttpResponseImpl{" +
               "statusCode=" + statusCode +
               ", statusMessage='" + statusMessage + '\'' +
               ", contentAsString='" + contentAsString() + '\'' +
               ", headers=" + headers +
               '}';
    }
}
