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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.utils.HttpRange;

/**
 * An abstract class representing a request for Sidecar
 */
public abstract class Request
{
    protected final String requestURI;

    /**
     * Constructs a Sidecar request with the given {@code requestURI}. Defaults to {@code ssl} enabled.
     *
     * @param requestURI the URI of the request
     */
    protected Request(String requestURI)
    {
        this.requestURI = requestURI;
    }

    /**
     * @return a map of headers for the request
     */
    public Map<String, String> headers()
    {
        HttpRange range = range();
        if (range == null)
        {
            return Collections.emptyMap();
        }
        Map<String, String> headers = new HashMap<>();
        headers.put(HttpHeaderNames.RANGE.toString(), range.toString());
        return Collections.unmodifiableMap(headers);
    }

    /**
     * @return the range for the HTTP request
     */
    protected HttpRange range()
    {
        return null;
    }

    /**
     * @return the HTTP method to be used for the request
     */
    public abstract HttpMethod method();

    /**
     * @return the URI for the request
     */
    public String requestURI()
    {
        return requestURI;
    }

    /**
     * @return the request body. Returns null, when there is no request body.
     */
    public Object requestBody()
    {
        return null;
    }

    /**
     * @return the response bytes decoder if configured, or null
     */
    public ResponseBytesDecoder<?> responseBytesDecoder()
    {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" +
               "requestURI='" + requestURI + '\'' +
               '}';
    }
}
