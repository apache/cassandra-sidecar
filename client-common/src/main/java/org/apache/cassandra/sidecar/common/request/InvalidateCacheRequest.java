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

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.response.HealthResponse;
import org.jetbrains.annotations.Nullable;

import static java.net.URLEncoder.encode;

/**
 * Represents a request to invalidate the specified cache
 */
public class InvalidateCacheRequest extends JsonRequest<HealthResponse>
{
    /**
     * Constructs a request to execute cache invalidation operation
     *
     * @param cacheName the name of the cache to invalidate
     * @param keys the specific keys to invalidate, or null to invalidate all keys
     */
    public InvalidateCacheRequest(String cacheName, @Nullable List<String> keys)
    {
        super(buildRequestURI(cacheName, keys));
    }

    /**
     * Builds the request URI with optional query parameters for specific keys
     *
     * @param cacheName the name of the cache to invalidate
     * @param keys the specific keys to invalidate, or null to invalidate all keys
     * @return the complete request URI with query parameters if keys are provided
     */
    private static String buildRequestURI(String cacheName, @Nullable List<String> keys)
    {
        String baseUri = ApiEndpointsV1.INVALIDATE_CACHE_ROUTE
                         .replaceAll(ApiEndpointsV1.CACHE_NAME_PARAM, cacheName);

        if (keys == null || keys.isEmpty())
        {
            return baseUri;
        }

        StringBuilder uri = new StringBuilder(baseUri).append('?');
        for (int i = 0; i < keys.size(); i++)
        {
            if (i > 0)
            {
                uri.append('&');
            }
            try
            {
                uri.append("keys=").append(encode(keys.get(i), StandardCharsets.UTF_8.name()));
            }
            catch (UnsupportedEncodingException e)
            {
                // UTF-8 is always supported, this should never happen
                throw new RuntimeException("UTF-8 encoding not supported", e);
            }
        }
        return uri.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HttpMethod method()
    {
        return HttpMethod.DELETE;
    }
}
