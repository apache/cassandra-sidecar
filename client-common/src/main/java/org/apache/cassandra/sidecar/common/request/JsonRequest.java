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

import java.lang.reflect.ParameterizedType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Json request that returns json response
 *
 * @param <T> response type
 */
public abstract class JsonRequest<T> extends Request
{
    private final JsonResponseBytesDecoder<T> responseDecoder;

    /**
     * Constructs a decodable request with the provided {@code requestURI}
     *
     * @param requestURI the URI of the request
     */
    protected JsonRequest(String requestURI)
    {
        super(requestURI);
        Class<T> type = (Class<T>) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        this.responseDecoder = new JsonResponseBytesDecoder<>(type);
    }

    @Override
    public ResponseBytesDecoder<T> responseBytesDecoder()
    {
        return responseDecoder;
    }

    @Override
    public Map<String, String> headers()
    {
        Map<String, String> headers = new HashMap<>(super.headers());
        headers.put("content-type", "application/json");
        return Collections.unmodifiableMap(headers);
    }
}
