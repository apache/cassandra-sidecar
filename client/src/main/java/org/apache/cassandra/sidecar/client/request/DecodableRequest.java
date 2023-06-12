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

package org.apache.cassandra.sidecar.client.request;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A request that can decode to the type {@code <T>}
 *
 * @param <T> the type to decode
 */
public abstract class DecodableRequest<T> extends Request
{
    static final ObjectMapper MAPPER = new ObjectMapper()
                                       // ignore all the properties that are not declared
                                       .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final JavaType javaType = MAPPER.constructType(((ParameterizedType) this.getClass().getGenericSuperclass())
                                                           .getActualTypeArguments()[0]);

    /**
     * Constructs a decodable request with the provided {@code requestURI}
     *
     * @param requestURI the URI of the request
     */
    protected DecodableRequest(String requestURI)
    {
        super(requestURI);
    }

    /**
     * Decodes the provided {@code bytes} to an instance of the type {@code <T>}
     *
     * @param bytes the raw bytes of the response
     * @return the decoded instance for the given {@code bytes}
     * @throws IOException when the decoder is unable to decode successfully
     */
    public T decode(byte[] bytes) throws IOException
    {
        if (bytes == null)
        {
            return null;
        }
        return MAPPER.readValue(bytes, javaType);
    }

    @Override
    public Map<String, String> headers()
    {
        Map<String, String> headers = new HashMap<>(super.headers());
        headers.put("content-type", "application/json");
        return Collections.unmodifiableMap(headers);
    }
}
