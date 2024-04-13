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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Decoder for json response body bytes
 * @param <T> expected java type
 */
public class JsonResponseBytesDecoder<T> implements ResponseBytesDecoder<T>
{
    private static final ObjectMapper MAPPER = new ObjectMapper()
                                       // ignore all the properties that are not declared
                                       .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final Class<T> type;

    public JsonResponseBytesDecoder(Class<T> type)
    {
        this.type = type;
    }

    @Override
    public T decode(byte[] bytes) throws IOException
    {
        if (bytes == null)
        {
            return null;
        }
        return MAPPER.readValue(bytes, type);
    }
}
