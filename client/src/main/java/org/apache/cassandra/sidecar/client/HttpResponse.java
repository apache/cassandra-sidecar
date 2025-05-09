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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Represents the HTTP response received from the remote Sidecar service
 */
public interface HttpResponse
{
    /**
     * @return the numerical status code
     */
    int statusCode();

    /**
     * @return the status message for the response
     */
    String statusMessage();

    /**
     * @return the raw bytes received from the service
     */
    byte[] raw();

    /**
     * @return the content of the response body with {@link StandardCharsets#UTF_8} encoding
     */
    default String contentAsString()
    {
        return contentAsString(StandardCharsets.UTF_8.name());
    }

    /**
     * Returns the content of the response body the specified {@code charset}.
     *
     * @param charset the charset for the response
     * @return the content of the response body the specified {@code charset}
     */
    String contentAsString(String charset);

    /**
     * @return the headers for the response
     */
    Map<String, List<String>> headers();

    /**
     * @return the sidecar server instance that returns the response
     */
    SidecarInstance sidecarInstance();
}
