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

package org.apache.cassandra.sidecar.common.http;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Non-standard HTTP response status codes used in Sidecar
 */
public class SidecarHttpResponseStatus extends HttpResponseStatus
{
    /**
     * 455 INVALID CHECKSUM
     */
    public static final HttpResponseStatus CHECKSUM_MISMATCH = newStatus(455, "Checksum Mismatch");

    /**
     * Creates a new instance with the specified {@code code} and its {@code reasonPhrase}.
     *
     * @param code         the HTTP status code
     * @param reasonPhrase the HTTP status reason
     */
    public SidecarHttpResponseStatus(int code, String reasonPhrase)
    {
        super(code, reasonPhrase);
    }

    private static HttpResponseStatus newStatus(int statusCode, String reasonPhrase)
    {
        return new SidecarHttpResponseStatus(statusCode, reasonPhrase);
    }
}
