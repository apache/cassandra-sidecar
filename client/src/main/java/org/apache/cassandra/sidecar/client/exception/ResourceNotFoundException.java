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

package org.apache.cassandra.sidecar.client.exception;

import org.apache.cassandra.sidecar.common.request.Request;

/**
 * An exception raised when the HTTP resource is Not Found (A 404 Status Code response).
 */
public class ResourceNotFoundException extends RuntimeException
{
    /**
     * Constructs a new exception for a request where the URI is not found on the remote server
     *
     * @param request the HTTP request
     */
    public ResourceNotFoundException(Request request)
    {
        super(String.format("The resource for the request '%s' does not exist", request.requestURI()));
    }
}
