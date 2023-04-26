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

import org.apache.cassandra.sidecar.client.request.Request;

/**
 * Represents an exception raised when the number of retries for a given request are exhausted
 */
public class RetriesExhaustedException extends RuntimeException
{
    /**
     * Constructs an exception with the number of {@code attempts} performed for the request.
     *
     * @param attempts the number of attempts performed for the request
     * @param request  the HTTP request
     */
    public RetriesExhaustedException(int attempts, Request request)
    {
        this(attempts, request, null);
    }

    /**
     * Constructs an exception with the number of {@code attempts} performed for the request.
     *
     * @param attempts  the number of attempts performed for the request
     * @param request   the HTTP request
     * @param throwable the underlying exception
     */
    public RetriesExhaustedException(int attempts, Request request, Throwable throwable)
    {
        super(String.format("Unable to complete request '%s' after %d attempt%s",
                            request.requestURI(), attempts, attempts == 1 ? "" : "s"), throwable);
    }
}
