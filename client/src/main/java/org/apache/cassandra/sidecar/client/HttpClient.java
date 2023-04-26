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

import java.util.concurrent.CompletableFuture;

/**
 * An interface to represent an HTTP client
 */
public interface HttpClient extends AutoCloseable
{
    /**
     * @return the configuration parameters for this client
     */
    HttpClientConfig config();

    /**
     * Returns an {@link HttpResponse} {@link CompletableFuture} for the {@code request} on the
     * provided {@code sidecarInstance}.
     *
     * @param sidecarInstance the Sidecar instance
     * @param requestContext  the request context
     * @return an {@link HttpResponse} {@link CompletableFuture} for the request on the provided instance
     */
    CompletableFuture<HttpResponse> execute(SidecarInstance sidecarInstance, RequestContext requestContext);

    /**
     * Consumes a stream for the {@code request} using the {@code streamConsumer} object on the provided
     * {@code sidecarInstance}.
     *
     * @param sidecarInstance the Sidecar instance
     * @param requestContext  the request context
     * @param streamConsumer  the object that consumes the stream
     * @return an {@link HttpResponse} {@link CompletableFuture} for the request on the provided instance
     */
    CompletableFuture<HttpResponse> stream(SidecarInstance sidecarInstance, RequestContext requestContext,
                                           StreamConsumer streamConsumer);
}
