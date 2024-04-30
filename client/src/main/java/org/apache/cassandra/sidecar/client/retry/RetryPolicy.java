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

package org.apache.cassandra.sidecar.client.retry;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.client.HttpResponse;
import org.apache.cassandra.sidecar.client.exception.UnexpectedStatusCodeException;
import org.apache.cassandra.sidecar.common.request.Request;

/**
 * An abstract class representing a retry policy
 */
public abstract class RetryPolicy
{
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Method called when a {@code response} is received and a decision needs to be made about the response status.
     *
     * @param responseFuture           a future that returns the response, or an exception when the retry is
     *                                 unsuccessful
     * @param request                  the HTTP request
     * @param response                 the {@link HttpResponse} received from the server
     * @param throwable                the error encountered during the request, or null if no error was encountered
     * @param attempts                 the number of attempts performed for the request
     * @param canRetryOnADifferentHost whether this request can be retried on a different host or not
     * @param retryAction              an action called when a request is retried
     */
    public abstract void onResponse(CompletableFuture<HttpResponse> responseFuture,
                                    Request request,
                                    HttpResponse response,
                                    Throwable throwable,
                                    int attempts,
                                    boolean canRetryOnADifferentHost,
                                    RetryAction retryAction);

    /**
     * Returns an {@link UnsupportedOperationException} with the provided {@code response}.
     *
     * @param response the response to use
     * @return an {@link UnsupportedOperationException} with the provided {@code response}
     */
    UnsupportedOperationException unsupportedOperation(HttpResponse response)
    {
        return new UnsupportedOperationException(response.contentAsString());
    }

    /**
     * Returns a {@link UnexpectedStatusCodeException} with the provided {@code response}.
     *
     * @param response the response to use
     * @return a {@link UnexpectedStatusCodeException} with the provided {@code response}
     */
    UnexpectedStatusCodeException unexpectedStatusCode(HttpResponse response)
    {
        return new UnexpectedStatusCodeException(response.statusCode(), response.contentAsString());
    }
}
