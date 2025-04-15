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

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.cassandra.sidecar.client.HttpResponse;
import org.apache.cassandra.sidecar.common.request.Request;

/**
 * A retry policy to handle status codes returned from Sidecar server when downloading a file for Live Migration.
 */
public class LiveMigrationDownloadRetryPolicy extends RetryPolicy
{
    private final RetryPolicy delegate;
    private final String destinationPath;

    public LiveMigrationDownloadRetryPolicy(RetryPolicy delegate, String destinationPath)
    {
        this.delegate = delegate;
        this.destinationPath = destinationPath;
    }

    @Override
    public void onResponse(CompletableFuture<HttpResponse> responseFuture,
                           Request request,
                           HttpResponse response,
                           Throwable throwable,
                           int attempts,
                           boolean canRetryOnADifferentHost,
                           RetryAction retryAction)
    {
        if (response != null)
        {
            if (response.statusCode() == HttpResponseStatus.OK.code())
            {
                logger.debug("Successfully downloaded file={}", destinationPath);
                responseFuture.complete(response);
                return;
            }
            else if (response.statusCode() == HttpResponseStatus.NOT_FOUND.code()
                     || response.statusCode() == HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE.code())
            {
                // 404 -  FILE NOT FOUND means file does not exist in remote
                // and no need to download file, hence considering it as success.
                // 416 -  RANGE NOT SATISFIABLE thrown by sidecar when file
                // of size length zero is requested.
                logger.info("File not found at URI={}", request.requestURI());
                responseFuture.complete(response);
                return;
            }
        }
        delegate.onResponse(responseFuture, request, response, throwable, attempts, canRetryOnADifferentHost,
                            retryAction);
    }
}
