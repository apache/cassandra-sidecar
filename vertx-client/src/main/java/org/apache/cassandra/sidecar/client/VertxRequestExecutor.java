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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.vertx.core.Vertx;

import static java.util.Objects.requireNonNull;

/**
 * A {@link RequestExecutor} implementation that uses vertx primitives
 */
public class VertxRequestExecutor extends RequestExecutor
{
    private final Vertx vertx;

    public VertxRequestExecutor(VertxHttpClient httpClient)
    {
        super(httpClient);
        this.vertx = requireNonNull(httpClient.vertx(), "The vertx instance is required");
    }

    /**
     * Use vertx's primitives to schedule the delay
     *
     * @param delayMillis the delay before retrying in milliseconds
     * @param runnable    the code to execute
     */
    @Override
    protected void schedule(long delayMillis, Runnable runnable)
    {
        if (delayMillis > 0)
        {
            vertx.setTimer(delayMillis, p -> runnable.run());
        }
        else
        {
            runnable.run();
        }
    }

    @Override
    public void close() throws Exception
    {
        super.close();
        try
        {
            vertx.close().toCompletionStage().toCompletableFuture().get(1, TimeUnit.MINUTES);
        }
        catch (InterruptedException | ExecutionException | TimeoutException exception)
        {
            logger.warn("Failed to close vertx after 1 minute", exception);
        }
    }
}
