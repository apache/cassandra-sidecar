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

package org.apache.cassandra.sidecar.metrics;

import java.util.concurrent.CompletableFuture;
import java.util.function.LongConsumer;

import io.vertx.core.Future;

/**
 * Provides functionality to measure time taken for operations to completed, time measured is generally used for
 * stats publishing.
 */
public class Timer
{
    private Timer()
    {
        throw new UnsupportedOperationException();
    }

    public static void measureTimeTaken(Runnable runnable, LongConsumer intervalConsumer)
    {
        long start = System.nanoTime();
        boolean success = true;
        try
        {
            runnable.run();
        }
        catch (RuntimeException rte)
        {
            success = false;
            throw rte;
        }
        finally
        {
            if (success)
            {
                intervalConsumer.accept(System.nanoTime() - start);
            }
        }
    }

    public static <V> Future<V> measureTimeTaken(Future<V> future, LongConsumer intervalConsumer)
    {
        long start = System.nanoTime();
        return future.onSuccess(v -> intervalConsumer.accept(System.nanoTime() - start));
    }

    public static <V> CompletableFuture<V> measureTimeTaken(CompletableFuture<V> future, LongConsumer intervalConsumer)
    {
        long start = System.nanoTime();
        return future.thenApply(v -> {
            intervalConsumer.accept(System.nanoTime() - start);
            return v;
        });
    }
}
