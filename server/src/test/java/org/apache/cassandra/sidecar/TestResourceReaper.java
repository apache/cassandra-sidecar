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

package org.apache.cassandra.sidecar;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import org.apache.cassandra.sidecar.common.server.utils.ThrowableUtils;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.server.Server;

/**
 * A helper to teardown test resources in the pre-defined order
 */
public class TestResourceReaper
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestResourceReaper.class);
    private Vertx vertx;
    private Server server;
    private ExecutorPools executorPools;
    private WebClient webClient;
    private List<AutoCloseable> parallelCloseables;

    public static TestResourceReaper create()
    {
        return new TestResourceReaper();
    }

    public TestResourceReaper with(Vertx vertx)
    {
        this.vertx = vertx;
        return this;
    }

    public TestResourceReaper with(Server server)
    {
        this.server = server;
        return this;
    }

    public TestResourceReaper with(ExecutorPools executorPools)
    {
        this.executorPools = executorPools;
        return this;
    }

    public TestResourceReaper with(WebClient webClient)
    {
        this.webClient = webClient;
        return this;
    }

    public TestResourceReaper with(AutoCloseable... closeables)
    {
        this.parallelCloseables = List.of(closeables);
        return this;
    }

    public Future<?> close()
    {
        try
        {
            return closeInternal();
        }
        catch (Throwable cause)
        {
            LOGGER.warn("Failed to trigger close on certain resources", cause);
            return Future.failedFuture(cause);
        }
    }

    private Future<?> closeInternal()
    {
        List<Future<?>> closeFutures = new ArrayList<>();
        if (server != null)
        {
            closeFutures.add(server.close());
        }
        if (webClient != null)
        {
            closeFutures.add(Future.future(p -> {
                webClient.close();
                p.complete();
            }));
        }

        Future<?> merged = Future.all(closeFutures);
        if (parallelCloseables != null)
        {
            List<Future<?>> batch = parallelCloseables.stream()
                                                      .map(c -> Future.future(p -> {
                                                          ThrowableUtils.propagate(c::close);
                                                          p.complete();
                                                      }))
                                                      .collect(Collectors.toList());
            merged = merged.andThen(ignored -> Future.all(batch));
        }
        if (executorPools != null)
        {
            merged = merged.andThen(ignored -> executorPools.close());
        }
        if (vertx != null)
        {
            merged = merged.andThen(ignored -> vertx.close());
        }
        return merged;
    }
}
