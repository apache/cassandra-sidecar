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
package org.apache.cassandra.sidecar.routes;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.lang3.NotImplementedException;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.mapping.Result;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.VirtualTables;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;

/**
 * Service web requests for current state of cassandra, the jvm and system
 */
@Singleton
public class InfoService
{
    private VirtualTables vtables;
    private ExecutorService infoExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                                                                             .setNameFormat("info-service")
                                                                             .setDaemon(true)
                                                                             .build());

    @Inject
    public InfoService(VirtualTables vtables)
    {
        this.vtables = vtables;
    }

    private <T> void resultFutureToJson(RoutingContext rc, final ListenableFuture<Result<T>> future)
    {
        Futures.addCallback(future, new FutureCallback<Result<T>>()
        {
            public void onSuccess(@NullableDecl Result<T> results)
            {
                List<T> all = results.all();
                rc.response()
                  .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                  .end(Json.encode(all));
            }

            public void onFailure(Throwable throwable)
            {
                // An error from driver expected if lost connectivity to C*
                int status = throwable instanceof DriverException ?
                             SERVICE_UNAVAILABLE.code() : INTERNAL_SERVER_ERROR.code();

                rc.response()
                  .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                  .setStatusCode(status)
                  .end(Json.encode(ImmutableMap.of("error", throwable.getMessage())));
            }
        }, infoExecutor);
    }

    @Nullable
    <T> void virtualTable(RoutingContext rc, Class<T> pojo)
    {
        try
        {
            // there are two layers that can error here, one the ORM failing to initialize or identifying the version
            // doesnt support the table and then resultFutureToJson's check if the actual query to fetch fails
            resultFutureToJson(rc, vtables.getTableResults(pojo));
        }
        catch (Exception e)
        {
            // NotImplementedException if virtual table doesnt exist, NHA if cannot connect to check
            int status = e instanceof NotImplementedException || e instanceof NoHostAvailableException ?
                         SERVICE_UNAVAILABLE.code() : INTERNAL_SERVER_ERROR.code();

            rc.response()
              .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
              .setStatusCode(status)
              .end(Json.encode(ImmutableMap.of("error", e.getMessage())));
        }
    }

    public void handleCompactions(RoutingContext rc)
    {
        virtualTable(rc, VirtualTables.SSTableTask.class);
    }

    public void handleClients(RoutingContext rc)
    {
        virtualTable(rc, VirtualTables.Client.class);
    }

    public void handleSettings(RoutingContext rc)
    {
        virtualTable(rc, VirtualTables.Setting.class);
    }

    public void handleThreadPools(RoutingContext rc)
    {
        virtualTable(rc, VirtualTables.ThreadStats.class);
    }
}
