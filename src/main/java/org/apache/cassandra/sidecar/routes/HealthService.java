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

import com.google.common.collect.ImmutableMap;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class HealthService
{
    private static final Logger logger = LoggerFactory.getLogger(HealthService.class);
    private final int CHECK_PERIOD_MS;
    private final Supplier<Boolean> check;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean lastKnownStatus = false;

    public HealthService(int checkPeriodMillis, Supplier<Boolean> check)
    {
        this.CHECK_PERIOD_MS = checkPeriodMillis;
        this.check = check;
    }

    synchronized public void start()
    {
        logger.info("Starting health check");
        executor.scheduleWithFixedDelay(this::refreshNow, 0, CHECK_PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    synchronized public void refreshNow()
    {
        try
        {
            lastKnownStatus = this.check.get();
        }
        catch (Exception e)
        {
            logger.error("Error while performing health check", e);
        }
    }

    synchronized public void stop()
    {
        logger.info("Stopping health check");
        executor.shutdown();
    }

    public void handleHealth(RoutingContext rc)
    {
        try
        {
            rc.response()
              .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
              .setStatusCode(lastKnownStatus ? HttpResponseStatus.OK.code() : HttpResponseStatus.SERVICE_UNAVAILABLE.code())
              .end(Json.encode(ImmutableMap.of("status", lastKnownStatus ? "OK" : "NOT_OK")));
        }
        catch (Exception e)
        {
            logger.error("Caught exception", e);
            rc.response().setStatusCode(400).end();
        }
    }
}
