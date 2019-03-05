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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.CQLSession;
import org.apache.cassandra.sidecar.Configuration;

/**
 * Tracks health check[s] and provides a REST response that should match that defined by api.yaml
 */
@Singleton
public class HealthService implements Host.StateListener
{
    private static final Logger logger = LoggerFactory.getLogger(HealthService.class);
    private final int checkPeriodMs;
    private final Supplier<Boolean> check;
    private volatile boolean registered = false;

    @Nullable
    private final CQLSession session;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean lastKnownStatus = false;

    @Inject
    public HealthService(Configuration config, HealthCheck check, @Nullable CQLSession session)
    {
        this.checkPeriodMs = config.getHealthCheckFrequencyMillis();
        this.session = session;
        this.check = check;
    }

    public synchronized void start()
    {
        logger.info("Starting health check");
        maybeRegisterHostListener();
        executor.scheduleWithFixedDelay(this::refreshNow, 0, checkPeriodMs, TimeUnit.MILLISECONDS);
    }

    public synchronized void refreshNow()
    {
        try
        {
            lastKnownStatus = this.check.get();
            maybeRegisterHostListener();
        }
        catch (Exception e)
        {
            logger.error("Error while performing health check", e);
        }
    }

    private synchronized void maybeRegisterHostListener()
    {
        if (!registered)
        {
            if (session != null && session.getLocalCql() != null)
            {
                session.getLocalCql().getCluster().register(this);
                registered = true;
            }
        }
    }

    public synchronized void stop()
    {
        logger.info("Stopping health check");
        executor.shutdown();
    }

    public void handleHealth(RoutingContext rc)
    {
        try
        {
            int status = lastKnownStatus ? HttpResponseStatus.OK.code() : HttpResponseStatus.SERVICE_UNAVAILABLE.code();
            rc.response()
              .putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
              .setStatusCode(status)
              .end(Json.encode(ImmutableMap.of("status", lastKnownStatus ? "OK" : "NOT_OK")));
        }
        catch (Exception e)
        {
            logger.error("Caught exception", e);
            rc.response().setStatusCode(400).end();
        }
    }

    public void onAdd(Host host)
    {
        refreshNow();
    }

    public void onUp(Host host)
    {
        refreshNow();
    }

    public void onDown(Host host)
    {
        refreshNow();
    }

    public void onRemove(Host host)
    {
        refreshNow();
    }

    public void onRegister(Cluster cluster)
    {
    }

    public void onUnregister(Cluster cluster)
    {
    }
}
