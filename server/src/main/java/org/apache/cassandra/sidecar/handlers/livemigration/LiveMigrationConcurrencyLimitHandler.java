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

package org.apache.cassandra.sidecar.handlers.livemigration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.concurrent.ConcurrencyLimiter;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler that enforces concurrency limits for live migration file operations.
 * Returns HTTP 503 (SERVICE_UNAVAILABLE) when the maximum concurrent file requests limit is exceeded.
 */
@Singleton
public class LiveMigrationConcurrencyLimitHandler implements Handler<RoutingContext>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationConcurrencyLimitHandler.class);

    private final ConcurrencyLimiter concurrencyLimiter;

    @Inject
    public LiveMigrationConcurrencyLimitHandler(SidecarConfiguration sidecarConfiguration)
    {
        this.concurrencyLimiter =
        new ConcurrencyLimiter(() -> sidecarConfiguration.liveMigrationConfiguration().maxConcurrentFileRequests());
    }

    @Override
    public void handle(RoutingContext rc)
    {
        if (!concurrencyLimiter.tryAcquire())
        {
            LOGGER.warn("Too many concurrent live migration file requests. Path={}", rc.request().path());
            rc.fail(wrapHttpException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                                      "Server is busy processing live migration file requests, " +
                                      "please try again later"));
            return;
        }
        rc.addEndHandler(v -> concurrencyLimiter.releasePermit());
        rc.next();
    }
}
