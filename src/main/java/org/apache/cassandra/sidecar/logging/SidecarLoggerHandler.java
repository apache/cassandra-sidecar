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

package org.apache.cassandra.sidecar.logging;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.LoggerFormat;
import io.vertx.ext.web.handler.LoggerFormatter;
import io.vertx.ext.web.handler.LoggerHandler;

/**
 * A decorator class for the {@link LoggerHandler} that logs server errors to better help debug server issues.
 */
public class SidecarLoggerHandler implements LoggerHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarLoggerHandler.class);
    private final LoggerHandler loggerHandler;

    /**
     * Constructs a new instance with the delegate {@link LoggerHandler}
     *
     * @param loggerHandler the delegate {@link LoggerHandler}
     */
    private SidecarLoggerHandler(LoggerHandler loggerHandler)
    {
        this.loggerHandler = loggerHandler;
    }

    public static LoggerHandler create(LoggerHandler handler)
    {
        return new SidecarLoggerHandler(handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handle(RoutingContext context)
    {
        context.addBodyEndHandler(v -> {
            if (context.statusCode() >= 500 && context.failure() != null)
            {
                LOGGER.error("Server error code={}, path={}, params={}", context.statusCode(),
                             context.request().path(), toJson(context.request().params()), context.failure());
            }
        });
        loggerHandler.handle(context);
    }

    /**
     * Set the custom formatter to be used by the handler.
     *
     * @param formatter the formatting function
     * @return the formatted log string
     * @throws IllegalStateException if current format is not {@link LoggerFormat#CUSTOM}
     * @deprecated Superseded by {@link #customFormatter(LoggerFormatter)}
     */
    @Override
    public LoggerHandler customFormatter(Function<HttpServerRequest, String> formatter)
    {
        return loggerHandler.customFormatter(formatter);
    }

    /**
     * Set the custom formatter to be used by the handler.
     *
     * @param formatter the formatter
     * @return the formatted log string
     * @throws IllegalStateException if current format is not {@link LoggerFormat#CUSTOM}
     */
    @Override
    public LoggerHandler customFormatter(LoggerFormatter formatter)
    {
        return loggerHandler.customFormatter(formatter);
    }

    private Object toJson(MultiMap params)
    {
        if (params == null) return "";
        return Json.encode(params.entries());
    }
}
