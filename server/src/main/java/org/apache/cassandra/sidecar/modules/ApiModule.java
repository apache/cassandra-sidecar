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

package org.apache.cassandra.sidecar.modules;

import java.util.Collections;
import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.file.FileSystemOptions;
import io.vertx.ext.dropwizard.DropwizardMetricsOptions;
import io.vertx.ext.dropwizard.Match;
import io.vertx.ext.dropwizard.MatchType;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.ErrorHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.TimeoutHandler;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.config.FileSystemOptionsConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.VertxConfiguration;
import org.apache.cassandra.sidecar.config.VertxMetricsConfiguration;
import org.apache.cassandra.sidecar.handlers.JsonErrorHandler;
import org.apache.cassandra.sidecar.handlers.TimeSkewHandler;
import org.apache.cassandra.sidecar.logging.SidecarLoggerHandler;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.MultiBindingTypeResolver;
import org.apache.cassandra.sidecar.modules.multibindings.RouteClassKey;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.RoutingOrder;
import org.apache.cassandra.sidecar.routes.SettableVertxRoute;
import org.apache.cassandra.sidecar.routes.VertxRoute;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.API_V1_ALL_ROUTES;

/**
 * Provides the glue code for API definition and the related, i.e. Vertx, handlers, etc.
 * <p>Note that feature-specific routes are defined in the corresponding modules, e.g. {@link HealthCheckModule}
 */
public class ApiModule extends AbstractModule
{
    /**
     * Basic response body
     */
    public static final Map<String, String> OK_STATUS = Collections.singletonMap("status", "OK");
    public static final Map<String, String> NOT_OK_STATUS = Collections.singletonMap("status", "NOT_OK");

    @Provides
    @Singleton
    Router vertxRouter(Vertx vertx, MultiBindingTypeResolver<VertxRoute> resolver)
    {
        Router router = Router.router(vertx);
        resolver.resolve().forEach((routeClassKey, route) -> {
            try
            {
                if (RouteClassKey.class.isAssignableFrom(routeClassKey))
                {
                    //noinspection unchecked
                    Class<? extends RouteClassKey> key = (Class<? extends RouteClassKey>) routeClassKey;
                    SettableVertxRoute settableVertxRoute = (SettableVertxRoute) route;
                    settableVertxRoute.setRouteClassKey(key);
                }
                route.mountTo(router);
            }
            catch (Throwable cause)
            {
                throw new RuntimeException("Failed to mount route: " + routeClassKey.getSimpleName(), cause);
            }
        });
        return router;
    }

    @Provides
    @Singleton
    Vertx vertx(SidecarConfiguration sidecarConfiguration, MetricRegistryFactory metricRegistryFactory)
    {
        VertxMetricsConfiguration metricsConfig = sidecarConfiguration.metricsConfiguration().vertxConfiguration();
        Match serverRouteMatch = new Match().setValue(API_V1_ALL_ROUTES).setType(MatchType.REGEX);
        DropwizardMetricsOptions dropwizardMetricsOptions
        = new DropwizardMetricsOptions().setEnabled(metricsConfig.enabled())
                                        .setJmxEnabled(metricsConfig.exposeViaJMX())
                                        .setJmxDomain(metricsConfig.jmxDomainName())
                                        .setMetricRegistry(metricRegistryFactory.getOrCreate())
                                        // Monitor all V1 endpoints.
                                        // Additional filtering is done by configuring yaml fields 'metrics.include|exclude'
                                        .addMonitoredHttpServerRoute(serverRouteMatch);

        VertxOptions vertxOptions = new VertxOptions().setMetricsOptions(dropwizardMetricsOptions);
        VertxConfiguration vertxConfiguration = sidecarConfiguration.vertxConfiguration();
        FileSystemOptionsConfiguration fsOptions = vertxConfiguration != null ? vertxConfiguration.filesystemOptionsConfiguration() : null;

        if (fsOptions != null)
        {
            vertxOptions.setFileSystemOptions(new FileSystemOptions()
                                              .setClassPathResolvingEnabled(fsOptions.classpathResolvingEnabled())
                                              .setFileCacheDir(fsOptions.fileCacheDir())
                                              .setFileCachingEnabled(fsOptions.fileCachingEnabled()));
        }

        return Vertx.vertx(vertxOptions);
    }

    @Provides
    @Singleton
    ErrorHandler errorHandler()
    {
        return new JsonErrorHandler();
    }

    @Provides
    @Singleton
    LoggerHandler loggerHandler()
    {
        return SidecarLoggerHandler.create(LoggerHandler.create());
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.GlobalUtilityHandlerKey.class)
    VertxRoute globalUtilityHandler(SidecarConfiguration sidecarConfiguration,
                                    LoggerHandler loggerHandler)
    {
        return VertxRoute.create(router -> {
            router.route()
                  .order(RoutingOrder.HIGHEST.order)
                  .handler(loggerHandler)
                  .handler(TimeoutHandler.create(sidecarConfiguration.serviceConfiguration().requestTimeout().toMillis(),
                                                 HttpResponseStatus.REQUEST_TIMEOUT.code()));
        });
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.GlobalErrorHandlerKey.class)
    VertxRoute globalErrorHandler(ErrorHandler errorHandler)
    {
        return VertxRoute.create(router -> {
            router.route()
                  .path(ApiEndpointsV1.API + "/*")
                  .failureHandler(errorHandler);
        });
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.TimeSkewRouteKey.class)
    VertxRoute timeSkewRoute(RouteBuilder.Factory factory,
                             TimeSkewHandler timeSkewHandler)
    {
        return factory.builderForUnauthorizedRoute()
                      .handler(timeSkewHandler)
                      .build();
    }
}
