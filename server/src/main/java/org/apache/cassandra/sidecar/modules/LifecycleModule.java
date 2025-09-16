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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.response.LifecycleInfoResponse;
import org.apache.cassandra.sidecar.config.LifecycleConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.handlers.LifecycleInfoHandler;
import org.apache.cassandra.sidecar.handlers.LifecycleUpdateHandler;
import org.apache.cassandra.sidecar.lifecycle.LifecycleProvider;
import org.apache.cassandra.sidecar.lifecycle.ProcessLifecycleProvider;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.jetbrains.annotations.NotNull;

/**
 * Provides the telemetry capability
 */
public class LifecycleModule extends AbstractModule
{
    @Provides
    @Singleton
    LifecycleProvider lifecycleProvider(SidecarConfiguration sidecarConfiguration)
    {
        LifecycleConfiguration lifecycleConfiguration = sidecarConfiguration.lifecycleConfiguration();
        if (!lifecycleConfiguration.enabled())
        {
            return getNoopProvider();
        }

        ParameterizedClassConfiguration providerClass = lifecycleConfiguration.lifecycleProvider();
        if (providerClass == null)
        {
            throw new ConfigurationException("Lifecycle management is enabled, but provider not set.");
        }

        if (providerClass.className().equalsIgnoreCase(ProcessLifecycleProvider.class.getName()))
        {
            Map<String, String> namedParams = providerClass.namedParameters();
            return new ProcessLifecycleProvider(namedParams != null ? namedParams : Collections.emptyMap());
        }

        throw new ConfigurationException("Unrecognized authorization provider " + providerClass.className() + " set");
    }

    private static @NotNull LifecycleProvider getNoopProvider()
    {
        return new LifecycleProvider()
        {
            @Override
            public void start(InstanceMetadata instance)
            {
                throw new UnsupportedOperationException("Lifecycle management is disabled. Cannot start instance " + instance.host());
            }

            @Override
            public void stop(InstanceMetadata instance)
            {
                throw new UnsupportedOperationException("Lifecycle management is disabled. Cannot stop instance " + instance.host());
            }

            @Override
            public boolean isRunning(InstanceMetadata instance)
            {
                throw new UnsupportedOperationException("Lifecycle management is disabled. Cannot check if instance " + instance.host() + " is running");
            }
        };
    }

    @PUT
    @Path(ApiEndpointsV1.LIFECYCLE_ROUTE)
    @Operation(summary = "Updates desired lifecycle state",
            description = "Updates the desired lifecycle state for a local Cassandra node. Valid states are 'RUNNING' or 'STOPPED'.")
    @APIResponse(description = "Desired lifecycle state updated successfully",
            responseCode = "202",
            content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = LifecycleInfoResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LifecycleUpdateRouteKey.class)
    VertxRoute lifecycleUpdateRoute(RouteBuilder.Factory factory,
                                    LifecycleUpdateHandler lifecycleUpdateHandler)
    {
        return factory.builderForRoute()
                      .setBodyHandler(true)
                      .handler(lifecycleUpdateHandler)
                      .build();
    }

    @GET
    @Path(ApiEndpointsV1.LIFECYCLE_ROUTE)
    @Operation(summary = "Gets lifecycle information",
            description = "Returns the lifecycle information for a local Cassandra node")
    @APIResponse(description = "Lifecycle information retrieved successfully",
            responseCode = "200",
            content = @Content(mediaType = "application/json",
            schema = @Schema(implementation = LifecycleInfoResponse.class)))
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.LifecycleInfoRouteKey.class)
    VertxRoute lifecycleInfoRoute(RouteBuilder.Factory factory,
                                    LifecycleInfoHandler lifecycleInfoHandler)
    {
        return factory.buildRouteWithHandler(lifecycleInfoHandler);
    }
}
