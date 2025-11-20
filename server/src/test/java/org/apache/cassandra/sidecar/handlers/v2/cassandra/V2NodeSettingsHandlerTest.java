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
package org.apache.cassandra.sidecar.handlers.v2.cassandra;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.response.v2.V2NodeSettings;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.mockito.Mockito;

/**
 * The V2NodeSettingsHandlerTest is responsible for verifying that we are able to properly construct a V2NodeSettings
 * response in the V2NodeSettings#handle function.
 */
public class V2NodeSettingsHandlerTest
{
    @Test
    public void handleShouldWriteNodeSettingsJson()
    {
        CassandraAdapterDelegate adapter = buildMockAdapterDelegate();
        InstanceMetadataFetcher metadataFetcher = Mockito.mock(InstanceMetadataFetcher.class);
        Mockito.when(metadataFetcher.delegate(Mockito.any())).thenReturn(adapter);
        ExecutorPools executorPools = buildMockExecutorPools();
        V2NodeSettingsHandler v2NodeSettingsHandler = new V2NodeSettingsHandler(metadataFetcher, executorPools);
        RoutingContext context = buildMockContext();
        v2NodeSettingsHandler.handle(context);
        V2NodeSettings v2NodeSettings = new V2NodeSettings(Map.of("concurrent_reads", "16"));
        Mockito.verify(context, Mockito.times(1)).json(v2NodeSettings);
    }



    private CassandraAdapterDelegate buildMockAdapterDelegate()
    {
        CassandraAdapterDelegate cassandraAdapterDelegate = Mockito.mock(CassandraAdapterDelegate.class);
        Mockito.when(cassandraAdapterDelegate.v2NodeSettings()).thenReturn(Map.of("concurrent_reads", "16"));
        return cassandraAdapterDelegate;
    }

    private ExecutorPools buildMockExecutorPools()
    {
        ServiceConfiguration sidecarConfig = Mockito.mock(ServiceConfiguration.class);
        WorkerPoolConfiguration workerPoolConfiguration = Mockito.mock(WorkerPoolConfiguration.class);
        Mockito.when(workerPoolConfiguration.workerPoolName()).thenReturn("name");
        Mockito.when(workerPoolConfiguration.workerPoolSize()).thenReturn(1);
        Mockito.when(workerPoolConfiguration.workerMaxExecutionTime())
               .thenReturn(new MillisecondBoundConfiguration(500, TimeUnit.MILLISECONDS));
        Mockito.when(sidecarConfig.serverWorkerPoolConfiguration()).thenReturn(workerPoolConfiguration);
        Mockito.when(sidecarConfig.serverInternalWorkerPoolConfiguration()).thenReturn(workerPoolConfiguration);
        Vertx vertx = Mockito.mock(Vertx.class);
        Mockito.when(vertx.createSharedWorkerExecutor(Mockito.any()))
               .thenReturn(Mockito.mock(WorkerExecutor.class));
        return new ExecutorPools(vertx, sidecarConfig);
    }

    private RoutingContext buildMockContext()
    {
        RoutingContext context = Mockito.mock(RoutingContext.class);
        HttpServerRequest request = Mockito.mock(HttpServerRequest.class);
        Mockito.when(request.remoteAddress()).thenReturn(Mockito.mock(SocketAddress.class));
        Mockito.when(request.host()).thenReturn("127.0.0.1");
        Mockito.when(request.params()).thenReturn(Mockito.mock(MultiMap.class));
        Mockito.when(context.request()).thenReturn(request);
        return context;
    }
}
