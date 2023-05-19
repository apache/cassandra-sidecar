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

package org.apache.cassandra.sidecar.client.request;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import okhttp3.mockwebserver.MockWebServer;
import org.apache.cassandra.sidecar.client.HttpClient;
import org.apache.cassandra.sidecar.client.HttpClientConfig;
import org.apache.cassandra.sidecar.client.RequestContext;
import org.apache.cassandra.sidecar.client.RequestExecutor;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.client.SimpleSidecarInstancesProvider;
import org.apache.cassandra.sidecar.client.retry.NoRetryPolicy;
import org.apache.cassandra.sidecar.client.retry.RetryPolicy;
import org.apache.cassandra.sidecar.client.selection.InstanceSelectionPolicy;
import org.apache.cassandra.sidecar.client.selection.OrderedInstanceSelectionPolicy;
import org.apache.cassandra.sidecar.client.selection.RandomInstanceSelectionPolicy;
import org.jetbrains.annotations.NotNull;

/**
 * Provides base functionality for testing requests
 */
public abstract class BaseRequestTest
{
    protected abstract RequestExecutor sidecarClient();

    protected abstract HttpClient httpClient();

    protected abstract HttpClientConfig httpClientConfig();

    protected RetryPolicy retryPolicy()
    {
        return new NoRetryPolicy();
    }

    protected RequestContext.Builder builder(InstanceSelectionPolicy instanceSelectionPolicy)
    {
        return newBuilder()
               .instanceSelectionPolicy(instanceSelectionPolicy)
               .retryPolicy(retryPolicy());
    }

    protected RequestContext.Builder newBuilder()
    {
        return new RequestContext.Builder();
    }

    public static InstanceSelectionPolicy buildRandomFromServerList(MockWebServer... servers)
    {
        List<SidecarInstanceImpl> instances = Arrays.stream(servers)
                                                    .map(RequestExecutorTest::newSidecarInstance)
                                                    .collect(Collectors.toList());
        return new RandomInstanceSelectionPolicy(new SimpleSidecarInstancesProvider(instances));
    }

    public static InstanceSelectionPolicy buildFromServerList(MockWebServer... servers)
    {
        List<SidecarInstanceImpl> instances = Arrays.stream(servers)
                                                    .map(RequestExecutorTest::newSidecarInstance)
                                                    .collect(Collectors.toList());
        return new OrderedInstanceSelectionPolicy(new SimpleSidecarInstancesProvider(instances));
    }

    @NotNull
    public static SidecarInstanceImpl newSidecarInstance(MockWebServer server)
    {
        return new SidecarInstanceImpl(server.getHostName(), server.getPort());
    }
}
