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

package org.apache.cassandra.sidecar.client;


import java.util.List;

import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.client.retry.ExponentialBackoffRetryPolicy;
import org.apache.cassandra.sidecar.client.retry.RetryPolicy;

/**
 * Unit tests for the {@link SidecarClient} using vertx
 */
public class VertxSidecarClientTest extends SidecarClientTest
{
    @Override
    protected SidecarClient initialize(List<SidecarInstanceImpl> instances)
    {
        Vertx vertx = Vertx.vertx();

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder<>()
                                            .ssl(false)
                                            .userAgent("cassandra-sidecar-test/0.0.1")
                                            .build();

        SidecarConfig sidecarConfig = new SidecarConfig.Builder().maxRetries(instances.size())
                                                                 .retryDelayMillis(50)
                                                                 .maxRetryDelayMillis(100)
                                                                 .build();

        RetryPolicy defaultRetryPolicy = new ExponentialBackoffRetryPolicy(sidecarConfig.maxRetries(),
                                                                           sidecarConfig.retryDelayMillis(),
                                                                           sidecarConfig.maxRetryDelayMillis());

        VertxHttpClient vertxHttpClient = new VertxHttpClient(vertx, httpClientConfig);
        VertxRequestExecutor requestExecutor = new VertxRequestExecutor(vertxHttpClient);
        SimpleSidecarInstancesProvider instancesProvider = new SimpleSidecarInstancesProvider(instances);
        return new SidecarClient(instancesProvider, requestExecutor, sidecarConfig, defaultRetryPolicy);
    }
}
