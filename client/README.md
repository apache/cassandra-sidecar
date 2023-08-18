<!--
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

# Cassandra Sidecar Client

The Sidecar Client is a fully featured client to access Cassandra Sidecar endpoints. It offers support for
retries and Sidecar instance selection policies. The client project itself is technology-agnostic, but we provide
a vertx implementation for the `HttpClient`.

## Usage

The `SidecarClient` provides convenience methods to access the sidecar service endpoints. For example, to access
the ring endpoint we will need to do the following.

```java
CompletableFuture<RingResponse> ringFuture = sidecarClient.ring(keyspace);
```

To instantiate the `SidecarClient`, an implementation of the `RequestExecutor` must be provided. Here is an example
on how to instantiate the `SidecarClient` using vertx's implementation:

```java
        Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(16));

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder<>()
                                            .ssl(false)
                                            .timeoutMillis(10_000)
                                            .idleTimeoutMillis(30_000)
                                            .receiveBufferSize(8196)
                                            .maxChunkSize(4092)
                                            .userAgent("sample-agent/0.0.1")
                                            .build();

        SidecarClientConfig clientConfig = new SidecarClientConfig.Builder()
                                      .maxRetries(10)
                                      .retryDelayMillis(200)
                                      .maxRetryDelayMillis(10_000)
                                      .build();

        RetryPolicy defaultRetryPolicy = new ExponentialBackoffRetryPolicy(clientConfig.maxRetries(),
                                                                           clientConfig.retryDelayMillis(),
                                                                           clientConfig.maxRetryDelayMillis());

        VertxHttpClient vertxHttpClient = new VertxHttpClient(vertx, httpClientConfig);
        VertxRequestExecutor requestExecutor = new VertxRequestExecutor(vertxHttpClient);
        SimpleSidecarInstancesProvider instancesProvider = new SimpleSidecarInstancesProvider(new ArrayList<>(clusterConfig));
        return new SidecarClient(instancesProvider, requestExecutor, clientConfig, defaultRetryPolicy);
```
