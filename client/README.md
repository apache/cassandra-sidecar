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

        SidecarConfig sidecarConfig = new SidecarConfig.Builder()
                                      .maxRetries(10)
                                      .retryDelayMillis(200)
                                      .maxRetryDelayMillis(10_000)
                                      .build();

        RetryPolicy defaultRetryPolicy = new ExponentialBackoffRetryPolicy(sidecarConfig.maxRetries(),
                                                                           sidecarConfig.retryDelayMillis(),
                                                                           sidecarConfig.maxRetryDelayMillis());

        VertxHttpClient vertxHttpClient = new VertxHttpClient(vertx, httpClientConfig);
        VertxRequestExecutor requestExecutor = new VertxRequestExecutor(vertxHttpClient);
        SimpleSidecarInstancesProvider instancesProvider = new SimpleSidecarInstancesProvider(new ArrayList<>(clusterConfig));
        return new SidecarClient(instancesProvider, requestExecutor, sidecarConfig, defaultRetryPolicy);
```
